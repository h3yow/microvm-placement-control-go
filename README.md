# microvm-placement-control-go

可独立本地运行的微虚拟机生命周期与资源放置控制面。在并发生命周期操作、容量竞争、
后端能力差异与后端失败下，保证实例状态与资源账目始终一致、可解释且可审计。
全部验证基于内存假后端，不依赖真实虚拟化平台、外部服务或真实凭据。

## 资源与状态模型

### 资源账目

- `Resources{MilliCPU, MemoryMB}`：可计量资源，支持加/减/容纳（`Fits`）判定。
- 每个节点维护 `Capacity`（总量）与 `Allocated`（已占用），
  不变量：**任意时刻 `Allocated ≤ Capacity`**。
- 全局配额 `Controller.Quota`：所有非终态实例 `Request` 之和不得超过配额。
- 资源只在 `Create` 成功放置时扣减一次；`Stop` 保留资源（可原地重启）；
  `Delete`、启动失败回滚、节点失联时释放。

### 实例状态机

```
Pending(隐含) → Allocated → Running ⇄ Stopped
                    │           │
                    ▼           ▼
                 Failed ◄── 节点失联 / 启动失败回滚      （终态）
                 Deleted ◄── Delete                       （终态）
```

- `Allocated`：资源已占用、后端尚未启动。
- `Failed` / `Deleted` 为明确终态，终态实例不占用任何资源与配额。
- 非法迁移（如停止未运行的实例）以 `InvalidState` 拒绝。

### 失败原因（可机器区分）

`model.Error.Reason`：`AlreadyExists`、`NotFound`、`InsufficientCapacity`、
`QuotaExceeded`、`NoSuitableNode`、`NodeUnavailable`、`CapabilityUnsupported`、
`VersionIncompatible`、`InvalidState`、`BackendFailure`、`BackendIndeterminate`。
通过 `model.ReasonOf(err)` 提取。

## 并发一致性边界

- 控制面以**单个互斥锁**串行化所有状态迁移、资源账目与后端调用
  （后端实现必须幂等，见 `backend.Driver` 契约）。
- 因此在任意观察点：不会超卖（容量与配额）、不存在可见的半完成状态、
  同一幂等键的并发请求只产生一个实例与一份资源占用。
- 放置判定（`chooseNode`）对相同输入稳定：节点按 ID 排序扫描，
  `Spread` 时选剩余容量最多者；判定依据与被拒节点原因写入审计日志。

## 幂等、回滚与恢复规则

- **幂等**：`Create` 按 `IdempotencyKey` 去重（重放返回已有实例）；
  `Start`/`Stop`/`Delete` 对已达目标状态的请求为空操作，且后端调用次数不放大。
- **回滚**：后端 `Start` 失败 → 释放资源、实例收敛到 `Failed` 并记录 `LastError`。
- **不确定结果**：后端返回 `BackendIndeterminate` 时，用 `IsRunning` 对账：
  实际已运行 → 收敛 `Running`；否则回滚到 `Failed`。重试不会重复扣减或重复启动。
- **节点失联**（`MarkNodeLost`）：节点退出放置候选，其上非终态实例
  全部收敛到 `Failed` 并释放资源。
- **删除失败**：状态与资源保持不变，修复后端后可安全重试。

## 能力差异与版本兼容

- 节点登记时校验后端协议版本，超出 `[MinProtocolVersion, MaxProtocolVersion]`
  以 `VersionIncompatible` 拒绝。
- 实例声明 `RequiredCaps`：任何 Ready 节点都不支持时以 `CapabilityUnsupported` 拒绝。
- **安全降级必须显式声明**：将能力加入 `Controller.AllowDegraded` 后，
  缺失能力被移出 `Required`（结果稳定），并写入审计日志（`capability degraded`）。

## 审计

`AuditLog` 追加式记录每个操作的序列号、操作类型、实例/节点、判定依据
（放置打分、回滚原因、降级声明）与失败原因。测试断言失败时自动打印完整轨迹。

## 本地验证

```bash
go build ./...          # 编译
go test ./... -race     # 全部测试（含并发竞争，启用竞态检测）
go run ./cmd/demo       # 本地演示：并发创建、容量拒绝、节点失联、审计导出
```

测试覆盖：并发放置不超卖、容量/配额竞争、重复请求幂等、节点失联收敛、
后端失败回滚、不确定结果对账、能力降级、版本兼容、删除失败重试。
