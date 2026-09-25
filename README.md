# microvm-placement-control-go

可独立本地运行的微虚拟机生命周期与资源放置控制面。在并发生命周期操作、容量竞争、
能力差异和后端失败下，保证实例状态与资源账目始终一致、可解释且可审计。
全部验证基于内存伪后端，不依赖真实虚拟化平台、外部服务或真实凭据。

## 资源与状态模型

### 资源账目

- `Resources{MilliCPU, MemoryMB}`：可计量资源量。
- 每个节点维护 `Capacity`（总量）与 `Allocated`（已记账）；控制面另维护全局
  `used` 与可选配额 `quota`（零值表示不限制）。
- 记账（`reserve`）与释放（`release`）是原子检查-扣减：任一约束不满足则不产生
  任何变更。拒绝原因可区分：
  - `QUOTA_EXCEEDED`：全局配额超限；
  - `CAPACITY_EXHAUSTED`：节点剩余容量不足；
  - `NODE_UNAVAILABLE`：无 Ready 节点（全部失联/未登记）；
  - `CAPABILITY_MISMATCH`：能力不满足且未显式声明降级；
  - `CONSTRAINT_VIOLATED`：亲和/反亲和不可满足；
  - `REQUEST_CONFLICT`：同一幂等键提交了不同规格。

### 实例状态机

```
Placing --创建成功--> Stopped --启动--> Running --停止--> Stopped
   |                    |                |
   |--失败/不确定--> Failed <----启动失败--+
                     |
任意状态 --删除--> Deleted          （Failed / Deleted 为明确终态）
```

- 不存在半完成态：停止失败回到 `Running`；创建失败/不确定回滚账目后进入 `Failed`。
- 每次状态或账目变更追加一条 `AuditEvent`（序号、操作、实例、节点、详情），
  可通过 `ControlPlane.Audit()` 获取，用于解释与审计。

## 并发一致性边界

- 所有账目与状态变更在控制面单把互斥锁下串行化；后端调用在锁外执行，
  结果通过锁内收敛逻辑写回。因此：
  - 并发放置不会超卖：容量检查与扣减原子完成；
  - 并发重复请求（同一 `RequestID`）只产生一个实例、只扣减一次、只调用一次后端；
  - 同一幂等键规格不同返回 `REQUEST_CONFLICT`。
- 放置决策确定：可行节点中剩余 CPU 最多者优先，并列时节点 ID 升序。

## 兼容与恢复规则

- 能力判定：节点能力为键值集合（`backend`、`backend_version`、特性键）。
  `backend_version` 支持 `1.*` 形式的主版本兼容判定（`backend.VersionCompatible`），
  其余键要求精确一致。
- 安全降级必须显式声明：仅当 `VMSpec.AcceptDegraded[k]` 等于节点实际值时才放行，
  降级事实写入实例 `Reason` 与审计日志，结果稳定可重放。
- 后端失败恢复：
  - 创建明确失败：回滚全部账目，实例进入 `Failed`，错误为 `BACKEND_ERROR`；
  - 创建结果不确定（`ErrUncertain`）：先 `QueryVM` 对账，若后端实际已创建则尽力
    `DeleteVM` 清理残留，再回滚账目进入 `Failed`，错误为 `BACKEND_UNCERTAIN`；
  - 启动失败：实例进入 `Failed`（资源保留），修复后端后可重试 `StartVM`，
    重试不重复扣减、不重复创建；
  - 停止失败：回到 `Running`；
  - 删除在任何状态下可用且幂等：后端删除失败时控制面仍回收账目并记录可能的后端残留。
- 节点失联（`Offline`）：不参与新放置，已有账目保留；恢复 `Ready` 后即可继续放置。
  节点上仍有未删除实例时拒绝移除节点。

## 本地验证

```sh
go build ./...        # 编译
go test ./...         # 全部测试
go test -race ./...   # 竞态检测
go run ./cmd/control-demo   # 本地演示：放置、幂等重放、故障注入、审计日志
```

测试覆盖：并发放置（容量不超卖）、容量竞争、配额、并发重复请求、节点失联与恢复、
后端创建失败/结果不确定、启动失败重试、停止失败回退、删除幂等、能力不匹配与显式
降级、反亲和约束。断言失败时测试会打印完整操作序列、判定依据与审计日志。
