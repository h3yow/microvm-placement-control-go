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
   |                    |      ^         |
   |                    |      |       Migrating（维护排空中从源疏散到目标）
   |--失败/不确定--> Failed <----启动失败--+
                     |
任意状态 --删除--> Deleted          （Failed / Deleted 为明确终态）
```

- 不存在半完成态：停止失败回到 `Running`；创建失败/不确定回滚账目后进入 `Failed`。
- `Migrating` 是节点维护排空中的受控中间态：账目已单点划转到目标节点，实例
  尚未在后端收敛；收敛只由排空流程原子提交或整体回滚，外部启动/停止/删除在此
  状态下返回 `INVALID_STATE` 并提示稍后重试。
- 每次状态或账目变更追加一条 `AuditEvent`（序号、操作、实例、节点、详情），
  可通过 `ControlPlane.Audit()` 获取，用于解释与审计。

## 节点维护排空与实例疏散

面向“机器下线维护、但上面仍有实例”的场景，提供两个接口：

- `DrainNode(ctx, nodeID) (*DrainResult, error)`：把节点切到 `Draining`
  （新增节点状态），此后该节点不再接收新实例（放置只选 `Ready`），并把其上
  稳定状态（`Running`/`Stopped`/`Failed`）的实例逐个疏散到其它可用节点。
  返回逐实例结果 `DrainResult.Results`：搬到了哪个节点、哪个失败、失败原因码、
  哪些留在原位。
- `EndMaintenance(ctx, nodeID) error`：节点上已无实例（且无在途疏散）后把它
  重新置为 `Ready`；仍有实例时返回 `INVALID_STATE`。

### 幂等与重试

- 同一节点重复 `DrainNode` 由节点级门控串行化：已搬走的实例不重复搬、不重复
  占账、不重复在后端创建；本轮维护（进入 `Draining` 到 `EndMaintenance`）内
  已成功疏散且仍存活的实例会在后续结果中继续可见，`EndMaintenance` 后结果
  历史清空。
- 疏散期间处于 `Placing`/`Starting`/`Stopping` 等瞬时状态的实例本次跳过、
  留在原位，由重试或对应生命周期流程收敛。
- `EndMaintenance` 重复调用稳定：节点已是 `Ready` 时直接成功。
- `Draining` 是显式生命周期：`SetNodeState`/`RegisterNode` 不会把它悄悄改回
  可放置状态，只能经 `EndMaintenance` 结束。

### 目标判定与失败回滚

- 每个实例都按其**原有 spec 在目标节点重新判定一遍**：节点容量、全局配额语义、
  必需能力与版本兼容、显式声明的降级、亲和/反亲和，全部沿用创建规则；不满足
  时该实例被拒绝并留在原节点、原状态、原占用，原因码与创建时一致
  （`CAPACITY_EXHAUSTED`/`CAPABILITY_MISMATCH`/`CONSTRAINT_VIOLATED`/
  `NODE_UNAVAILABLE`）。单个实例搬不动不影响同批其它实例。
- 搬移后端序列：目标 `CreateVM` →（原 `Running` 才）目标 `StartVM` →
  源 `DeleteVM`。任一步明确失败或结果不确定（`ErrUncertain`）都整体回滚：
  对账后清理目标/源残留，账目划回源节点，实例恢复原节点、原状态，不留半搬状态。
  源删除“不确定但对账确认已删”时向前提交，收敛到目标节点的明确终态。

### 节点状态与资源账目边界

- 搬移采用**账面单点**：发布搬移时账目立即从源节点划转到目标节点（源减、目标加，
  全局 `used` 不变），`vm.NodeID` 同步指向目标；后端实例可能短暂并存于源与目标，
  但任何时刻同一实例在整个系统里只有一份节点占账，全局不重复占账、不被搬移消耗
  配额。回滚时把账目划回源（排空期源不接收放置、只减不增，划回必然仍有容量）。
- 因此即便与创建、启动、停止、删除并发执行，也不会出现：同一实例在两个节点同时
  占账、节点或全局已用资源为负、凭空多算或重复回收。审计事件
  （`node.drain.begin/retry/end`、`vm.evac.begin/commit/rollback/reject`）
  使整个过程可复现。

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
- 维护排空（`Draining`）与失联（`Offline`）是两个独立状态：`Draining` 表示节点
  本身健康、正在主动疏散；它同样不参与新放置，差别在于可以经 `EndMaintenance`
  平滑回到 `Ready`。

## 对既有接口与行为的兼容范围

- 新增节点状态 `Draining` 与实例状态 `Migrating` 为附加状态，不改变既有
  `Ready`/`Offline` 与既有生命周期迁移的语义；未调用排空接口的代码行为不变。
- 既有 `CreateVM/StartVM/StopVM/DeleteVM/SetNodeState/RegisterNode` 签名不变。
  仅增加两个新拒绝分支：对 `Migrating` 实例的启动/停止/删除返回
  `INVALID_STATE`（可稍后重试）；对 `Draining` 节点调用 `SetNodeState` 强制
  改状态返回 `INVALID_STATE`（应走 `EndMaintenance`）。
- 放置过滤规则不变：`pickNode` 仍然只选 `Ready`，因此 `Draining` 节点天然不接收
  新实例，无需调用方改造。

## 本地验证

```sh
go build ./...        # 编译
go vet ./...          # 静态检查
go test ./...         # 全部测试
go test -race ./...   # 竞态检测
go run ./cmd/control-demo   # 本地演示：放置、幂等重放、故障注入、维护排空、审计日志
```

测试覆盖：并发放置（容量不超卖）、容量竞争、配额、并发重复请求、节点失联与恢复、
后端创建失败/结果不确定、启动失败重试、停止失败回退、删除幂等、能力不匹配与显式
降级、反亲和约束；以及本轮新增的——正常排空成功（含运行态保持）、目标容量/能力/
反亲和不满足时逐实例拒绝并整体回滚、重复排空与结束维护的幂等、目标创建失败/结果
不确定、源删除不确定（确认仍在则回滚、确认已删则前推）、排空与并发创建/删除竞争、
多节点同时排空的链式疏散不双占不超卖。断言失败时测试会打印完整操作序列、判定依据
与审计日志；每个用例都核对账目恒等式（节点账之和 == 全局 `used`、不为负、不超卖、
按实例份额逐节点精确对账）。
