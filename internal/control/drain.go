package control

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// EvacuationResult 记录一次排空尝试中单个实例的疏散结果。
type EvacuationResult struct {
	VMID     string
	FromNode string
	ToNode   string     // 成功搬移时的目标节点；未搬移为空
	Moved    bool       // 是否已搬到目标节点
	Code     ReasonCode // 未搬移时的可区分原因；Moved 时为空
	Detail   string     // 判定依据（失败原因或跳过原因）
}

// DrainResult 是一次排空操作的逐实例结果汇总。
type DrainResult struct {
	NodeID   string
	Results  []EvacuationResult
	Complete bool // 节点上是否已无任何未删除实例
}

// DrainNode 将节点切换为维护中（Draining）并尽量疏散其上的实例。
//
// 幂等：重复或并发调用由 drainMu 串行化；已进入 Draining 的节点不会重复切换状态，
// 仅对仍留在节点上的实例继续疏散。每个实例的搬移按原有放置规则重新判定
// （容量、配额、能力与版本兼容、显式降级、亲和/反亲和），判定或后端失败时
// 该实例整体回滚到原节点原状态，不产生半搬状态或重复占账。
func (c *ControlPlane) DrainNode(ctx context.Context, nodeID string) (*DrainResult, error) {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()

	c.mu.Lock()
	n, ok := c.nodes[nodeID]
	if !ok {
		c.mu.Unlock()
		return nil, newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	if n.State != model.NodeStateDraining {
		n.State = model.NodeStateDraining
		c.log("node.drain.begin", "", nodeID, "node enters maintenance; new placements rejected")
	} else {
		c.log("node.drain.replay", "", nodeID, "drain re-invoked; evacuating remaining instances")
	}
	// 快照当前仍在该节点上的未删除实例（按 ID 排序保证结果确定）。
	var targets []*model.MicroVM
	for _, vm := range c.vms {
		if vm.NodeID == nodeID && vm.State != model.VMStateDeleted {
			targets = append(targets, vm)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	c.mu.Unlock()

	res := &DrainResult{NodeID: nodeID}
	for _, vm := range targets {
		res.Results = append(res.Results, c.evacuateVM(ctx, vm))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	res.Complete = true
	for _, vm := range c.vms {
		if vm.NodeID == nodeID && vm.State != model.VMStateDeleted {
			res.Complete = false
			break
		}
	}
	if res.Complete {
		c.log("node.drain.complete", "", nodeID, "node fully evacuated")
	} else {
		c.log("node.drain.partial", "", nodeID, "some instances remain on node")
	}
	return res, nil
}

// EndMaintenance 结束节点维护，恢复 Ready 重新投入放置。幂等：重复调用稳定成功。
func (c *ControlPlane) EndMaintenance(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	if n.State == model.NodeStateReady {
		c.log("node.maintenance.end.replay", "", nodeID, "already ready")
		return nil
	}
	n.State = model.NodeStateReady
	c.log("node.maintenance.end", "", nodeID, "node returns to Ready")
	return nil
}

// evacuateVM 疏散单个实例：锁内完成目标判定与账目原子迁移，锁外执行后端迁移，
// 失败时整体回滚。任何时刻该实例只在一个节点上占账，全局 used 不变（净零迁移）。
func (c *ControlPlane) evacuateVM(ctx context.Context, vm *model.MicroVM) EvacuationResult {
	res := EvacuationResult{VMID: vm.ID, FromNode: vm.NodeID}

	// 第一阶段：锁内完成状态判定、目标选择与账目原子迁移。
	c.mu.Lock()
	from := c.nodes[vm.NodeID]
	switch vm.State {
	case model.VMStateStopped, model.VMStateRunning, model.VMStateFailed:
		// 可疏散的稳定状态（Failed 保留占账，一并迁走以便排空节点）。
	default:
		// Placing/Starting/Stopping/Migrating：并发生命周期操作进行中，本轮跳过。
		res.Code = ReasonInvalidState
		res.Detail = fmt.Sprintf("vm busy in state %s; left in place", vm.State)
		c.log("vm.evacuate.skip", vm.ID, vm.NodeID, res.Detail)
		c.mu.Unlock()
		return res
	}
	target, degraded, err := c.pickNode(vm.Spec)
	if err != nil {
		// 目标不满足原有放置规则：实例留在原节点，状态与占用不变。
		res.Code = CodeOf(err)
		res.Detail = err.Error()
		c.log("vm.evacuate.reject", vm.ID, vm.NodeID, res.Detail)
		c.mu.Unlock()
		return res
	}
	// 全局配额复核：搬移是净零变更（used 不变），此处确认既有账目未越限。
	if !c.quota.IsZero() && !c.used.Fits(c.quota) {
		res.Code = ReasonQuota
		res.Detail = fmt.Sprintf("quota exceeded: used=%+v quota=%+v", c.used, c.quota)
		c.log("vm.evacuate.reject", vm.ID, vm.NodeID, res.Detail)
		c.mu.Unlock()
		return res
	}
	prevState := vm.State
	prevNodeID := vm.NodeID
	r := vm.Spec.Resources
	// 原子迁移账目：先出后进在同一临界区完成，不存在双节点同时占账的窗口。
	from.Allocated = from.Allocated.Sub(r)
	target.Allocated = target.Allocated.Add(r)
	vm.NodeID = target.ID
	vm.State = model.VMStateMigrating
	vm.Reason = ""
	res.ToNode = target.ID
	if len(degraded) > 0 {
		vm.Reason = "degraded: " + fmt.Sprint(degraded)
		c.log("vm.degraded", vm.ID, target.ID, vm.Reason)
	}
	c.log("vm.evacuate.begin", vm.ID, target.ID, fmt.Sprintf("moving from %s, reserved %+v", prevNodeID, r))
	beFrom := c.backendFor(from)
	beTo := c.backendFor(target)
	c.mu.Unlock()

	// 第二阶段：锁外后端迁移。先在目标幂等创建，原状态为 Running 则在目标启动，
	// 全部就绪后才删除源后端实例，避免任一时刻两端都不可用。
	migErr := beTo.CreateVM(ctx, vm.BackendID, vm.Spec)
	if migErr == nil && prevState == model.VMStateRunning {
		migErr = beTo.StartVM(ctx, vm.BackendID)
	}
	if migErr == nil {
		delErr := beFrom.DeleteVM(ctx, vm.BackendID)
		c.mu.Lock()
		if vm.State != model.VMStateMigrating {
			// 迁移期间被并发删除：账目已由 DeleteVM 在目标节点释放，此处不再触碰，
			// 否则重复回收。清理刚在目标后端创建的实例，避免残留。
			c.log("vm.evacuate.aborted", vm.ID, target.ID, "vm deleted concurrently; target backend copy cleaned")
			c.mu.Unlock()
			_ = beTo.DeleteVM(ctx, vm.BackendID)
			res.Moved, res.ToNode = false, ""
			res.Code = ReasonInvalidState
			res.Detail = "vm deleted concurrently during migration"
			return res
		}
		vm.State = prevState
		res.Moved = true
		res.Detail = fmt.Sprintf("evacuated to %s", target.ID)
		if delErr != nil {
			vm.Reason = string(ReasonBackend) + ": source backend delete failed, residue possible: " + delErr.Error()
			c.log("vm.evacuate.residue", vm.ID, prevNodeID, vm.Reason)
		} else {
			c.log("vm.evacuate.ok", vm.ID, target.ID, fmt.Sprintf("moved from %s state=%s", prevNodeID, prevState))
		}
		c.mu.Unlock()
		return res
	}

	// 失败或不确定：先对账目标后端并清理可能的残留，再整体回滚到账目前状态。
	var unc *backend.ErrUncertain
	uncertain := errors.As(migErr, &unc)
	if exists, qerr := beTo.QueryVM(ctx, vm.BackendID); qerr == nil && exists {
		_ = beTo.DeleteVM(ctx, vm.BackendID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if vm.State != model.VMStateMigrating {
		// 并发删除已释放目标节点账目，不得再次回滚，否则重复回收。
		c.log("vm.evacuate.aborted", vm.ID, target.ID, "vm deleted concurrently during failed migration")
		res.Moved, res.ToNode = false, ""
		res.Code = ReasonInvalidState
		res.Detail = "vm deleted concurrently during migration"
		return res
	}
	// 整体回滚：账目迁回原节点，实例回到原状态，不留半搬状态。
	target.Allocated = target.Allocated.Sub(r)
	from.Allocated = from.Allocated.Add(r)
	vm.NodeID = prevNodeID
	vm.State = prevState
	res.Moved, res.ToNode = false, ""
	if uncertain {
		res.Code = ReasonBackendUncertain
	} else {
		res.Code = ReasonBackend
	}
	res.Detail = fmt.Sprintf("target backend migration failed: %v; rolled back to %s", migErr, prevNodeID)
	vm.Reason = string(res.Code) + ": " + res.Detail
	c.log("vm.evacuate.rollback", vm.ID, prevNodeID, res.Detail)
	return res
}
