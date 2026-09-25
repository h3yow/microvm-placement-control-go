package control

import (
	"context"
	"errors"
	"fmt"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// specEqual 比较幂等重放的请求规格是否一致。
func specEqual(a, b model.VMSpec) bool {
	if a.AntiAffinityKey != b.AntiAffinityKey || a.Resources != b.Resources {
		return false
	}
	if len(a.RequiredCaps) != len(b.RequiredCaps) || len(a.AffinityLabels) != len(b.AffinityLabels) ||
		len(a.AcceptDegraded) != len(b.AcceptDegraded) {
		return false
	}
	for k, v := range a.AcceptDegraded {
		if b.AcceptDegraded[k] != v {
			return false
		}
	}
	for k, v := range a.RequiredCaps {
		if b.RequiredCaps[k] != v {
			return false
		}
	}
	for k, v := range a.AffinityLabels {
		if b.AffinityLabels[k] != v {
			return false
		}
	}
	return true
}

// CreateVM 创建（放置）微虚拟机。
// 幂等：同一 RequestID 且规格一致的重复请求返回已有实例，不重复扣减资源；
// 同一 RequestID 规格不同返回 ReasonConflict。
// 后端失败或结果不确定时，资源账目回滚，实例收敛到 Failed 终态。
func (c *ControlPlane) CreateVM(ctx context.Context, spec model.VMSpec) (*model.MicroVM, error) {
	if spec.RequestID == "" {
		return nil, newErr(ReasonInvalidState, "request_id is required for idempotent create")
	}
	if spec.Resources.IsZero() {
		return nil, newErr(ReasonInvalidState, "resources must be positive")
	}

	// 第一阶段：锁内完成幂等判定、放置与记账。
	c.mu.Lock()
	if id, ok := c.byReqID[spec.RequestID]; ok {
		vm := c.vms[id]
		if !specEqual(vm.Spec, spec) {
			c.mu.Unlock()
			return nil, newErr(ReasonConflict, "request_id %s reused with different spec", spec.RequestID)
		}
		cp := *vm
		c.mu.Unlock()
		return &cp, nil
	}
	node, degraded, err := c.pickNode(spec)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if err := c.reserve(node, spec.Resources); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	vmID := fmt.Sprintf("vm-%s", spec.RequestID)
	vm := &model.MicroVM{
		ID:        vmID,
		RequestID: spec.RequestID,
		Spec:      spec,
		NodeID:    node.ID,
		State:     model.VMStatePlacing,
		BackendID: vmID,
	}
	c.vms[vmID] = vm
	c.byReqID[spec.RequestID] = vmID
	c.log("vm.reserve", vmID, node.ID, fmt.Sprintf("reserved %+v", spec.Resources))
	if len(degraded) > 0 {
		vm.Reason = "degraded: " + fmt.Sprint(degraded)
		c.log("vm.degraded", vmID, node.ID, vm.Reason)
	}
	be := c.backendFor(node)
	c.mu.Unlock()

	// 第二阶段：锁外调用后端。
	createErr := be.CreateVM(ctx, vm.BackendID, spec)

	// 第三阶段：锁内收敛。
	c.mu.Lock()
	defer c.mu.Unlock()
	if createErr == nil {
		vm.State = model.VMStateStopped
		c.log("vm.created", vmID, vm.NodeID, "backend create ok")
		cp := *vm
		return &cp, nil
	}
	// 失败或不确定：对账后回滚账目，收敛到 Failed 终态。
	var unc *backend.ErrUncertain
	uncertain := errors.As(createErr, &unc)
	exists, qerr := be.QueryVM(ctx, vm.BackendID)
	if qerr == nil && exists {
		// 后端实际已创建：尽力清理，避免泄漏。
		_ = be.DeleteVM(ctx, vm.BackendID)
	}
	c.release(vm.NodeID, vm.Spec.Resources)
	vm.State = model.VMStateFailed
	if uncertain {
		vm.Reason = string(ReasonBackendUncertain) + ": create result uncertain, reconciled and rolled back"
		c.log("vm.rollback", vmID, vm.NodeID, vm.Reason)
		cp := *vm
		return &cp, newErr(ReasonBackendUncertain, "vm %s create uncertain; rolled back", vmID)
	}
	vm.Reason = string(ReasonBackend) + ": " + createErr.Error()
	c.log("vm.rollback", vmID, vm.NodeID, vm.Reason)
	cp := *vm
	return &cp, newErr(ReasonBackend, "vm %s create failed: %v", vmID, createErr)
}

// StartVM 启动微虚拟机。幂等：Running 时重复调用直接成功。
// 后端失败收敛到 Failed 终态（资源保留，可删除回收）。
func (c *ControlPlane) StartVM(ctx context.Context, vmID string) (*model.MicroVM, error) {
	c.mu.Lock()
	vm, ok := c.vms[vmID]
	if !ok {
		c.mu.Unlock()
		return nil, newErr(ReasonNotFound, "vm %s not found", vmID)
	}
	switch vm.State {
	case model.VMStateRunning:
		cp := *vm
		c.mu.Unlock()
		return &cp, nil // 幂等重放
	case model.VMStateStopped, model.VMStateFailed:
		// 允许从 Failed 重试启动（后端可能未实际创建，先确保存在）
	default:
		c.mu.Unlock()
		return nil, newErr(ReasonInvalidState, "vm %s in state %s cannot start", vmID, vm.State)
	}
	prev := vm.State
	vm.State = model.VMStateStarting
	node := c.nodes[vm.NodeID]
	be := c.backendFor(node)
	c.mu.Unlock()

	if prev == model.VMStateFailed {
		// 从失败态重试：先幂等确保后端实例存在。
		if err := be.CreateVM(ctx, vm.BackendID, vm.Spec); err != nil {
			return c.failStart(vm, err)
		}
	}
	err := be.StartVM(ctx, vm.BackendID)
	if err != nil {
		return c.failStart(vm, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	vm.State = model.VMStateRunning
	vm.Reason = ""
	c.log("vm.start", vmID, vm.NodeID, "running")
	cp := *vm
	return &cp, nil
}

// failStart 将启动失败收敛到明确终态。
func (c *ControlPlane) failStart(vm *model.MicroVM, err error) (*model.MicroVM, error) {
	var unc *backend.ErrUncertain
	uncertain := errors.As(err, &unc)
	c.mu.Lock()
	defer c.mu.Unlock()
	vm.State = model.VMStateFailed
	if uncertain {
		vm.Reason = string(ReasonBackendUncertain) + ": start result uncertain; marked Failed, resources retained"
		c.log("vm.start.uncertain", vm.ID, vm.NodeID, vm.Reason)
		cp := *vm
		return &cp, newErr(ReasonBackendUncertain, "vm %s start uncertain", vm.ID)
	}
	vm.Reason = string(ReasonBackend) + ": " + err.Error()
	c.log("vm.start.failed", vm.ID, vm.NodeID, vm.Reason)
	cp := *vm
	return &cp, newErr(ReasonBackend, "vm %s start failed: %v", vm.ID, err)
}

// StopVM 停止微虚拟机。幂等：Stopped 时重复调用直接成功。
func (c *ControlPlane) StopVM(ctx context.Context, vmID string) (*model.MicroVM, error) {
	c.mu.Lock()
	vm, ok := c.vms[vmID]
	if !ok {
		c.mu.Unlock()
		return nil, newErr(ReasonNotFound, "vm %s not found", vmID)
	}
	switch vm.State {
	case model.VMStateStopped:
		cp := *vm
		c.mu.Unlock()
		return &cp, nil
	case model.VMStateRunning:
	default:
		c.mu.Unlock()
		return nil, newErr(ReasonInvalidState, "vm %s in state %s cannot stop", vmID, vm.State)
	}
	vm.State = model.VMStateStopping
	be := c.backendFor(c.nodes[vm.NodeID])
	c.mu.Unlock()

	err := be.StopVM(ctx, vm.BackendID)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		// 停止失败：回到 Running（资源与状态守恒，不进入半完成态）。
		vm.State = model.VMStateRunning
		vm.Reason = string(ReasonBackend) + ": stop failed: " + err.Error()
		c.log("vm.stop.failed", vmID, vm.NodeID, vm.Reason)
		cp := *vm
		return &cp, newErr(ReasonBackend, "vm %s stop failed: %v", vmID, err)
	}
	vm.State = model.VMStateStopped
	vm.Reason = ""
	c.log("vm.stop", vmID, vm.NodeID, "stopped")
	cp := *vm
	return &cp, nil
}

// DeleteVM 删除微虚拟机并释放全部账目。幂等：重复删除返回成功。
// 任何状态下均可删除（终态收敛手段）；后端删除失败时资源仍回收，
// 实例进入 Deleted 并记录后端残留，保证控制面账目守恒。
func (c *ControlPlane) DeleteVM(ctx context.Context, vmID string) error {
	c.mu.Lock()
	vm, ok := c.vms[vmID]
	if !ok {
		c.mu.Unlock()
		return nil // 幂等：不存在视为已删除
	}
	if vm.State == model.VMStateDeleted {
		c.mu.Unlock()
		return nil
	}
	be := c.backendFor(c.nodes[vm.NodeID])
	c.mu.Unlock()

	err := be.DeleteVM(ctx, vm.BackendID)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.release(vm.NodeID, vm.Spec.Resources)
	vm.State = model.VMStateDeleted
	if err != nil {
		vm.Reason = string(ReasonBackend) + ": delete failed, resources reclaimed, backend residue possible: " + err.Error()
		c.log("vm.delete.residue", vmID, vm.NodeID, vm.Reason)
		return newErr(ReasonBackend, "vm %s deleted with backend residue: %v", vmID, err)
	}
	vm.Reason = ""
	c.log("vm.delete", vmID, vm.NodeID, "deleted, resources released")
	return nil
}
