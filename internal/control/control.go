// Package control 实现微虚拟机生命周期与资源放置控制面。
//
// 并发一致性边界：所有状态迁移与资源账目变更均在单个互斥锁内完成，
// 后端调用也在锁内执行（后端实现必须幂等）。因此任意时刻
// 节点 Allocated 之和不超过 Capacity，全局配额不会被超卖，
// 且不存在对观察者可见的半完成状态。
package control

import (
	"context"
	"fmt"
	"sync"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// CreateRequest 是创建实例的请求。
type CreateRequest struct {
	VMID           string
	IdempotencyKey string
	Resources      model.Resources
	Constraints    Constraints
}

// Controller 是控制面入口。全部方法并发安全。
type Controller struct {
	mu        sync.Mutex
	nodes     map[string]*model.Node
	vms       map[string]*model.MicroVM
	byIdemKey map[string]string // idempotencyKey -> vmID
	drivers   map[string]backend.Driver
	audit     *AuditLog
	gen       uint64
	// Quota 为全局配额（所有非终态实例 Request 之和不得超过）。
	Quota model.Resources
	// AllowDegraded 显式声明允许的安全降级（缺失能力降级为警告并记录审计）。
	AllowDegraded map[model.Capability]bool
}

// New 构造控制面。
func New(quota model.Resources) *Controller {
	return &Controller{
		nodes:         map[string]*model.Node{},
		vms:           map[string]*model.MicroVM{},
		byIdemKey:     map[string]string{},
		drivers:       map[string]backend.Driver{},
		audit:         NewAuditLog(),
		Quota:         quota,
		AllowDegraded: map[model.Capability]bool{},
	}
}

// Audit 返回审计日志。
func (c *Controller) Audit() *AuditLog { return c.audit }

func (c *Controller) log(op, vmID, nodeID, decision, reason, detail string) {
	c.audit.Append(Event{Op: op, VMID: vmID, NodeID: nodeID, Decision: decision, Reason: reason, Detail: detail})
}

func (c *Controller) nextGen() uint64 {
	c.gen++
	return c.gen
}

// RegisterNode 登记节点及其后端驱动；重复登记同 ID 节点返回 AlreadyExists；
// 后端协议版本不兼容时拒绝。
func (c *Controller) RegisterNode(nodeID string, capacity model.Resources, drv backend.Driver) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.nodes[nodeID]; ok {
		return model.NewError(model.ReasonAlreadyExists, "node %s already registered", nodeID)
	}
	if !backend.Compatible(drv.ProtocolVersion()) {
		c.log("register-node", "", nodeID, "rejected", string(model.ReasonVersionIncompatible),
			fmt.Sprintf("protocol version %d outside [%d,%d]", drv.ProtocolVersion(), backend.MinProtocolVersion, backend.MaxProtocolVersion))
		return model.NewError(model.ReasonVersionIncompatible, "node %s backend protocol %d unsupported", nodeID, drv.ProtocolVersion())
	}
	c.nodes[nodeID] = &model.Node{
		ID:             nodeID,
		State:          model.NodeReady,
		Capacity:       capacity,
		Caps:           drv.Capabilities(),
		BackendVersion: drv.ProtocolVersion(),
	}
	c.drivers[nodeID] = drv
	c.log("register-node", "", nodeID, "accepted", "", fmt.Sprintf("capacity %+v caps %v", capacity, drv.Capabilities()))
	return nil
}

// MarkNodeLost 标记节点失联；其上的非终态实例转为 Failed（资源回收）并记录审计。
func (c *Controller) MarkNodeLost(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return model.NewError(model.ReasonNotFound, "node %s not found", nodeID)
	}
	n.State = model.NodeLost
	for _, vm := range c.vms {
		if vm.NodeID == nodeID && !vm.State.Terminal() {
			n.Allocated = n.Allocated.Sub(vm.Request)
			vm.State = model.StateFailed
			vm.LastError = fmt.Sprintf("node %s lost", nodeID)
			vm.Generation = c.nextGen()
			c.log("mark-lost", vm.ID, nodeID, "vm->Failed, resources released", string(model.ReasonNodeUnavailable), vm.LastError)
		}
	}
	c.log("mark-lost", "", nodeID, "node->Lost", "", "")
	return nil
}

// quotaUsed 统计非终态实例的配额占用（调用方须持锁）。
func (c *Controller) quotaUsed() model.Resources {
	var used model.Resources
	for _, vm := range c.vms {
		if !vm.State.Terminal() {
			used = used.Add(vm.Request)
		}
	}
	return used
}

// resolveCaps 处理能力降级：返回实际要求的能力集合。
// 任一必需能力在所有 Ready 节点都不可用且未显式声明降级时返回错误。
func (c *Controller) resolveCaps(vmID string, required []model.Capability) ([]model.Capability, error) {
	var out []model.Capability
	for _, cap := range required {
		supported := false
		for _, n := range c.nodes {
			if n.State == model.NodeReady && n.Caps[cap] {
				supported = true
				break
			}
		}
		if supported {
			out = append(out, cap)
			continue
		}
		if c.AllowDegraded[cap] {
			c.log("create", vmID, "", "capability degraded", string(model.ReasonCapabilityMissing),
				fmt.Sprintf("capability %s unavailable; degraded by explicit opt-in", cap))
			continue
		}
		return nil, model.NewError(model.ReasonCapabilityMissing, "capability %s not supported by any ready node", cap)
	}
	return out, nil
}

// Create 登记并放置实例（幂等：相同 IdempotencyKey 返回已有实例）。
// 成功时实例处于 Allocated：资源已占用、后端尚未启动。
func (c *Controller) Create(ctx context.Context, req CreateRequest) (*model.MicroVM, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := req.IdempotencyKey
	if key == "" {
		key = req.VMID
	}
	if id, ok := c.byIdemKey[key]; ok {
		vm := c.vms[id]
		c.log("create", id, vm.NodeID, "idempotent replay", "", fmt.Sprintf("key %s", key))
		cp := *vm
		return &cp, nil
	}
	if _, ok := c.vms[req.VMID]; ok {
		return nil, model.NewError(model.ReasonAlreadyExists, "vm %s already exists", req.VMID)
	}
	if req.Resources.IsZero() {
		return nil, model.NewError(model.ReasonInvalidState, "vm %s: resources must be positive", req.VMID)
	}
	if used := c.quotaUsed().Add(req.Resources); !c.Quota.Fits(used) {
		c.log("create", req.VMID, "", "rejected", string(model.ReasonQuotaExceeded),
			fmt.Sprintf("quota %+v would be exceeded by %+v", c.Quota, used))
		return nil, model.NewError(model.ReasonQuotaExceeded, "quota %+v exceeded by request %+v", c.Quota, used)
	}
	caps, err := c.resolveCaps(req.VMID, req.Constraints.RequiredCaps)
	if err != nil {
		c.log("create", req.VMID, "", "rejected", string(model.ReasonOf(err)), err.Error())
		return nil, err
	}
	cons := req.Constraints
	cons.RequiredCaps = caps
	dec, ok := chooseNode(c.nodes, req.Resources, cons)
	if !ok {
		c.log("create", req.VMID, "", "rejected", string(model.ReasonNoSuitableNode), fmt.Sprintf("rejected: %v", dec.Rejected))
		return nil, model.NewError(model.ReasonNoSuitableNode, "vm %s: %v", req.VMID, dec.Rejected)
	}
	node := c.nodes[dec.NodeID]
	node.Allocated = node.Allocated.Add(req.Resources)
	vm := &model.MicroVM{
		ID:             req.VMID,
		NodeID:         dec.NodeID,
		State:          model.StateAllocated,
		Request:        req.Resources,
		Required:       caps,
		IdempotencyKey: key,
		Generation:     c.nextGen(),
	}
	c.vms[vm.ID] = vm
	c.byIdemKey[key] = vm.ID
	c.log("create", vm.ID, dec.NodeID, "allocated", "", fmt.Sprintf("decision: %s; alloc %+v", dec.Reason, req.Resources))
	cp := *vm
	return &cp, nil
}

// Start 启动实例（幂等）。后端失败时回收资源并收敛到 Failed 终态；
// 后端结果不确定时通过 IsRunning 对账，收敛到 Running 或 Failed。
func (c *Controller) Start(ctx context.Context, vmID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	vm, ok := c.vms[vmID]
	if !ok {
		return model.NewError(model.ReasonNotFound, "vm %s not found", vmID)
	}
	switch vm.State {
	case model.StateRunning:
		return nil // 幂等
	case model.StateAllocated, model.StateStopped:
	default:
		return model.NewError(model.ReasonInvalidState, "vm %s in state %s cannot start", vmID, vm.State)
	}
	drv := c.drivers[vm.NodeID]
	err := drv.Start(ctx, vmID, vm.Request)
	if err == nil {
		vm.State = model.StateRunning
		vm.Generation = c.nextGen()
		c.log("start", vmID, vm.NodeID, "running", "", "")
		return nil
	}
	if model.ReasonOf(err) == model.ReasonBackendIndeterminate {
		// 对账：以后端实际状态为准收敛。
		running, qerr := drv.IsRunning(ctx, vmID)
		if qerr == nil && running {
			vm.State = model.StateRunning
			vm.Generation = c.nextGen()
			c.log("start", vmID, vm.NodeID, "reconciled to Running", string(model.ReasonBackendIndeterminate), "backend actually started")
			return nil
		}
		c.rollbackToFailed(vm, fmt.Sprintf("start indeterminate, reconcile shows not running: %v", err))
		return model.NewError(model.ReasonBackendIndeterminate, "vm %s start indeterminate, rolled back", vmID)
	}
	c.rollbackToFailed(vm, fmt.Sprintf("backend start failed: %v", err))
	return model.NewError(model.ReasonBackendFailure, "vm %s start failed: %v", vmID, err)
}

// rollbackToFailed 回收资源并将实例收敛到 Failed 终态（调用方须持锁）。
func (c *Controller) rollbackToFailed(vm *model.MicroVM, why string) {
	if n, ok := c.nodes[vm.NodeID]; ok {
		n.Allocated = n.Allocated.Sub(vm.Request)
	}
	vm.State = model.StateFailed
	vm.LastError = why
	vm.Generation = c.nextGen()
	c.log("rollback", vm.ID, vm.NodeID, "vm->Failed, resources released", string(model.ReasonBackendFailure), why)
}

// Stop 停止实例（幂等）；资源保留，可再次 Start。
func (c *Controller) Stop(ctx context.Context, vmID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	vm, ok := c.vms[vmID]
	if !ok {
		return model.NewError(model.ReasonNotFound, "vm %s not found", vmID)
	}
	switch vm.State {
	case model.StateStopped:
		return nil // 幂等
	case model.StateRunning:
	default:
		return model.NewError(model.ReasonInvalidState, "vm %s in state %s cannot stop", vmID, vm.State)
	}
	if err := c.drivers[vm.NodeID].Stop(ctx, vmID); err != nil {
		c.log("stop", vmID, vm.NodeID, "backend stop failed, state kept Running", string(model.ReasonBackendFailure), err.Error())
		return model.NewError(model.ReasonBackendFailure, "vm %s stop failed: %v", vmID, err)
	}
	vm.State = model.StateStopped
	vm.Generation = c.nextGen()
	c.log("stop", vmID, vm.NodeID, "stopped", "", "")
	return nil
}

// Delete 删除实例并回收资源（幂等）。后端删除失败时状态不变，可重试。
func (c *Controller) Delete(ctx context.Context, vmID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	vm, ok := c.vms[vmID]
	if !ok {
		return model.NewError(model.ReasonNotFound, "vm %s not found", vmID)
	}
	if vm.State == model.StateDeleted {
		return nil // 幂等
	}
	if err := c.drivers[vm.NodeID].Delete(ctx, vmID); err != nil {
		c.log("delete", vmID, vm.NodeID, "backend delete failed, state unchanged", string(model.ReasonBackendFailure), err.Error())
		return model.NewError(model.ReasonBackendFailure, "vm %s delete failed: %v", vmID, err)
	}
	if !vm.State.Terminal() {
		if n, ok := c.nodes[vm.NodeID]; ok {
			n.Allocated = n.Allocated.Sub(vm.Request)
		}
	}
	vm.State = model.StateDeleted
	vm.Generation = c.nextGen()
	c.log("delete", vmID, vm.NodeID, "deleted, resources released", "", "")
	return nil
}

// GetVM 返回实例快照。
func (c *Controller) GetVM(vmID string) (*model.MicroVM, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vm, ok := c.vms[vmID]
	if !ok {
		return nil, false
	}
	cp := *vm
	return &cp, true
}

// NodeSnapshot 返回节点账目快照。
func (c *Controller) NodeSnapshot(nodeID string) (*model.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}
