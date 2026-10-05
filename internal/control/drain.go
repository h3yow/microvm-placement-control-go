// Package control 的 drain.go 实现节点维护排空与实例疏散。
//
// 一致性设计：
//   - 进入 Draining 的节点不参与新放置（pickNode 只选 Ready 节点）。
//   - 每个实例的搬移分“锁内发布 -> 锁外后端 -> 锁内收敛”三段。发布时账目从源
//     单点划转到目标（源减、目标加，全局 used 不变），vm.NodeID 同步指向目标；
//     后端实例可能短暂双存，但账面任何时刻同一实例只有一份占账，配额不被搬移消耗。
//     回滚时把账目划回源（排空期源不接收放置、只减不增，划回必有容量）。
//   - 目标判定复用创建时的全部规则（容量、配额语义、能力/版本兼容、显式降级、
//     亲和/反亲和）。判定失败的实例不动账、不动状态，返回可区分原因。
//   - 后端失败或结果不确定时整体回滚：清理目标/源后端残留、把账目划回源节点、
//     恢复原节点原状态，不留下半搬状态；实例始终留在原节点。
//   - 节点级门控串行化同一节点的重复排空：已搬实例不重复搬、不重复占账；
//     本轮维护内的成功结果累计返回。
package control

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// VMResult 是单个实例的疏散结果。
type VMResult struct {
	VMID       string
	Evacuated  bool // 是否成功搬到目标节点
	FromNode   string
	ToNode     string // 成功时为目标节点；留在原位时为 FromNode
	State      string // 疏散后实例状态
	ReasonCode ReasonCode
	Reason     string
}

// DrainResult 是一次（含重试合并后的）节点排空结果。
type DrainResult struct {
	NodeID  string
	Results []VMResult // 按 VM ID 升序，含本轮维护内已疏散与本次留在原位的全部实例
}

// Evacuated 数：成功疏散的实例数。
func (r *DrainResult) Evacuated() int {
	n := 0
	for _, x := range r.Results {
		if x.Evacuated {
			n++
		}
	}
	return n
}

// Remaining 数：仍留在该节点的实例数。
func (r *DrainResult) Remaining() int { return len(r.Results) - r.Evacuated() }

// DrainNode 将节点切换为维护排空（Draining），不再接收新实例，并尝试疏散其上全部
// 可搬实例。对同一节点重复调用幂等：已搬实例不重复搬移、不重复占账，结果合并返回。
//
// 单个实例搬不动（容量/约束不满足等）不会中止整个排空：该实例留在原位并给出可区分
// 原因，其余实例继续疏散。节点不存在返回 NOT_FOUND。
func (c *ControlPlane) DrainNode(ctx context.Context, nodeID string) (*DrainResult, error) {
	// 阶段 0：门控 + 状态发布。同一节点的排空（含重试）串行化。
	c.mu.Lock()
	n, ok := c.nodes[nodeID]
	if !ok {
		c.mu.Unlock()
		return nil, newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	gate := c.nodeGateLocked(nodeID)
	ds := c.drains[nodeID]
	if ds == nil {
		ds = &drainState{evacuated: map[string]VMResult{}}
		c.drains[nodeID] = ds
	}
	if n.State != model.NodeStateDraining {
		n.State = model.NodeStateDraining
		c.log("node.drain.begin", "", nodeID, "node entering maintenance drain")
	} else {
		c.log("node.drain.retry", "", nodeID, "drain idempotent retry")
	}
	c.mu.Unlock()

	gate.Lock()
	defer gate.Unlock()

	var results []VMResult
	for _, vmID := range c.drainableVMs(nodeID) {
		results = append(results, c.evacuateOne(ctx, vmID))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// 合并本轮维护内此前已疏散的实例，给出完整逐实例结果；已删除实例不再列入。
	for _, prev := range ds.evacuated {
		cur, still := c.vms[prev.VMID]
		if !still || cur.State == model.VMStateDeleted {
			continue
		}
		found := false
		for _, r := range results {
			if r.VMID == prev.VMID {
				found = true
				break
			}
		}
		if !found {
			results = append(results, prev)
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].VMID < results[j].VMID })
	return &DrainResult{NodeID: nodeID, Results: results}, nil
}

// drainableVMs 返回节点上当前仍需疏散的实例（ID 升序，保证确定性）：
// 处于稳定状态（Running/Stopped/Failed）且仍记账在该节点上的实例。
// 瞬时状态（Placing/Starting/Stopping/Migrating）的实例本次跳过，留在原位，
// 由重试或对应生命周期流程收敛，避免与进行中的后端调用交叉。
func (c *ControlPlane) drainableVMs(nodeID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []string
	for id, vm := range c.vms {
		if vm.NodeID != nodeID || vm.State == model.VMStateDeleted {
			continue
		}
		switch vm.State {
		case model.VMStateRunning, model.VMStateStopped, model.VMStateFailed:
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// evacuateOne 疏散单个实例：锁内发布搬移、锁外执行后端序列、锁内提交或回滚。
func (c *ControlPlane) evacuateOne(ctx context.Context, vmID string) VMResult {
	// 阶段 1：锁内重新判定并发布搬移。
	c.mu.Lock()
	vm, ok := c.vms[vmID]
	if !ok || vm.State == model.VMStateDeleted {
		c.mu.Unlock()
		return VMResult{VMID: vmID, Evacuated: false, FromNode: "", ToNode: "",
			State: string(model.VMStateDeleted), ReasonCode: ReasonNotFound, Reason: "instance vanished before evacuation"}
	}
	from := vm.NodeID
	// 复查状态：自列出待疏散实例以来，并发删除/生命周期可能已让实例离开稳定态。
	// Deleting 已被删除认领、Migrating 已在搬、瞬时态未收敛——都跳过，留在原位。
	switch vm.State {
	case model.VMStateRunning, model.VMStateStopped, model.VMStateFailed:
	default:
		cp := *vm
		c.mu.Unlock()
		return VMResult{VMID: vmID, Evacuated: false, FromNode: from, ToNode: from,
			State:      string(cp.State),
			ReasonCode: ReasonInvalidState,
			Reason:     fmt.Sprintf("vm in state %s at drain time, skipped (retry later)", cp.State)}
	}
	target, degraded, err := c.pickEvacTarget(vm)
	if err != nil {
		c.log("vm.evac.reject", vmID, from, err.Error())
		cp := *vm
		c.mu.Unlock()
		return VMResult{VMID: vmID, Evacuated: false, FromNode: from, ToNode: from,
			State: string(cp.State), ReasonCode: CodeOf(err), Reason: err.Error()}
	}
	mv, err := c.beginMoveLocked(vm, target, degraded)
	srcBE := c.backendFor(c.nodes[from])
	dstBE := c.backendFor(target)
	c.mu.Unlock()
	if err != nil {
		// 理论上不可达（pickEvacTarget 已判定），保守处理为留在原位。
		return VMResult{VMID: vmID, Evacuated: false, FromNode: from, ToNode: from,
			State: "", ReasonCode: CodeOf(err), Reason: err.Error()}
	}

	// 阶段 2：锁外执行后端序列：目标创建 -> （原 Running 才）目标启动 -> 源删除。
	// 后端接口本身幂等；任一步失败/不确定都进入整体回滚。
	if cerr := dstBE.CreateVM(ctx, vm.BackendID, vm.Spec); cerr != nil {
		return c.abortMove(ctx, mv, "create on target", cerr, dstBE)
	}
	startedOnTarget := false
	if mv.prevState == model.VMStateRunning {
		if serr := dstBE.StartVM(ctx, vm.BackendID); serr != nil {
			return c.abortMove(ctx, mv, "start on target", serr, dstBE)
		}
		startedOnTarget = true
	}
	derr := srcBE.DeleteVM(ctx, vm.BackendID)

	// 阶段 3：锁内收敛。
	c.mu.Lock()
	defer c.mu.Unlock()
	if derr == nil {
		return c.commitMoveLocked(mv, startedOnTarget)
	}
	return c.rollbackAfterSourceFailureLocked(ctx, mv, derr, startedOnTarget, srcBE, dstBE)
}

// pickEvacTarget 按实例原有 spec 在除源节点外的 Ready 节点上重新判定一遍。
// 返回错误原因与创建流程可区分码一致。须持锁。
func (c *ControlPlane) pickEvacTarget(vm *model.MicroVM) (*model.Node, []string, error) {
	spec := vm.Spec
	var capMismatch, constraintErr bool
	var candidates []*model.Node
	var degraded []string
	for _, n := range c.nodes {
		if n.State != model.NodeStateReady || n.ID == vm.NodeID {
			continue
		}
		ok, dg := checkCaps(n, spec)
		if !ok {
			capMismatch = true
			continue
		}
		if !labelsMatch(n.Capabilities, spec.AffinityLabels) {
			constraintErr = true
			continue
		}
		if spec.AntiAffinityKey != "" && c.hasAntiAffinityConflict(n.ID, spec.AntiAffinityKey) {
			constraintErr = true
			continue
		}
		if !spec.Resources.Fits(n.Capacity.Sub(n.Allocated)) {
			continue // 容量不足：落入默认 ReasonCapacity
		}
		if len(candidates) == 0 {
			degraded = dg
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		ready := 0
		for _, n := range c.nodes {
			if n.State == model.NodeStateReady && n.ID != vm.NodeID {
				ready++
			}
		}
		if ready == 0 {
			return nil, nil, newErr(ReasonNodeUnavailable, "no alternative ready node for evacuating vm %s", vm.ID)
		}
		switch {
		case capMismatch:
			return nil, nil, newErr(ReasonCapability, "required capabilities unavailable on alternative nodes: %v", spec.RequiredCaps)
		case constraintErr:
			return nil, nil, newErr(ReasonConstraint, "affinity/anti-affinity unsatisfiable on alternative nodes")
		default:
			return nil, nil, newErr(ReasonCapacity, "no alternative node with sufficient free capacity")
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		fi := candidates[i].Capacity.Sub(candidates[i].Allocated)
		fj := candidates[j].Capacity.Sub(candidates[j].Allocated)
		if fi.MilliCPU != fj.MilliCPU {
			return fi.MilliCPU > fj.MilliCPU
		}
		return candidates[i].ID < candidates[j].ID
	})
	return candidates[0], degraded, nil
}

// beginMoveLocked 发布一次搬移：账目单点划转到目标节点（源减、目标加，全局 used
// 不变），vm.NodeID 同步指向目标，实例进入 Migrating。这样搬移期间同一实例在
// 整个系统里也只有一份占账：后端实例可能短暂并存于源与目标，但账面永不双占。
// 排空期源节点不接收新放置、Allocated 只减不增，因此回滚时把账划回源必然仍有容量。
// 须持锁。
func (c *ControlPlane) beginMoveLocked(vm *model.MicroVM, target *model.Node, degraded []string) (*moveCtx, error) {
	free := target.Capacity.Sub(target.Allocated)
	if !vm.Spec.Resources.Fits(free) {
		return nil, newErr(ReasonCapacity, "node %s free=%+v need=%+v", target.ID, free, vm.Spec.Resources)
	}
	fromID := vm.NodeID
	if src, ok := c.nodes[fromID]; ok {
		src.Allocated = src.Allocated.Sub(vm.Spec.Resources)
	}
	target.Allocated = target.Allocated.Add(vm.Spec.Resources)
	mv := &moveCtx{
		vmID:      vm.ID,
		from:      fromID,
		to:        target.ID,
		prevState: vm.State,
		degraded:  degraded,
	}
	c.moving[vm.ID] = mv
	vm.NodeID = target.ID
	vm.State = model.VMStateMigrating
	c.log("vm.evac.begin", vm.ID, fromID,
		fmt.Sprintf("evacuating to %s, accounts moved provisionally", target.ID))
	return mv, nil
}

// abortMove 在目标创建/启动失败或结果不确定时回滚：对账清理目标残留、把账目从目标
// 划回源节点、实例 NodeID/状态恢复原状。调用时不持锁；收敛阶段重新取锁。
func (c *ControlPlane) abortMove(ctx context.Context, mv *moveCtx, phase string, cause error, dstBE backend.Backend) VMResult {
	var unc *backend.ErrUncertain
	uncertain := errors.As(cause, &unc)
	if uncertain {
		// 结果不确定：先对账，目标确实存在则清理，避免重复/残留。
		if exists, qerr := dstBE.QueryVM(ctx, mv.vmID); qerr == nil && exists {
			_ = dstBE.DeleteVM(ctx, mv.vmID)
		}
	} else if phase == "start on target" {
		// 创建已成功、启动失败：目标实例确定存在，清理掉。
		_ = dstBE.DeleteVM(ctx, mv.vmID)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.restoreMoveAccountsLocked(mv)
	vm := c.vms[mv.vmID]
	delete(c.moving, mv.vmID)
	vm.NodeID = mv.from
	vm.State = mv.prevState
	reason := ""
	code := ReasonBackend
	if uncertain {
		code = ReasonBackendUncertain
		reason = fmt.Sprintf("%s: target %s result uncertain, reconciled and moved back", code, phase)
	} else {
		reason = fmt.Sprintf("%s: target %s failed: %v; moved back", code, phase, cause)
	}
	vm.Reason = reason
	c.log("vm.evac.rollback", mv.vmID, mv.from, reason)
	return VMResult{VMID: mv.vmID, Evacuated: false, FromNode: mv.from, ToNode: mv.from,
		State: string(mv.prevState), ReasonCode: code, Reason: reason}
}

// restoreMoveAccountsLocked 把临时划到目标的账目划回源节点（全局 used 不变）。须持锁。
func (c *ControlPlane) restoreMoveAccountsLocked(mv *moveCtx) {
	vm := c.vms[mv.vmID]
	if t, ok := c.nodes[mv.to]; ok {
		t.Allocated = t.Allocated.Sub(vm.Spec.Resources)
	}
	if s, ok := c.nodes[mv.from]; ok {
		s.Allocated = s.Allocated.Add(vm.Spec.Resources)
	}
}

// commitMoveLocked 提交成功搬移：账目已在发布时单点划转到目标，这里只落定 NodeID
// （已指向目标）与运行态。须持锁。
func (c *ControlPlane) commitMoveLocked(mv *moveCtx, startedOnTarget bool) VMResult {
	vm := c.vms[mv.vmID]
	delete(c.moving, mv.vmID)
	vm.NodeID = mv.to
	switch {
	case startedOnTarget:
		vm.State = model.VMStateRunning
	case mv.prevState == model.VMStateFailed:
		vm.State = model.VMStateFailed
	default:
		vm.State = model.VMStateStopped
	}
	if len(mv.degraded) > 0 {
		vm.Reason = "degraded: " + fmt.Sprint(mv.degraded)
	} else {
		vm.Reason = ""
	}
	detail := fmt.Sprintf("evacuated %s -> %s, state=%s", mv.from, mv.to, vm.State)
	c.log("vm.evac.commit", mv.vmID, mv.to, detail)
	res := VMResult{VMID: mv.vmID, Evacuated: true, FromNode: mv.from, ToNode: mv.to,
		State: string(vm.State)}
	if ds := c.drains[mv.from]; ds != nil {
		ds.evacuated[mv.vmID] = res
	}
	return res
}

// rollbackAfterSourceFailureLocked 处理“目标已就绪、源删除失败/不确定”：
// 若能确认源仍存在，则整体回滚（清目标、账目划回源、恢复原状）；
// 若确认源已不存在（删除实际生效但响应失败），则向前提交，收敛到明确终态。
// 须持锁；后端对账在短暂放锁期间进行。
func (c *ControlPlane) rollbackAfterSourceFailureLocked(ctx context.Context, mv *moveCtx, cause error, startedOnTarget bool, srcBE, dstBE backend.Backend) VMResult {
	var unc *backend.ErrUncertain
	uncertain := errors.As(cause, &unc)
	vm := c.vms[mv.vmID]

	if uncertain {
		c.mu.Unlock()
		exists, qerr := srcBE.QueryVM(ctx, vm.BackendID)
		c.mu.Lock()
		if qerr == nil && !exists {
			// 源删除实际生效：向前提交，避免双占。
			detail := "source delete result uncertain but confirmed gone; committed evacuation"
			c.log("vm.evac.uncertain.commit", mv.vmID, mv.to, detail)
			return c.commitMoveLocked(mv, startedOnTarget)
		}
	}

	// 源确认仍在（明确失败，或不确定且对账确认仍存在）：回滚，清掉目标侧。
	c.mu.Unlock()
	_ = dstBE.DeleteVM(ctx, vm.BackendID)
	c.mu.Lock()
	c.restoreMoveAccountsLocked(mv)
	delete(c.moving, mv.vmID)
	vm.NodeID = mv.from
	vm.State = mv.prevState
	code := ReasonBackend
	reason := ""
	if uncertain {
		code = ReasonBackendUncertain
		reason = fmt.Sprintf("%s: source delete uncertain, source confirmed present; moved back", code)
	} else {
		reason = fmt.Sprintf("%s: source delete failed: %v; moved back", code, cause)
	}
	vm.Reason = reason
	c.log("vm.evac.rollback", mv.vmID, mv.from, reason)
	return VMResult{VMID: mv.vmID, Evacuated: false, FromNode: mv.from, ToNode: mv.from,
		State: string(mv.prevState), ReasonCode: code, Reason: reason}
}

// EndMaintenance 结束维护、将节点重新置为 Ready。节点上仍有未疏散（或正在疏散）的
// 实例时拒绝（INVALID_STATE）。重复调用稳定：节点已经 Ready 时直接成功。
func (c *ControlPlane) EndMaintenance(ctx context.Context, nodeID string) error {
	c.mu.Lock()
	n, ok := c.nodes[nodeID]
	if !ok {
		c.mu.Unlock()
		return newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	gate := c.nodeGateLocked(nodeID)
	if n.State == model.NodeStateReady {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	gate.Lock()
	defer gate.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok = c.nodes[nodeID]
	if !ok {
		return newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	if n.State == model.NodeStateReady {
		return nil
	}
	for _, vm := range c.vms {
		if vm.NodeID != nodeID || vm.State == model.VMStateDeleted {
			continue
		}
		return newErr(ReasonInvalidState, "node %s still hosts vm %s, drain incomplete", nodeID, vm.ID)
	}
	for _, m := range c.moving {
		if m.from == nodeID {
			return newErr(ReasonInvalidState, "node %s has in-flight evacuation", nodeID)
		}
	}
	n.State = model.NodeStateReady
	delete(c.drains, nodeID)
	c.log("node.drain.end", "", nodeID, "maintenance finished, node back to Ready")
	return nil
}
