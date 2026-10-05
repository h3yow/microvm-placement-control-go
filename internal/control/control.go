package control

import (
	"sort"
	"sync"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// moveCtx 记录一次进行中的疏散搬移（锁内发布、锁内终结）。
// 搬移期间源节点保留实例记账、目标节点临时等额记账，全局 used 不变，
// 从而保证全局不重复占账且配额不被搬移本身消耗。
type moveCtx struct {
	vmID      string
	from      string
	to        string
	prevState model.VMState
	degraded  []string
}

// drainState 记录节点本轮维护（进入 Draining 至结束维护）内累计的成功疏散结果，
// 使重复排空在不重复搬移的同时仍能给出完整的逐实例结果。
type drainState struct {
	evacuated map[string]VMResult
}

// AuditEvent 记录一次状态或资源账目变更，用于可解释性与审计。
type AuditEvent struct {
	Seq    int64
	Op     string
	VMID   string
	NodeID string
	Detail string
}

// ControlPlane 是控制面主体。所有账目与状态变更均在 mu 下串行化，
// 后端调用在锁外执行，结果通过收敛逻辑写回，保证并发一致性。
type ControlPlane struct {
	mu         sync.Mutex
	nodes      map[string]*model.Node
	vms        map[string]*model.MicroVM
	byReqID    map[string]string // requestID -> vmID
	quota      model.Resources   // 全局配额上限（零值表示不限制）
	used       model.Resources   // 全局已记账
	seq        int64
	audit      []AuditEvent
	backendFor func(node *model.Node) backend.Backend
	moving     map[string]*moveCtx        // vmID -> 进行中的疏散搬移
	gates      map[string]*sync.Mutex     // 每节点排空串行化门控
	drains     map[string]*drainState     // nodeID -> 本轮维护的累计疏散状态
	delWaits   map[string][]chan struct{} // vmID -> 等待并发删除收敛的通道
}

// New 创建控制面。backendFor 按节点解析后端（测试可注入伪后端）。
func New(quota model.Resources, backendFor func(*model.Node) backend.Backend) *ControlPlane {
	return &ControlPlane{
		nodes:      map[string]*model.Node{},
		vms:        map[string]*model.MicroVM{},
		byReqID:    map[string]string{},
		quota:      quota,
		backendFor: backendFor,
		moving:     map[string]*moveCtx{},
		gates:      map[string]*sync.Mutex{},
		drains:     map[string]*drainState{},
		delWaits:   map[string][]chan struct{}{},
	}
}

// nodeGateLocked 返回节点级排空门控（惰性创建；调用方须持 c.mu 取得门控，锁外加锁）。
func (c *ControlPlane) nodeGateLocked(nodeID string) *sync.Mutex {
	g, ok := c.gates[nodeID]
	if !ok {
		g = &sync.Mutex{}
		c.gates[nodeID] = g
	}
	return g
}

// log 追加审计事件（须持锁）。
func (c *ControlPlane) log(op, vmID, nodeID, detail string) {
	c.seq++
	c.audit = append(c.audit, AuditEvent{Seq: c.seq, Op: op, VMID: vmID, NodeID: nodeID, Detail: detail})
}

// RegisterNode 登记或更新节点。重复登记同一 ID 视为更新容量与能力（不重置账目）。
func (c *ControlPlane) RegisterNode(node model.Node) error {
	if node.ID == "" {
		return newErr(ReasonInvalidState, "node id is empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.nodes[node.ID]; ok {
		if node.Capacity.MilliCPU < existing.Allocated.MilliCPU || node.Capacity.MemoryMB < existing.Allocated.MemoryMB {
			return newErr(ReasonCapacity, "node %s new capacity below allocated", node.ID)
		}
		existing.Capacity = node.Capacity
		existing.Capabilities = node.Capabilities
		// 维护排空是显式生命周期：重登记不得把 Draining 节点悄悄改回可放置状态。
		if existing.State != model.NodeStateDraining {
			existing.State = node.State
		}
		c.log("node.update", "", node.ID, "node re-registered")
		return nil
	}
	n := node
	if n.State == "" {
		n.State = model.NodeStateReady
	}
	c.nodes[n.ID] = &n
	c.log("node.register", "", n.ID, "node registered")
	return nil
}

// SetNodeState 标记节点 Ready/Offline（失联）。Offline 节点不参与新放置，账目保留。
func (c *ControlPlane) SetNodeState(nodeID string, state model.NodeState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	// Draining 是显式维护生命周期：不能经失联状态接口直接改写，
	// 结束维护必须走 EndMaintenance（先排空再 Ready）。
	if n.State == model.NodeStateDraining && state != model.NodeStateDraining {
		return newErr(ReasonInvalidState,
			"node %s is draining; use EndMaintenance after evacuation instead of forcing %s", nodeID, state)
	}
	n.State = state
	c.log("node.state", "", nodeID, "state="+string(state))
	return nil
}

// RemoveNode 删除节点；节点上仍有未删除 VM 时拒绝。
func (c *ControlPlane) RemoveNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	for _, vm := range c.vms {
		if vm.NodeID == nodeID && vm.State != model.VMStateDeleted {
			return newErr(ReasonInvalidState, "node %s still hosts vm %s", nodeID, vm.ID)
		}
	}
	// 进行中的疏散把该节点作为源或目标时也拒绝，防止端点头像被删导致账目悬挂。
	for _, m := range c.moving {
		if m.from == nodeID || m.to == nodeID {
			return newErr(ReasonInvalidState, "node %s has in-flight evacuation", nodeID)
		}
	}
	c.used = c.used.Sub(n.Allocated)
	delete(c.nodes, nodeID)
	c.log("node.remove", "", nodeID, "node removed")
	return nil
}

// GetVM 查询实例快照。
func (c *ControlPlane) GetVM(vmID string) (*model.MicroVM, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vm, ok := c.vms[vmID]
	if !ok {
		return nil, newErr(ReasonNotFound, "vm %s not found", vmID)
	}
	cp := *vm
	return &cp, nil
}

// NodeSnapshot 返回节点快照（含账目）。
func (c *ControlPlane) NodeSnapshot(nodeID string) (*model.Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return nil, newErr(ReasonNotFound, "node %s not found", nodeID)
	}
	cp := *n
	return &cp, nil
}

// Audit 返回审计日志副本。
func (c *ControlPlane) Audit() []AuditEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]AuditEvent, len(c.audit))
	copy(out, c.audit)
	return out
}

// ---------- 内部：账目（须持锁） ----------

// reserve 在节点与全局配额上记账；不满足时返回可区分错误，不产生任何变更。
func (c *ControlPlane) reserve(n *model.Node, r model.Resources) error {
	if !c.quota.IsZero() && !c.used.Add(r).Fits(c.quota) {
		return newErr(ReasonQuota, "quota exceeded: used=%+v need=%+v quota=%+v", c.used, r, c.quota)
	}
	if !r.Fits(n.Capacity.Sub(n.Allocated)) {
		return newErr(ReasonCapacity, "node %s free=%+v need=%+v", n.ID, n.Capacity.Sub(n.Allocated), r)
	}
	n.Allocated = n.Allocated.Add(r)
	c.used = c.used.Add(r)
	return nil
}

// release 释放节点与全局账目（须持锁）。
func (c *ControlPlane) release(nodeID string, r model.Resources) {
	if n, ok := c.nodes[nodeID]; ok {
		n.Allocated = n.Allocated.Sub(r)
	}
	c.used = c.used.Sub(r)
}

// ---------- 内部：放置 ----------

// checkCaps 判定节点能力是否满足 spec。
// 版本键使用 VersionCompatible 判定；其余键要求精确一致；
// 不一致时仅当 spec.AcceptDegraded 显式声明接受该实际值才放行并记录降级。
func checkCaps(n *model.Node, spec model.VMSpec) (ok bool, degraded []string) {
	for k, want := range spec.RequiredCaps {
		actual := n.Capabilities[k]
		if actual == want {
			continue
		}
		if k == "backend_version" && backend.VersionCompatible(want, actual) {
			continue
		}
		if d, declared := spec.AcceptDegraded[k]; declared && d == actual {
			degraded = append(degraded, k+"="+actual)
			continue
		}
		return false, nil
	}
	return true, degraded
}

// pickNode 按能力、状态、亲和/反亲和与容量过滤节点，返回得分最高者（确定性：ID 升序取首）。
// 返回选中的节点与显式接受的降级项；失败时错误原因可区分。
func (c *ControlPlane) pickNode(spec model.VMSpec) (*model.Node, []string, error) {
	var capMismatch, constraintErr bool
	var candidates []*model.Node
	var degraded []string
	for _, n := range c.nodes {
		if n.State != model.NodeStateReady {
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
			continue // 容量不足：落入默认的 ReasonCapacity
		}
		if len(candidates) == 0 {
			degraded = dg
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		ready := 0
		for _, n := range c.nodes {
			if n.State == model.NodeStateReady {
				ready++
			}
		}
		if ready == 0 {
			return nil, nil, newErr(ReasonNodeUnavailable, "no ready node for vm")
		}
		// 原因优先级：能力 > 约束 > 容量，保证可区分且确定。
		switch {
		case capMismatch:
			return nil, nil, newErr(ReasonCapability, "required capabilities unavailable: %v", spec.RequiredCaps)
		case constraintErr:
			return nil, nil, newErr(ReasonConstraint, "affinity/anti-affinity unsatisfiable")
		default:
			return nil, nil, newErr(ReasonCapacity, "no node with sufficient free capacity")
		}
	}
	// 最松节点优先（剩余容量最大），ID 升序保证确定性。
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

func labelsMatch(nodeCaps, required map[string]string) bool {
	for k, v := range required {
		if nodeCaps[k] != v {
			return false
		}
	}
	return true
}

// hasAntiAffinityConflict 判断节点上是否已有相同反亲和键的活跃 VM（须持锁）。
// 疏散搬移中、账目已划转到目标节点的 Migrating 实例通过 vm.NodeID 一并计入
// （账面单点），防止并发排空把同键实例搬到同一节点。
func (c *ControlPlane) hasAntiAffinityConflict(nodeID, key string) bool {
	for _, vm := range c.vms {
		if vm.NodeID == nodeID && vm.Spec.AntiAffinityKey == key && vm.State != model.VMStateDeleted && vm.State != model.VMStateFailed {
			return true
		}
	}
	return false
}
