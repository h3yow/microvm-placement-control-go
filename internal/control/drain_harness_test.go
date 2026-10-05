package control

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// multiHarness 为每个节点提供独立的伪后端，用于验证疏散时跨后端的搬移序列。
type multiHarness struct {
	t     *testing.T
	cp    *ControlPlane
	fakes map[string]*backend.Fake
	mu    sync.Mutex
	ops   []string
}

var testCtx = context.Background()

func newMultiHarness(t *testing.T, quota model.Resources) *multiHarness {
	t.Helper()
	h := &multiHarness{t: t, fakes: map[string]*backend.Fake{}}
	h.cp = New(quota, func(n *model.Node) backend.Backend {
		f, ok := h.fakes[n.ID]
		if !ok {
			t.Fatalf("no fake backend for node %s", n.ID)
		}
		return f
	})
	return h
}

func (h *multiHarness) recordf(format string, args ...any) {
	h.mu.Lock()
	h.ops = append(h.ops, fmt.Sprintf(format, args...))
	h.mu.Unlock()
}

func (h *multiHarness) check(cond bool, rationale string, args ...any) {
	h.t.Helper()
	if cond {
		return
	}
	h.mu.Lock()
	h.t.Logf("=== operation sequence ===")
	for i, op := range h.ops {
		h.t.Logf("  [%d] %s", i, op)
	}
	h.mu.Unlock()
	h.t.Logf("=== audit log ===")
	for _, e := range h.cp.Audit() {
		h.t.Logf("  #%d %s vm=%s node=%s %s", e.Seq, e.Op, e.VMID, e.NodeID, e.Detail)
	}
	h.t.Fatalf("ASSERTION FAILED: "+rationale, args...)
}

func (h *multiHarness) addNode(id string, cpu, mem int64, caps ...map[string]string) {
	h.recordf("RegisterNode(%s cpu=%d mem=%d)", id, cpu, mem)
	base := map[string]string{"backend": "firecracker", "backend_version": "1.7.0", "snapshot": "1"}
	for _, m := range caps {
		for k, v := range m {
			base[k] = v
		}
	}
	f := backend.NewFake(backend.CapabilitySet{
		Backend: "firecracker", Version: "1.7.0",
		Features: map[string]string{"snapshot": "1"},
	})
	h.fakes[id] = f
	if err := h.cp.RegisterNode(model.Node{
		ID: id, State: model.NodeStateReady,
		Capacity: model.Resources{MilliCPU: cpu, MemoryMB: mem}, Capabilities: base,
	}); err != nil {
		h.check(false, "RegisterNode(%s) failed: %v", id, err)
	}
}

func (h *multiHarness) create(reqID string, cpu, mem int64) *model.MicroVM {
	h.recordf("CreateVM(req=%s %dm/%dMB)", reqID, cpu, mem)
	vm, err := h.cp.CreateVM(testCtx, spec(reqID, cpu, mem))
	h.check(err == nil, "CreateVM(%s) failed: %v", reqID, err)
	return vm
}

// nodeAllocated 读取节点已记账资源。
func (h *multiHarness) nodeAllocated(id string) model.Resources {
	n, err := h.cp.NodeSnapshot(id)
	h.check(err == nil, "NodeSnapshot(%s): %v", id, err)
	return n.Allocated
}

// assertBooksConsistent 核对全局账目恒等式：
//  1. 全局 used == 所有节点 Allocated 之和（搬移临时双占是节点局部的，全局不重复占账）；
//  2. 每个节点 0 <= Allocated <= Capacity（不超卖、不为负、不凭空多算）；
//  3. 全局 used <= quota（若配置）；
//  4. 每个未删除实例在其所在节点上恰好占一份账：按实例汇总 == 节点 Allocated。
func (h *multiHarness) assertBooksConsistent(stage string) {
	h.t.Helper()
	cp := h.cp

	cp.mu.Lock()
	var nodeSum model.Resources
	nodeIDs := make([]string, 0, len(cp.nodes))
	for id := range cp.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)
	for _, id := range nodeIDs {
		n := cp.nodes[id]
		h.check(n.Allocated.MilliCPU >= 0 && n.Allocated.MemoryMB >= 0,
			"[%s] node %s allocated went negative: %+v", stage, id, n.Allocated)
		h.check(n.Allocated.Fits(n.Capacity),
			"[%s] node %s oversold: allocated=%+v capacity=%+v", stage, id, n.Allocated, n.Capacity)
		nodeSum = nodeSum.Add(n.Allocated)
	}
	h.check(nodeSum == cp.used,
		"[%s] global used %+v != sum of node allocated %+v (double/missing global charge)", stage, cp.used, nodeSum)
	if !cp.quota.IsZero() {
		h.check(cp.used.Fits(cp.quota), "[%s] quota breached: used=%+v quota=%+v", stage, cp.used, cp.quota)
	}
	// 按实例汇总：稳定状态 + Migrating 的实例都在 vm.NodeID 上占一份；
	// Migrating 实例的目标临时占账通过 moveCtx 额外补一份，两者相加必须精确等于节点账目。
	perNode := map[string]model.Resources{}
	extra := map[string]model.Resources{}
	for _, vm := range cp.vms {
		if vm.State == model.VMStateDeleted {
			continue
		}
		perNode[vm.NodeID] = perNode[vm.NodeID].Add(vm.Spec.Resources)
		if vm.State == model.VMStateMigrating {
			if m := cp.moving[vm.ID]; m != nil {
				extra[m.to] = extra[m.to].Add(vm.Spec.Resources)
				// 同一实例全局份额守恒：Migrating 实例源占账保留、目标为临时账，全局只算一份。
				h.check(m.from == vm.NodeID,
					"[%s] migrating vm %s source book drifted: move.from=%s vm.NodeID=%s",
					stage, vm.ID, m.from, vm.NodeID)
			} else {
				h.check(false, "[%s] migrating vm %s has no moveCtx (untrackable intermediate state)", stage, vm.ID)
			}
		}
	}
	for _, id := range nodeIDs {
		want := perNode[id].Add(extra[id])
		got := cp.nodes[id].Allocated
		h.check(want == got,
			"[%s] node %s books mismatch: want(vms+provisional)=%+v got=%+v", stage, id, want, got)
	}
	cp.mu.Unlock()
}
