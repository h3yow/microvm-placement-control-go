package control

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// dharness 是多后端测试支架：每个节点一个伪后端，可单独注入故障。
// 断言失败时打印操作序列、判定依据与审计日志。
type dharness struct {
	t       *testing.T
	cp      *ControlPlane
	fakes   map[string]*backend.Fake // nodeID -> backend
	nodeIDs []string
	mu      sync.Mutex
	ops     []string
}

func newDHarness(t *testing.T, quota model.Resources) *dharness {
	t.Helper()
	h := &dharness{t: t, fakes: map[string]*backend.Fake{}}
	h.cp = New(quota, func(n *model.Node) backend.Backend { return h.fakes[n.ID] })
	return h
}

func (h *dharness) recordf(format string, args ...any) {
	h.mu.Lock()
	h.ops = append(h.ops, fmt.Sprintf(format, args...))
	h.mu.Unlock()
}

func (h *dharness) check(cond bool, rationale string, args ...any) {
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

func (h *dharness) addNode(id string, cpu, mem int64, caps map[string]string) {
	h.recordf("RegisterNode(%s cpu=%d mem=%d caps=%v)", id, cpu, mem, caps)
	h.fakes[id] = backend.NewFake(backend.CapabilitySet{
		Backend: "firecracker", Version: "1.7.0",
		Features: map[string]string{"snapshot": "1"},
	})
	base := map[string]string{"backend": "firecracker", "backend_version": "1.7.0", "snapshot": "1"}
	for k, v := range caps {
		base[k] = v
	}
	if err := h.cp.RegisterNode(model.Node{
		ID: id, State: model.NodeStateReady,
		Capacity: model.Resources{MilliCPU: cpu, MemoryMB: mem}, Capabilities: base,
	}); err != nil {
		h.check(false, "RegisterNode(%s) failed: %v", id, err)
	}
	h.nodeIDs = append(h.nodeIDs, id)
}

// checkAccounting 校验全局账目不变量：每节点 0<=allocated<=capacity，且各节点之和等于全局已用。
func (h *dharness) checkAccounting() {
	h.t.Helper()
	var sum model.Resources
	for _, id := range h.nodeIDs {
		n, err := h.cp.NodeSnapshot(id)
		h.check(err == nil, "NodeSnapshot(%s) failed: %v", id, err)
		h.check(n.Allocated.MilliCPU >= 0 && n.Allocated.MemoryMB >= 0,
			"node %s allocated must never go negative, got %+v", id, n.Allocated)
		h.check(n.Allocated.Fits(n.Capacity),
			"node %s allocated %+v exceeds capacity %+v (oversell)", id, n.Allocated, n.Capacity)
		sum = sum.Add(n.Allocated)
	}
	used := h.cp.UsedSnapshot()
	h.check(sum == used, "sum of node allocated %+v must equal global used %+v (ledger mismatch)", sum, used)
}

func (h *dharness) auditHas(op, vmID string) bool {
	for _, e := range h.cp.Audit() {
		if e.Op == op && (vmID == "" || e.VMID == vmID) {
			return true
		}
	}
	return false
}

func dspec(reqID string, cpu, mem int64) model.VMSpec {
	return model.VMSpec{
		RequestID: reqID,
		Resources: model.Resources{MilliCPU: cpu, MemoryMB: mem},
	}
}

// TestDrainNodeSuccess 正常排空：实例搬到其它节点、状态保留、账目迁移、节点进入维护，
// 结束后恢复 Ready；重复结束维护稳定。
func TestDrainNodeSuccess(t *testing.T) {
	h := newDHarness(t, model.Resources{})
	h.addNode("n1", 8000, 8192, nil) // 最松节点；与 n2 并列时 ID 升序仍选 n1
	h.addNode("n2", 7000, 7168, nil)
	ctx := context.Background()

	h.recordf("CreateVM(req=a) on n1; CreateVM(req=b) on n1")
	a, err := h.cp.CreateVM(ctx, dspec("a", 1000, 1024))
	h.check(err == nil, "create a failed: %v", err)
	b, err := h.cp.CreateVM(ctx, dspec("b", 1000, 1024))
	h.check(err == nil, "create b failed: %v", err)
	h.check(a.NodeID == "n1" && b.NodeID == "n1", "both vms must land on n1, got %s %s", a.NodeID, b.NodeID)
	h.recordf("StartVM(%s)", a.ID)
	a, err = h.cp.StartVM(ctx, a.ID)
	h.check(err == nil && a.State == model.VMStateRunning, "start a failed: %v", err)

	h.recordf("DrainNode(n1)")
	dr, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain failed: %v", err)
	h.check(dr.Complete, "drain must complete, results=%+v", dr.Results)
	h.check(len(dr.Results) == 2, "expect 2 per-instance results, got %+v", dr.Results)
	for _, r := range dr.Results {
		h.check(r.Moved && r.ToNode == "n2",
			"vm %s must move to n2, got %+v", r.VMID, r)
	}

	a, _ = h.cp.GetVM(a.ID)
	b, _ = h.cp.GetVM(b.ID)
	h.check(a.NodeID == "n2" && a.State == model.VMStateRunning,
		"a must be Running on n2, got %s %s", a.NodeID, a.State)
	h.check(b.NodeID == "n2" && b.State == model.VMStateStopped,
		"b must be Stopped on n2, got %s %s", b.NodeID, b.State)
	h.check(h.fakes["n2"].Exists(a.BackendID) && h.fakes["n2"].Exists(b.BackendID),
		"target backend must hold both instances")
	h.check(!h.fakes["n1"].Exists(a.BackendID) && !h.fakes["n1"].Exists(b.BackendID),
		"source backend must be cleaned after move")

	n1, _ := h.cp.NodeSnapshot("n1")
	n2, _ := h.cp.NodeSnapshot("n2")
	h.check(n1.State == model.NodeStateDraining, "n1 must be Draining, got %s", n1.State)
	h.check(n1.Allocated.IsZero(), "n1 allocated must be zero after drain, got %+v", n1.Allocated)
	h.check(n2.Allocated.MilliCPU == 2000 && n2.Allocated.MemoryMB == 2048,
		"n2 must carry both vms, got %+v", n2.Allocated)
	h.checkAccounting()

	h.recordf("CreateVM(req=c) while n1 draining")
	c, err := h.cp.CreateVM(ctx, dspec("c", 1000, 1024))
	h.check(err == nil, "create c failed: %v", err)
	h.check(c.NodeID == "n2", "draining node must not receive new instances, got %s", c.NodeID)

	h.recordf("EndMaintenance(n1) twice")
	h.check(h.cp.EndMaintenance("n1") == nil, "end maintenance failed")
	h.check(h.cp.EndMaintenance("n1") == nil, "repeated end maintenance must be stable")
	n1, _ = h.cp.NodeSnapshot("n1")
	h.check(n1.State == model.NodeStateReady, "n1 must be Ready after maintenance, got %s", n1.State)
	h.checkAccounting()
}

// TestDrainIdempotent 重复/并发排空同一节点：不重复搬移、不重复占账、不报错。
func TestDrainIdempotent(t *testing.T) {
	h := newDHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	h.addNode("n2", 8000, 8192, nil)
	ctx := context.Background()
	h.recordf("CreateVM(req=x)")
	vm, err := h.cp.CreateVM(ctx, dspec("x", 1000, 1024))
	h.check(err == nil, "create failed: %v", err)

	h.recordf("DrainNode(n1) first")
	dr, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && dr.Complete, "first drain failed: %v %+v", err, dr.Results)
	createsAfterFirst := h.fakes["n2"].CountOp("CreateVM")

	h.recordf("DrainNode(n1) again (idempotent replay)")
	dr2, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "repeated drain must not error, got %v", err)
	h.check(dr2.Complete && len(dr2.Results) == 0,
		"repeated drain must find nothing to move, got %+v", dr2.Results)
	h.check(h.fakes["n2"].CountOp("CreateVM") == createsAfterFirst,
		"repeated drain must not re-create backend instances (%d -> %d)",
		createsAfterFirst, h.fakes["n2"].CountOp("CreateVM"))

	h.recordf("concurrent DrainNode(n1) x4")
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = h.cp.DrainNode(ctx, "n1")
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		h.check(e == nil, "concurrent drain %d must not error, got %v", i, e)
	}
	h.check(h.fakes["n2"].CountOp("CreateVM") == createsAfterFirst,
		"concurrent drains must not duplicate backend creates, got %d", h.fakes["n2"].CountOp("CreateVM"))
	vm, _ = h.cp.GetVM(vm.ID)
	h.check(vm.NodeID == "n2", "vm must stay on n2, got %s", vm.NodeID)
	h.checkAccounting()
}

// TestDrainConstraintFailure 目标容量/约束不满足：实例留在原节点，状态与占用不变，
// 原因可区分，节点保持维护中，排空报告未完成。
func TestDrainConstraintFailure(t *testing.T) {
	h := newDHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	h.addNode("n2", 1500, 1536, nil) // 只装得下一个
	ctx := context.Background()

	h.recordf("CreateVM(req=p), CreateVM(req=q) both on n1")
	p, err := h.cp.CreateVM(ctx, dspec("p", 1000, 1024))
	h.check(err == nil, "create p failed: %v", err)
	q, err := h.cp.CreateVM(ctx, dspec("q", 1000, 1024))
	h.check(err == nil, "create q failed: %v", err)

	h.recordf("DrainNode(n1): n2 fits only one of the two")
	dr, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain itself must not error, got %v", err)
	h.check(!dr.Complete, "drain must report incomplete when an instance cannot move")
	moved, stayed := 0, 0
	var stayReason ReasonCode
	for _, r := range dr.Results {
		if r.Moved {
			moved++
		} else {
			stayed++
			stayReason = r.Code
		}
	}
	h.check(moved == 1 && stayed == 1, "exactly one vm must move, results=%+v", dr.Results)
	h.check(stayReason == ReasonCapacity,
		"unmovable vm must be rejected with CAPACITY_EXHAUSTED, got %s", stayReason)

	p2, _ := h.cp.GetVM(p.ID)
	q2, _ := h.cp.GetVM(q.ID)
	n1, _ := h.cp.NodeSnapshot("n1")
	n2, _ := h.cp.NodeSnapshot("n2")
	h.check(n1.Allocated.MilliCPU == 1000, "n1 must still charge the remaining vm, got %+v", n1.Allocated)
	h.check(n2.Allocated.MilliCPU == 1000, "n2 must charge exactly the moved vm, got %+v", n2.Allocated)
	h.check(n1.State == model.NodeStateDraining, "n1 must remain Draining, got %s", n1.State)
	onN1, onN2 := 0, 0
	for _, v := range []*model.MicroVM{p2, q2} {
		switch v.NodeID {
		case "n1":
			onN1++
			h.check(v.State == model.VMStateStopped,
				"unmoved vm must keep original state, got %s", v.State)
		case "n2":
			onN2++
		}
	}
	h.check(onN1 == 1 && onN2 == 1, "vms must be split 1/1, got n1=%d n2=%d", onN1, onN2)
	h.checkAccounting()

	// 反亲和：唯一可去的目标上已有同 key 实例。
	h2 := newDHarness(t, model.Resources{})
	h2.addNode("m1", 4000, 4096, nil)
	h2.addNode("m2", 4000, 4096, nil)
	s := dspec("aa1", 1000, 1024)
	s.AntiAffinityKey = "db"
	h2.recordf("CreateVM(aa=db) x2 spread over m1/m2")
	va, err := h2.cp.CreateVM(ctx, s)
	h2.check(err == nil, "create aa1 failed: %v", err)
	s.RequestID = "aa2"
	vb, err := h2.cp.CreateVM(ctx, s)
	h2.check(err == nil, "create aa2 failed: %v", err)
	h2.check(va.NodeID != vb.NodeID, "anti-affinity vms must be on different nodes")
	src := va.NodeID
	h2.recordf("DrainNode(%s): only other node holds same anti-affinity key", src)
	dr2, err := h2.cp.DrainNode(ctx, src)
	h2.check(err == nil, "drain failed: %v", err)
	h2.check(!dr2.Complete, "drain must be incomplete")
	h2.check(len(dr2.Results) == 1 && !dr2.Results[0].Moved && dr2.Results[0].Code == ReasonConstraint,
		"anti-affinity blocked move must be CONSTRAINT_VIOLATED, got %+v", dr2.Results)
	va2, _ := h2.cp.GetVM(va.ID)
	h2.check(va2.NodeID == src && va2.State == model.VMStateStopped,
		"blocked vm must stay on source unchanged, got %s %s", va2.NodeID, va2.State)
	h2.checkAccounting()
}

// TestDrainBackendFailureRollback 目标后端创建失败：整体回滚，实例回到原节点原状态，
// 两端账目不变，原因可区分且写入审计。
func TestDrainBackendFailureRollback(t *testing.T) {
	h := newDHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	h.addNode("n2", 4000, 4096, nil)
	ctx := context.Background()
	h.recordf("CreateVM(req=r) on n1, then StartVM")
	vm, err := h.cp.CreateVM(ctx, dspec("r", 1000, 1024))
	h.check(err == nil, "create failed: %v", err)
	vm, err = h.cp.StartVM(ctx, vm.ID)
	h.check(err == nil && vm.State == model.VMStateRunning, "start failed: %v", err)

	h.fakes["n2"].FailOn("CreateVM", fmt.Errorf("hypervisor refused"))
	h.recordf("DrainNode(n1) with target backend CreateVM failing")
	dr, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain must not error on per-instance failure, got %v", err)
	h.check(!dr.Complete, "drain must be incomplete")
	h.check(len(dr.Results) == 1 && !dr.Results[0].Moved && dr.Results[0].Code == ReasonBackend,
		"result must be BACKEND_ERROR, got %+v", dr.Results)

	vm2, _ := h.cp.GetVM(vm.ID)
	h.check(vm2.NodeID == "n1" && vm2.State == model.VMStateRunning,
		"vm must roll back to source node and original state, got %s %s", vm2.NodeID, vm2.State)
	n1, _ := h.cp.NodeSnapshot("n1")
	n2, _ := h.cp.NodeSnapshot("n2")
	h.check(n1.Allocated.MilliCPU == 1000 && n2.Allocated.IsZero(),
		"accounting must be fully rolled back: n1=%+v n2=%+v", n1.Allocated, n2.Allocated)
	h.check(!h.fakes["n2"].Exists(vm.BackendID),
		"target backend residue must be reconciled away")
	h.check(h.auditHas("vm.evacuate.rollback", vm.ID),
		"audit must record the rollback for %s", vm.ID)
	h.checkAccounting()

	h.fakes["n2"].ClearFail("CreateVM")
	h.recordf("DrainNode(n1) retry after backend recovered")
	dr, err = h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && dr.Complete, "retry drain must succeed: %v %+v", err, dr.Results)
	vm3, _ := h.cp.GetVM(vm.ID)
	h.check(vm3.NodeID == "n2" && vm3.State == model.VMStateRunning,
		"vm must be Running on n2 after retry, got %s %s", vm3.NodeID, vm3.State)
	h.checkAccounting()
}

// TestDrainBackendUncertainRetry 目标后端结果不确定（实际已生效）：对账清理残留、
// 整体回滚、原因 BACKEND_UNCERTAIN；重试不重复占账、不残留重复后端实例。
func TestDrainBackendUncertainRetry(t *testing.T) {
	h := newDHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	h.addNode("n2", 4000, 4096, nil)
	ctx := context.Background()
	h.recordf("CreateVM(req=u)")
	vm, err := h.cp.CreateVM(ctx, dspec("u", 1000, 1024))
	h.check(err == nil, "create failed: %v", err)

	// 不确定但实际生效：模拟“目标已创建但响应丢失”。
	h.fakes["n2"].UncertainOn("CreateVM", true)
	h.recordf("DrainNode(n1) with uncertain target backend (effect applied)")
	dr, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain must not error, got %v", err)
	h.check(len(dr.Results) == 1 && !dr.Results[0].Moved &&
		dr.Results[0].Code == ReasonBackendUncertain,
		"result must be BACKEND_UNCERTAIN, got %+v", dr.Results)
	vm2, _ := h.cp.GetVM(vm.ID)
	h.check(vm2.NodeID == "n1" && vm2.State == model.VMStateStopped,
		"vm must roll back to source unchanged, got %s %s", vm2.NodeID, vm2.State)
	h.check(!h.fakes["n2"].Exists(vm.BackendID),
		"uncertain residue on target must be reconciled away")
	h.checkAccounting()

	h.fakes["n2"].ClearUncertain("CreateVM")
	h.recordf("DrainNode(n1) retry")
	dr, err = h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && dr.Complete, "retry must succeed: %v %+v", err, dr.Results)
	vm3, _ := h.cp.GetVM(vm.ID)
	h.check(vm3.NodeID == "n2", "vm must be on n2 after retry, got %s", vm3.NodeID)
	h.check(h.fakes["n2"].Exists(vm.BackendID) && !h.fakes["n1"].Exists(vm.BackendID),
		"exactly one backend copy must exist, on target")
	n2, _ := h.cp.NodeSnapshot("n2")
	h.check(n2.Allocated.MilliCPU == 1000,
		"retry must charge exactly once, n2 allocated=%+v", n2.Allocated)
	h.checkAccounting()
}

// TestDrainConcurrentCreateDelete 排空与并发创建/删除竞争：任何时刻不超卖、
// 不出现负账目，结束后全局账目对得上。
func TestDrainConcurrentCreateDelete(t *testing.T) {
	h := newDHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	h.addNode("n2", 8000, 8192, nil)
	h.addNode("n3", 8000, 8192, nil)
	ctx := context.Background()

	h.recordf("seed 2 vms on n1")
	for i := 0; i < 2; i++ {
		_, err := h.cp.CreateVM(ctx, dspec(fmt.Sprintf("seed-%d", i), 1000, 1024))
		h.check(err == nil, "seed create %d failed: %v", i, err)
	}

	var wg sync.WaitGroup
	h.recordf("concurrent: drain n1 (x2) + 12 creates + deletes of created vms")
	var createdMu sync.Mutex
	var created []string
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.cp.DrainNode(ctx, "n1"); err != nil {
				h.recordf("drain error: %v", err)
			}
		}()
	}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vm, err := h.cp.CreateVM(ctx, dspec(fmt.Sprintf("cc-%d", i), 1000, 1024))
			if err != nil {
				h.recordf("create cc-%d rejected: %v", i, err)
				return
			}
			if vm.NodeID == "n1" {
				h.recordf("create cc-%d landed on draining node n1", i)
			}
			createdMu.Lock()
			created = append(created, vm.ID)
			createdMu.Unlock()
			if i%2 == 0 {
				if err := h.cp.DeleteVM(ctx, vm.ID); err != nil {
					h.recordf("delete %s error: %v", vm.ID, err)
				}
			}
		}(i)
	}
	wg.Wait()

	h.recordf("final DrainNode(n1) to settle")
	dr, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "final drain failed: %v", err)
	h.check(dr.Complete, "n1 must be fully evacuated at the end, results=%+v", dr.Results)
	n1, _ := h.cp.NodeSnapshot("n1")
	h.check(n1.Allocated.IsZero(), "n1 must hold nothing after drain, got %+v", n1.Allocated)
	h.checkAccounting()

	// 每个后端实例必须恰好存在于其账目所在节点的后端上（无双占、无丢失）。
	for _, id := range created {
		vm, err := h.cp.GetVM(id)
		h.check(err == nil, "GetVM(%s) failed: %v", id, err)
		if vm.State == model.VMStateDeleted {
			continue
		}
		for _, nid := range h.nodeIDs {
			exists := h.fakes[nid].Exists(vm.BackendID)
			if nid == vm.NodeID {
				h.check(exists, "vm %s accounted on %s but missing in its backend", id, nid)
			} else {
				h.check(!exists, "vm %s accounted on %s but also present in backend %s (double)", id, vm.NodeID, nid)
			}
		}
	}
}
