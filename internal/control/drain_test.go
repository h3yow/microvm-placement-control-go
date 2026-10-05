package control

import (
	"strings"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// TestDrainSuccess normal evacuation.
func TestDrainSuccess(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 16000, 16384) // initially freest -> first vms pile here
	h.addNode("n2", 8000, 8192)   // evac target with room for both vms
	ctx := testCtx

	vmA := h.create("a", 1000, 1024) // lands on n1 (freest node)
	vmB := h.create("b", 1000, 1024) // lands on n1 again (still freest after a)
	h.check(vmA.NodeID == "n1" && vmB.NodeID == "n1", "fixture: both vms should land on n1, got %s %s",
		vmA.NodeID, vmB.NodeID)
	h.assertBooksConsistent("initial")

	h.recordf("DrainNode(n1)")
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain failed: %v", err)
	h.check(res.Evacuated() == 2, "both vms must evacuate, results=%+v", res.Results)
	h.check(res.Remaining() == 0, "no vm may remain, results=%+v", res.Results)
	for _, r := range res.Results {
		h.check(r.Evacuated && r.FromNode == "n1" && r.ToNode == "n2",
			"vm %s must move n1->n2: %+v", r.VMID, r)
	}

	gotA, _ := h.cp.GetVM(vmA.ID)
	gotB, _ := h.cp.GetVM(vmB.ID)
	h.check(gotA.NodeID == "n2" && gotB.NodeID == "n2", "vms must be on n2 after drain: %s %s",
		gotA.NodeID, gotB.NodeID)
	h.check(gotA.State == model.VMStateStopped && gotB.State == model.VMStateStopped,
		"stopped vms stay stopped after move: %s %s", gotA.State, gotB.State)

	h.check(h.nodeAllocated("n1").IsZero(), "n1 allocated must be zero after drain, got %+v",
		h.nodeAllocated("n1"))
	h.check(h.nodeAllocated("n2") == (model.Resources{MilliCPU: 2000, MemoryMB: 2048}),
		"n2 must hold both vms, got %+v", h.nodeAllocated("n2"))
	h.check(h.fakes["n2"].CountOp("CreateVM") == 2, "n2 backend must see 2 creates, got %d",
		h.fakes["n2"].CountOp("CreateVM"))
	h.check(h.fakes["n1"].CountOp("DeleteVM") == 2, "n1 backend must see 2 deletes, got %d",
		h.fakes["n1"].CountOp("DeleteVM"))
	h.check(h.fakes["n2"].Exists(vmA.BackendID) && !h.fakes["n1"].Exists(vmA.BackendID),
		"evacuated vm must exist only on target backend")
	h.assertBooksConsistent("after drain")

	// Draining node rejects new placement.
	h.recordf("CreateVM(req=c) while n1 draining")
	vmC := h.create("c", 1000, 1024)
	h.check(vmC.NodeID == "n2", "new vm must avoid draining n1, got %s", vmC.NodeID)

	h.recordf("EndMaintenance(n1)")
	h.check(h.cp.EndMaintenance(ctx, "n1") == nil, "end maintenance failed")
	h.check(h.cp.EndMaintenance(ctx, "n1") == nil, "end maintenance replay must be idempotent")
	n1, _ := h.cp.NodeSnapshot("n1")
	h.check(n1.State == model.NodeStateReady, "n1 must be Ready again, got %s", n1.State)
	vmD := h.create("d", 1000, 1024)
	h.check(vmD.NodeID == "n1", "after maintenance n1 must accept vms again, got %s", vmD.NodeID)
	h.assertBooksConsistent("after maintenance")
}

// TestDrainRunningVMStatePreserved running vm must be running on target after move.
func TestDrainRunningVMStatePreserved(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 8000, 8192)
	h.addNode("n2", 8000, 8192)
	ctx := testCtx
	vm := h.create("run1", 1000, 1024) // lands on freest n1
	h.recordf("StartVM(%s)", vm.ID)
	vm, err := h.cp.StartVM(ctx, vm.ID)
	h.check(err == nil && vm.State == model.VMStateRunning, "setup start: %v %s", err, vm.State)

	h.recordf("DrainNode(n1)")
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res.Evacuated() == 1, "drain must evacuate running vm: %v %+v", err, res)
	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n2" && got.State == model.VMStateRunning,
		"running vm must run on n2 after move, got node=%s state=%s", got.NodeID, got.State)
	h.check(h.fakes["n2"].CountOp("StartVM") == 1, "target backend must start vm once, got %d",
		h.fakes["n2"].CountOp("StartVM"))
	h.assertBooksConsistent("after running drain")
}

// TestDrainTargetCapacityRejectRollback target too small: one vm moves, others stay put.
func TestDrainTargetCapacityRejectRollback(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 8000, 8192)
	h.addNode("n2", 1500, 2000) // fits one 1000m vm only
	ctx := testCtx
	vmA := h.create("a", 1000, 1024)
	vmB := h.create("b", 1000, 1024)
	vmC := h.create("c", 1000, 1024)
	h.assertBooksConsistent("before drain")

	h.recordf("DrainNode(n1) with n2 capacity for 1 only")
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain returns per-vm results, not aggregate error: %v", err)
	h.check(res.Evacuated() == 1, "exactly one vm can fit on n2, got %+v", res.Results)

	movedCount := 0
	for _, r := range res.Results {
		if r.Evacuated {
			movedCount++
			continue
		}
		h.check(r.ToNode == "n1" && r.State == string(model.VMStateStopped),
			"rejected vm must stay put unchanged: %+v", r)
		h.check(r.ReasonCode == ReasonCapacity,
			"rejection reason must be CAPACITY_EXHAUSTED, got %s (%s)", r.ReasonCode, r.Reason)
	}
	h.check(movedCount == 1, "exactly one moved, got %d", movedCount)

	for _, id := range []string{vmA.ID, vmB.ID, vmC.ID} {
		got, _ := h.cp.GetVM(id)
		if got.NodeID == "n1" {
			h.check(got.State == model.VMStateStopped, "stranded vm %s must keep Stopped state, got %s", id, got.State)
		}
	}
	h.check(h.nodeAllocated("n1") == (model.Resources{MilliCPU: 2000, MemoryMB: 2048}),
		"n1 must retain exactly the two stranded vms, got %+v", h.nodeAllocated("n1"))
	h.check(h.nodeAllocated("n2") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}),
		"n2 must hold exactly one vm, got %+v", h.nodeAllocated("n2"))
	for _, r := range res.Results {
		if !r.Evacuated {
			h.check(!h.fakes["n2"].Exists(r.VMID), "rejected vm %s must not exist on target backend", r.VMID)
		}
	}
	h.assertBooksConsistent("after partial drain")

	h.recordf("EndMaintenance(n1) expect rejection")
	err = h.cp.EndMaintenance(ctx, "n1")
	h.check(CodeOf(err) == ReasonInvalidState, "end maintenance with stranded vms must be INVALID_STATE, got %v", err)
}

// TestDrainCapabilityRejectAndDegrade capability mismatch rejects with distinct code;
// declaring accepted degradation allows the retry move.
func TestDrainCapabilityRejectAndDegrade(t *testing.T) {
	mkSpec := func(req string, degraded bool) model.VMSpec {
		s := spec(req, 1000, 1024)
		s.RequiredCaps = map[string]string{"backend_version": "1.7.0"}
		if degraded {
			s.AcceptDegraded = map[string]string{"backend_version": "1.5.0"}
		}
		return s
	}

	// 第一次：不接受降级，目标只有 1.5.0，拒绝且不动账。
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 16000, 16384) // freest so placement lands on 1.7.0 node
	h.addNode("n2", 8000, 8192, map[string]string{"backend_version": "1.5.0"})
	h.recordf("CreateVM(req=strict, requires 1.7.0)")
	vm, err := h.cp.CreateVM(testCtx, mkSpec("strict", false))
	h.check(err == nil && vm.NodeID == "n1", "create on capable n1: %v %+v", err, vm)

	res, err := h.cp.DrainNode(testCtx, "n1")
	h.check(err == nil, "drain call succeeds: %v", err)
	h.check(res.Remaining() == 1 && res.Results[0].ReasonCode == ReasonCapability,
		"must strand with CAPABILITY_MISMATCH, got %+v", res.Results)
	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n1" && got.State == model.VMStateStopped, "vm unchanged on n1: %s %s",
		got.NodeID, got.State)
	h.check(h.nodeAllocated("n1") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}),
		"stranded vm keeps charge, got %+v", h.nodeAllocated("n1"))
	h.assertBooksConsistent("after rejected drain")

	// 第二次：新夹具，显式声明接受 1.5.0 降级，疏散成功并记录降级事实。
	h2 := newMultiHarness(t, model.Resources{})
	h2.addNode("n1", 16000, 16384)
	h2.addNode("n2", 8000, 8192, map[string]string{"backend_version": "1.5.0"})
	h2.recordf("CreateVM(req=flex, requires 1.7.0 accepts 1.5.0)")
	vm2, err := h2.cp.CreateVM(testCtx, mkSpec("flex", true))
	h2.check(err == nil && vm2.NodeID == "n1", "create on n1: %v %+v", err, vm2)
	res2, err := h2.cp.DrainNode(testCtx, "n1")
	h2.check(err == nil && res2.Evacuated() == 1, "degraded move must succeed: %v %+v", err, res2)
	moved, _ := h2.cp.GetVM(vm2.ID)
	h2.check(moved.NodeID == "n2" && strings.Contains(moved.Reason, "degraded"),
		"degradation must be recorded on vm, got %+v", moved)
	h2.assertBooksConsistent("after degraded drain")
}

// TestDrainAntiAffinityReject same anti-affinity key cannot land together on target.
func TestDrainAntiAffinityReject(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 8000, 8192)
	h.addNode("n2", 8000, 8192)
	ctx := testCtx

	// anchor 先落在 n2，占用反亲和键 db；n1 上的 db 实例将无处可去。
	anchor := spec("anchor", 1000, 1024)
	anchor.AntiAffinityKey = "db"
	// 让 anchor 落 n2：先排空 n1 前直接制造——更简单：先建第一个 vm 落 n1，
	// 再建 anchor 时只剩 n2 可满足反亲和（n1 已有同键）。
	v1s := spec("db1", 1000, 1024)
	v1s.AntiAffinityKey = "db"
	v1, err := h.cp.CreateVM(ctx, v1s)
	h.check(err == nil && v1.NodeID == "n1", "db1 should land n1: %v %+v", err, v1)
	v2, err := h.cp.CreateVM(ctx, anchor)
	h.check(err == nil && v2.NodeID == "n2", "anchor should land n2: %v %+v", err, v2)

	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain ok: %v", err)
	h.check(res.Remaining() == 1 && res.Results[0].ReasonCode == ReasonConstraint,
		"anti-affinity must yield CONSTRAINT_VIOLATED, got %+v", res.Results)
	h.assertBooksConsistent("after anti-affinity drain")
}

// TestDrainNoAlternativeNode no ready alternative yields NODE_UNAVAILABLE.
func TestDrainNoAlternativeNode(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096)
	h.addNode("n2", 4000, 4096)
	h.check(h.cp.SetNodeState("n2", model.NodeStateOffline) == nil, "offline n2")
	h.create("x", 1000, 1024)

	res, err := h.cp.DrainNode(testCtx, "n1")
	h.check(err == nil, "drain call ok: %v", err)
	h.check(res.Remaining() == 1 && res.Results[0].ReasonCode == ReasonNodeUnavailable,
		"must be NODE_UNAVAILABLE, got %+v", res.Results)
}

// TestDrainUnknownNode unknown node yields NOT_FOUND.
func TestDrainUnknownNode(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	_, err := h.cp.DrainNode(testCtx, "ghost")
	h.check(CodeOf(err) == ReasonNotFound, "must be NOT_FOUND, got %v", err)
	err = h.cp.EndMaintenance(testCtx, "ghost")
	h.check(CodeOf(err) == ReasonNotFound, "end maintenance must be NOT_FOUND, got %v", err)
}
