package control

import (
	"fmt"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// fixtureOnN1 在 n1(大)、n2 上准备一个落在 n1 的实例。
func fixtureOnN1(t *testing.T) *multiHarness {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 16000, 16384)
	h.addNode("n2", 8000, 8192)
	return h
}

// TestDrainRepeatedIdempotent 重复排空幂等：成功搬走的实例不重复搬、不重复占账、
// 不重复调用后端；重试结果仍包含已疏散实例的逐实例记录。
func TestDrainRepeatedIdempotent(t *testing.T) {
	h := fixtureOnN1(t)
	ctx := testCtx
	vm := h.create("dup", 1000, 1024)
	h.check(vm.NodeID == "n1", "fixture: vm on n1, got %s", vm.NodeID)

	h.recordf("DrainNode(n1) #1")
	res1, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res1.Evacuated() == 1, "first drain must evacuate: %v %+v", err, res1)

	n2Creates := h.fakes["n2"].CountOp("CreateVM")
	n1Deletes := h.fakes["n1"].CountOp("DeleteVM")

	h.recordf("DrainNode(n1) #2 (idempotent replay)")
	res2, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "repeated drain must not error: %v", err)
	h.check(len(res2.Results) == 1 && res2.Results[0].Evacuated && res2.Results[0].ToNode == "n2",
		"replay must still report the evacuated vm, got %+v", res2.Results)
	h.check(h.fakes["n2"].CountOp("CreateVM") == n2Creates,
		"replay must not re-create on backend: before=%d after=%d",
		n2Creates, h.fakes["n2"].CountOp("CreateVM"))
	h.check(h.fakes["n1"].CountOp("DeleteVM") == n1Deletes,
		"replay must not re-delete on backend: before=%d after=%d",
		n1Deletes, h.fakes["n1"].CountOp("DeleteVM"))

	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n2" && got.State == model.VMStateStopped, "vm stays on n2 Stopped: %+v", got)
	h.assertBooksConsistent("after repeated drain")
	h.check(h.nodeAllocated("n1").IsZero(), "n1 stays empty, got %+v", h.nodeAllocated("n1"))
	h.check(h.nodeAllocated("n2") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}),
		"n2 charged exactly once, got %+v", h.nodeAllocated("n2"))

	// 排空期间新落在 n2 的实例，重复排空 n1 时不受影响、不被搬。
	vm2 := h.create("dup2", 1000, 1024)
	h.check(vm2.NodeID == "n2", "new vm lands on n2 while n1 draining, got %s", vm2.NodeID)
	res3, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res3.Evacuated() == 1 && res3.Remaining() == 0,
		"n1 drain result only tracks n1-origin vm, got %+v", res3)
	got2, _ := h.cp.GetVM(vm2.ID)
	h.check(got2.NodeID == "n2", "n2-native vm must not be moved by n1 drain, got %s", got2.NodeID)
	h.assertBooksConsistent("after third drain")

	// 结束维护后，累计结果清空；节点重新可放置。
	h.check(h.cp.EndMaintenance(ctx, "n1") == nil, "end maintenance")
	h.check(h.cp.EndMaintenance(ctx, "n1") == nil, "end maintenance replay stable")
}

// TestDrainRetryAfterCapacityFrees 首次因容量不足留下的实例，目标腾挪后重试可以搬走，
// 重试不触碰之前已成功搬移的实例。
func TestDrainRetryAfterCapacityFrees(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 16000, 16384)
	h.addNode("n2", 1500, 2000) // initially only one 1000m vm fits
	ctx := testCtx
	v1 := h.create("v1", 1000, 1024)
	v2 := h.create("v2", 1000, 1024)
	_ = v1

	res1, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res1.Evacuated() == 1 && res1.Remaining() == 1,
		"first drain moves one, strands one: %+v", res1)
	moved := res1.Results[0].VMID // sorted by vm id: vm-v1 first
	if !res1.Results[0].Evacuated {
		moved = res1.Results[1].VMID
	}

	// 删除已搬到 n2 的实例腾出空间，重试排空，剩下的实例搬走；此前搬走的不重复操作。
	h.recordf("DeleteVM(%s) to free n2", moved)
	h.check(h.cp.DeleteVM(ctx, moved) == nil, "delete moved vm")
	createsBefore := h.fakes["n2"].CountOp("CreateVM")
	res2, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res2.Evacuated() == 1 && res2.Remaining() == 0 && len(res2.Results) == 1,
		"retry reports only the live v2 (deleted v1 excluded from cumulative results), got %+v", res2)
	h.check(res2.Results[0].VMID == v2.ID && res2.Results[0].ToNode == "n2",
		"v2 must be the single live result moving to n2: %+v", res2.Results)
	gotV2, _ := h.cp.GetVM(v2.ID)
	h.check(gotV2.NodeID == "n2", "v2 must evacuate on retry, got %s", gotV2.NodeID)
	h.check(h.fakes["n2"].CountOp("CreateVM") == createsBefore+1,
		"retry creates only the newly moved vm")
	h.assertBooksConsistent("after successful retry")

	h.check(h.cp.EndMaintenance(ctx, "n1") == nil, "n1 fully drained -> maintenance can end")
}

// TestDrainRetryCumulativeResults 已搬走但仍存活的实例在重复排空结果中持续可见；
// 删除后不再出现。验证逐实例结果的幂等合并语义。
func TestDrainRetryCumulativeResults(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 16000, 16384)
	h.addNode("n2", 8000, 8192)
	ctx := testCtx
	v1 := h.create("w1", 1000, 1024)
	v2 := h.create("w2", 1000, 1024)

	r1, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && r1.Evacuated() == 2, "both move: %+v", r1)
	r2, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && len(r2.Results) == 2, "both still live -> both still reported, got %+v", r2)
	h.check(r2.Results[0].ToNode == "n2" && r2.Results[1].ToNode == "n2", "cumulative targets n2")

	h.check(h.cp.DeleteVM(ctx, v1.ID) == nil, "delete one evacuated vm")
	r3, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && len(r3.Results) == 1 && r3.Results[0].VMID == v2.ID,
		"deleted v1 drops out, only live v2 remains: %+v", r3)
	h.assertBooksConsistent("after cumulative result checks")
}

// TestDrainTargetCreateFailure 目标后端创建明确失败：实例回原位原状态，目标不残留，
// 账目完全回滚；故障清除后重试疏散成功。
func TestDrainTargetCreateFailure(t *testing.T) {
	h := fixtureOnN1(t)
	ctx := testCtx
	vm := h.create("bk1", 1000, 1024)

	h.fakes["n2"].FailOn("CreateVM", fmt.Errorf("hypervisor refused"))
	h.recordf("DrainNode(n1) with n2 CreateVM failing")
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain call ok: %v", err)
	h.check(res.Remaining() == 1 && res.Results[0].ReasonCode == ReasonBackend,
		"must strand with BACKEND_ERROR, got %+v", res.Results)

	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n1" && got.State == model.VMStateStopped,
		"vm must be back on n1 Stopped, got %s %s", got.NodeID, got.State)
	h.check(h.nodeAllocated("n1") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}),
		"source charge intact after rollback, got %+v", h.nodeAllocated("n1"))
	h.check(h.nodeAllocated("n2").IsZero(), "no provisional charge may remain on n2, got %+v",
		h.nodeAllocated("n2"))
	h.check(!h.fakes["n2"].Exists(vm.BackendID), "no residue on target backend")
	h.assertBooksConsistent("after target create failure")

	h.fakes["n2"].ClearFail("CreateVM")
	h.recordf("DrainNode(n1) retry after backend recovery")
	res2, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res2.Evacuated() == 1, "retry must succeed: %v %+v", err, res2)
	got, _ = h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n2", "vm moved on retry, got %s", got.NodeID)
	// 硬失败的那次调用未实际创建；重试后后端必须恰好存在一份（不重复创建同一实例）。
	h.check(h.fakes["n2"].Exists(vm.BackendID), "vm must exist on target after retry")
	h.check(h.nodeAllocated("n2") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}),
		"n2 charged exactly once after retry, got %+v", h.nodeAllocated("n2"))
	h.assertBooksConsistent("after retry")
}

// TestDrainTargetCreateUncertainEffectApplied 目标创建结果不确定但实际生效：
// 首次排空对账后清理残留并整体回滚；恢复后重试成功，全程无重复占账。
func TestDrainTargetCreateUncertainEffectApplied(t *testing.T) {
	h := fixtureOnN1(t)
	ctx := testCtx
	vm := h.create("unc1", 1000, 1024)

	h.fakes["n2"].UncertainOn("CreateVM", true) // response lost but created
	h.recordf("DrainNode(n1) with n2 CreateVM uncertain(effect applied)")
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res.Remaining() == 1, "one stranded result: %v %+v", err, res)
	h.check(res.Results[0].ReasonCode == ReasonBackendUncertain,
		"must be BACKEND_UNCERTAIN, got %s", res.Results[0].ReasonCode)

	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n1" && got.State == model.VMStateStopped,
		"vm reconciled back to n1 Stopped, got %s %s", got.NodeID, got.State)
	h.check(!h.fakes["n2"].Exists(vm.BackendID), "uncertain residue must be reconciled away")
	h.check(h.nodeAllocated("n1") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}) &&
		h.nodeAllocated("n2").IsZero(), "books fully rolled back: n1=%+v n2=%+v",
		h.nodeAllocated("n1"), h.nodeAllocated("n2"))
	h.assertBooksConsistent("after uncertain create")

	// 不确定效果关闭后重试：后端创建幂等，只占一次账。
	h.fakes["n2"].ClearFail("CreateVM")
	// Fake 没有单独关闭 Uncertain 的方法：重建 n2 的 fake 并替换注入。
	newFake := backend.NewFake(backend.CapabilitySet{
		Backend: "firecracker", Version: "1.7.0",
		Features: map[string]string{"snapshot": "1"},
	})
	h.fakes["n2"] = newFake
	res2, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res2.Evacuated() == 1, "retry after reconciliation must succeed: %v %+v", err, res2)
	got, _ = h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n2" && got.State == model.VMStateStopped, "vm on n2 Stopped: %+v", got)
	h.check(h.nodeAllocated("n2") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}),
		"n2 charged exactly once after retry, got %+v", h.nodeAllocated("n2"))
	h.assertBooksConsistent("after retried drain")
}

// TestDrainSourceDeleteUncertainStillPresent 源删除不确定、对账确认源仍存在：
// 整体回滚，目标侧被清理，实例留在源节点原状态。
func TestDrainSourceDeleteUncertainStillPresent(t *testing.T) {
	h := fixtureOnN1(t)
	ctx := testCtx
	vm := h.create("unc2", 1000, 1024)

	h.fakes["n1"].UncertainOn("DeleteVM", false) // uncertain, effect NOT applied
	h.recordf("DrainNode(n1) with n1 DeleteVM uncertain(no effect)")
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil, "drain call ok: %v", err)
	h.check(res.Remaining() == 1 && res.Results[0].ReasonCode == ReasonBackendUncertain,
		"must strand BACKEND_UNCERTAIN, got %+v", res)

	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n1" && got.State == model.VMStateStopped,
		"vm must stay on n1 Stopped, got %s %s", got.NodeID, got.State)
	h.check(h.fakes["n1"].Exists(vm.BackendID), "source backend instance must still exist")
	h.check(!h.fakes["n2"].Exists(vm.BackendID), "target side must be cleaned up on rollback")
	h.check(h.nodeAllocated("n1") == (model.Resources{MilliCPU: 1000, MemoryMB: 1024}) &&
		h.nodeAllocated("n2").IsZero(), "books rolled back: n1=%+v n2=%+v",
		h.nodeAllocated("n1"), h.nodeAllocated("n2"))
	h.assertBooksConsistent("after source uncertain-present")
}

// TestDrainSourceDeleteUncertainActuallyGone 源删除不确定但对账确认源已删除：
// 向前提交，实例收敛到目标节点的明确终态，不回滚造成双删/双占。
func TestDrainSourceDeleteUncertainActuallyGone(t *testing.T) {
	h := fixtureOnN1(t)
	ctx := testCtx
	vm := h.create("unc3", 1000, 1024)

	h.fakes["n1"].UncertainOn("DeleteVM", true) // effect applied: source really gone
	h.recordf("DrainNode(n1) with n1 DeleteVM uncertain(effect applied)")
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res.Evacuated() == 1,
		"must commit forward when source confirmed gone: %v %+v", err, res)
	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n2" && got.State == model.VMStateStopped,
		"vm converges on n2 Stopped, got %s %s", got.NodeID, got.State)
	h.check(h.fakes["n2"].Exists(vm.BackendID) && !h.fakes["n1"].Exists(vm.BackendID),
		"vm exists only on target backend")
	h.assertBooksConsistent("after source uncertain-gone commit")
}

// TestDrainTargetStartFailureRunningVM 运行中实例在目标启动失败：回滚，源仍 Running。
func TestDrainTargetStartFailureRunningVM(t *testing.T) {
	h := fixtureOnN1(t)
	ctx := testCtx
	vm := h.create("runbk", 1000, 1024)
	vm, err := h.cp.StartVM(ctx, vm.ID)
	h.check(err == nil && vm.State == model.VMStateRunning, "setup: %v", err)

	h.fakes["n2"].FailOn("StartVM", fmt.Errorf("kernel panic"))
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res.Remaining() == 1 && res.Results[0].ReasonCode == ReasonBackend,
		"start failure strands with BACKEND_ERROR: %v %+v", err, res)
	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.NodeID == "n1" && got.State == model.VMStateRunning,
		"running vm must be restored Running on n1, got %s %s", got.NodeID, got.State)
	// 目标创建已成功过，回滚必须把目标实例删掉。
	h.check(!h.fakes["n2"].Exists(vm.BackendID), "created target must be removed on rollback")
	h.assertBooksConsistent("after target start failure")
}
