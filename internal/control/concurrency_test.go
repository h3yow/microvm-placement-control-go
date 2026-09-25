package control

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// TestConcurrentPlacementNoOversell 并发生命周期操作下容量不得超卖。
func TestConcurrentPlacementNoOversell(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil) // 恰好容纳 4 个
	ctx := context.Background()

	const goroutines = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded, capacityRejected := 0, 0
	h.recordf("spawn %d concurrent CreateVM(1000m/1024MB) on node with capacity for 4", goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.cp.CreateVM(ctx, spec(fmt.Sprintf("p%d", i), 1000, 1024))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				succeeded++
			} else if CodeOf(err) == ReasonCapacity {
				capacityRejected++
			} else {
				h.recordf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	h.recordf("results: succeeded=%d capacityRejected=%d", succeeded, capacityRejected)

	h.check(succeeded == 4, "exactly 4 vms must succeed, got %d", succeeded)
	h.check(succeeded+capacityRejected == goroutines,
		"every failure must be CAPACITY_EXHAUSTED, succeeded=%d capacity=%d total=%d",
		succeeded, capacityRejected, goroutines)
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.MilliCPU == 4000 && n.Allocated.MemoryMB == 4096,
		"allocated must equal capacity exactly (no oversell), got %+v", n.Allocated)
	h.check(h.fake.CountOp("CreateVM") == 4,
		"backend CreateVM must be called exactly 4 times, got %d", h.fake.CountOp("CreateVM"))
}

// TestConcurrentDuplicateRequests 并发重复请求不得产生重复实例或重复扣减。
func TestConcurrentDuplicateRequests(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 8000, 8192, nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	ids := make([]string, 8)
	h.recordf("spawn 8 concurrent CreateVM with SAME request_id=dup")
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vm, err := h.cp.CreateVM(ctx, spec("dup", 1000, 1024))
			if err != nil {
				h.recordf("duplicate request error: %v", err)
				return
			}
			ids[i] = vm.ID
		}(i)
	}
	wg.Wait()
	first := ""
	for _, id := range ids {
		if id == "" {
			continue
		}
		if first == "" {
			first = id
		}
		h.check(id == first, "all replays must return same instance id, got %s and %s", first, id)
	}
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.MilliCPU == 1000,
		"duplicate requests must charge once, allocated=%d", n.Allocated.MilliCPU)
	h.check(h.fake.CountOp("CreateVM") == 1,
		"backend CreateVM must be called once, got %d", h.fake.CountOp("CreateVM"))
}

// TestNodeOffline 节点失联后拒绝新放置且原因可区分，已有实例账目保留。
func TestNodeOffline(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := context.Background()
	h.recordf("CreateVM(req=keep)")
	vm, err := h.cp.CreateVM(ctx, spec("keep", 1000, 1024))
	h.check(err == nil, "create failed: %v", err)

	h.recordf("SetNodeState(n1, Offline)  // 模拟失联")
	h.check(h.cp.SetNodeState("n1", model.NodeStateOffline) == nil, "set offline failed")

	h.recordf("CreateVM(req=new) expect NODE_UNAVAILABLE")
	_, err = h.cp.CreateVM(ctx, spec("new", 1000, 1024))
	h.check(CodeOf(err) == ReasonNodeUnavailable,
		"create on all-offline nodes must be NODE_UNAVAILABLE, got %v", err)

	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.MilliCPU == 1000,
		"offline node must retain accounting, allocated=%d", n.Allocated.MilliCPU)

	h.recordf("SetNodeState(n1, Ready)  // 恢复")
	h.check(h.cp.SetNodeState("n1", model.NodeStateReady) == nil, "set ready failed")
	h.recordf("CreateVM(req=after) expect success")
	_, err = h.cp.CreateVM(ctx, spec("after", 1000, 1024))
	h.check(err == nil, "create after recovery failed: %v", err)
	_ = vm
}

// TestBackendCreateFailure 后端创建失败：账目回滚，实例收敛到 Failed 终态。
func TestBackendCreateFailure(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := context.Background()
	h.fake.FailOn("CreateVM", fmt.Errorf("hypervisor refused"))

	h.recordf("CreateVM(req=f1) with backend CreateVM failing")
	vm, err := h.cp.CreateVM(ctx, spec("f1", 1000, 1024))
	h.check(CodeOf(err) == ReasonBackend, "must be BACKEND_ERROR, got %v", err)
	h.check(vm != nil && vm.State == model.VMStateFailed,
		"vm must converge to Failed terminal state, got %+v", vm)

	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.IsZero(),
		"resources must be rolled back, allocated=%+v", n.Allocated)

	// 故障恢复后，新请求（新 request_id）可成功，证明账目确实释放。
	h.fake.ClearFail("CreateVM")
	h.recordf("CreateVM(req=f2) after backend recovered")
	_, err = h.cp.CreateVM(ctx, spec("f2", 4000, 4096))
	h.check(err == nil, "full-capacity create after rollback must succeed, got %v", err)
}

// TestBackendCreateUncertain 后端结果不确定：对账清理残留，账目回滚，收敛到 Failed。
func TestBackendCreateUncertain(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := context.Background()
	// 不确定但实际生效：模拟"创建成功但响应丢失"。
	h.fake.UncertainOn("CreateVM", true)

	h.recordf("CreateVM(req=u1) with uncertain backend (effect applied)")
	vm, err := h.cp.CreateVM(ctx, spec("u1", 1000, 1024))
	h.check(CodeOf(err) == ReasonBackendUncertain, "must be BACKEND_UNCERTAIN, got %v", err)
	h.check(vm != nil && vm.State == model.VMStateFailed,
		"vm must converge to Failed, got %+v", vm)
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.IsZero(), "resources must be rolled back, allocated=%+v", n.Allocated)
	h.check(!h.fake.Exists(vm.BackendID),
		"backend residue must be reconciled away, but %s still exists", vm.BackendID)
}

// TestLifecycleStartStopDelete 启动/停止/删除的幂等性与失败收敛。
func TestLifecycleStartStopDelete(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := context.Background()
	h.recordf("CreateVM(req=l1)")
	vm, err := h.cp.CreateVM(ctx, spec("l1", 1000, 1024))
	h.check(err == nil, "create failed: %v", err)
	h.check(vm.State == model.VMStateStopped, "new vm must be Stopped, got %s", vm.State)

	h.recordf("StartVM(%s)", vm.ID)
	vm, err = h.cp.StartVM(ctx, vm.ID)
	h.check(err == nil && vm.State == model.VMStateRunning, "start failed: %v state=%s", err, vm.State)

	h.recordf("StartVM(%s) replay", vm.ID)
	vm2, err := h.cp.StartVM(ctx, vm.ID)
	h.check(err == nil && vm2.State == model.VMStateRunning, "start replay must be idempotent: %v", err)
	h.check(h.fake.CountOp("StartVM") == 1,
		"backend StartVM must be called once, got %d", h.fake.CountOp("StartVM"))

	// 停止失败：状态回到 Running，不留半完成态。
	h.fake.FailOn("StopVM", fmt.Errorf("agent unreachable"))
	h.recordf("StopVM(%s) with backend failing", vm.ID)
	vm, err = h.cp.StopVM(ctx, vm.ID)
	h.check(CodeOf(err) == ReasonBackend, "stop failure must be BACKEND_ERROR, got %v", err)
	h.check(vm.State == model.VMStateRunning,
		"failed stop must return to Running (no half-done state), got %s", vm.State)
	h.fake.ClearFail("StopVM")

	h.recordf("StopVM(%s) after recovery", vm.ID)
	vm, err = h.cp.StopVM(ctx, vm.ID)
	h.check(err == nil && vm.State == model.VMStateStopped, "stop failed: %v state=%s", err, vm.State)

	h.recordf("DeleteVM(%s)", vm.ID)
	h.check(h.cp.DeleteVM(ctx, vm.ID) == nil, "delete failed")
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.IsZero(), "delete must release all resources, allocated=%+v", n.Allocated)

	h.recordf("DeleteVM(%s) replay", vm.ID)
	h.check(h.cp.DeleteVM(ctx, vm.ID) == nil, "delete replay must be idempotent")
	h.check(h.fake.CountOp("DeleteVM") == 1,
		"backend DeleteVM must be called once, got %d", h.fake.CountOp("DeleteVM"))

	vm, err = h.cp.GetVM(vm.ID)
	h.check(err == nil && vm.State == model.VMStateDeleted,
		"deleted vm must be in Deleted terminal state, got %v %v", vm, err)
}

// TestStartFailureRetry 启动失败收敛到 Failed，重试不重复扣减且可成功。
func TestStartFailureRetry(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := context.Background()
	h.recordf("CreateVM(req=s1)")
	vm, err := h.cp.CreateVM(ctx, spec("s1", 1000, 1024))
	h.check(err == nil, "create failed: %v", err)

	h.fake.FailOn("StartVM", fmt.Errorf("kernel image missing"))
	h.recordf("StartVM(%s) with backend failing", vm.ID)
	vm, err = h.cp.StartVM(ctx, vm.ID)
	h.check(CodeOf(err) == ReasonBackend, "must be BACKEND_ERROR, got %v", err)
	h.check(vm.State == model.VMStateFailed, "vm must be Failed, got %s", vm.State)
	h.fake.ClearFail("StartVM")

	h.recordf("StartVM(%s) retry after recovery", vm.ID)
	vm, err = h.cp.StartVM(ctx, vm.ID)
	h.check(err == nil && vm.State == model.VMStateRunning,
		"retry must succeed without duplicate charge: %v state=%s", err, vm.State)
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.MilliCPU == 1000,
		"retry must not double-charge, allocated=%d", n.Allocated.MilliCPU)
}
