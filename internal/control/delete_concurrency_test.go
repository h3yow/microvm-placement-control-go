package control

import (
	"strconv"
	"sync"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// TestConcurrentDeleteReleasesOnce 多个并发删除同一实例：账目只能回收一次，
// 全局与节点已用量不得为负。回归删除“锁外调用、多次 release”的并发缺陷。
func TestConcurrentDeleteReleasesOnce(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := testCtx
	vm, err := h.cp.CreateVM(ctx, spec("dcdel", 1000, 1024))
	h.check(err == nil, "setup: %v", err)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h.cp.DeleteVM(ctx, vm.ID)
		}()
	}
	wg.Wait()

	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.IsZero(), "concurrent deletes must release exactly once, got %+v", n.Allocated)
	got, _ := h.cp.GetVM(vm.ID)
	h.check(got.State == model.VMStateDeleted, "vm ends Deleted, got %s", got.State)
}

// TestDrainInterleavedWithDelete 单个实例上排空与删除高频交错：账目恒成立，
// 实例要么仍在某个节点占一份账，要么已删除且账已回收，绝不出现负账或双占。
func TestDrainInterleavedWithDelete(t *testing.T) {
	for round := 0; round < 20; round++ {
		h := newMultiHarness(t, model.Resources{})
		h.addNode("n1", 16000, 16384)
		h.addNode("n2", 8000, 8192)
		ctx := testCtx
		sc := newSafeChecks()
		vm := h.create("inter"+strconv.Itoa(round), 1000, 1024)

		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _ = h.cp.DrainNode(ctx, "n1")
			h.invariantCheck("drain", sc)
		}()
		go func() {
			defer wg.Done()
			<-start
			for attempt := 0; attempt < 10; attempt++ {
				if err := h.cp.DeleteVM(ctx, vm.ID); err == nil || CodeOf(err) != ReasonInvalidState {
					break
				}
			}
			h.invariantCheck("delete", sc)
		}()
		close(start)
		wg.Wait()
		h.invariantCheck("final", sc)
		sc.verify(h)

		got, _ := h.cp.GetVM(vm.ID)
		if got.State != model.VMStateDeleted {
			h.check(got.NodeID == "n1" || got.NodeID == "n2", "live vm on valid node, got %s", got.NodeID)
		}
	}
}
