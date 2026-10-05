package control

import (
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// safeChecks 在 goroutine 间线程安全地收集致命问题与操作序列，
// 避免在子 goroutine 内直接调用 t.Fatalf（只会 Goexit 该 goroutine，导致主测试挂起）。
type safeChecks struct {
	mu   sync.Mutex
	errs []string
}

func newSafeChecks() *safeChecks { return &safeChecks{} }

func (s *safeChecks) addf(format string, args ...any) {
	s.mu.Lock()
	s.errs = append(s.errs, fmt.Sprintf(format, args...))
	s.mu.Unlock()
}

func (s *safeChecks) failIf(cond bool, format string, args ...any) {
	if cond {
		s.addf(format, args...)
	}
}

func (s *safeChecks) verify(h *multiHarness) {
	h.t.Helper()
	if len(s.errs) == 0 {
		return
	}
	h.t.Logf("=== concurrent failures (%d) ===", len(s.errs))
	for _, e := range s.errs {
		h.t.Logf("  - %s", e)
	}
	h.t.Logf("=== audit tail ===")
	for _, e := range h.cp.Audit() {
		h.t.Logf("  #%d %s vm=%s node=%s %s", e.Seq, e.Op, e.VMID, e.NodeID, e.Detail)
	}
	h.t.Fatalf("%d concurrent invariant failures", len(s.errs))
}

// invariantCheck 在任意阶段核对账目恒等式（供主/子 goroutine 调用，问题写入 sc）。
func (h *multiHarness) invariantCheck(stage string, sc *safeChecks) {
	h.t.Helper()
	cp := h.cp
	cp.mu.Lock()
	defer cp.mu.Unlock()
	var nodeSum model.Resources
	for id, n := range cp.nodes {
		sc.failIf(n.Allocated.MilliCPU < 0 || n.Allocated.MemoryMB < 0,
			"[%s] node %s allocated negative: %+v", stage, id, n.Allocated)
		sc.failIf(!n.Allocated.Fits(n.Capacity),
			"[%s] node %s oversold: %+v > %+v", stage, id, n.Allocated, n.Capacity)
		nodeSum = nodeSum.Add(n.Allocated)
	}
	sc.failIf(nodeSum != cp.used,
		"[%s] global used %+v != node sum %+v", stage, cp.used, nodeSum)
	if !cp.quota.IsZero() {
		sc.failIf(!cp.used.Fits(cp.quota), "[%s] quota breached: %+v > %+v", stage, cp.used, cp.quota)
	}
	perNode := map[string]model.Resources{}
	for _, vm := range cp.vms {
		if vm.State == model.VMStateDeleted {
			continue
		}
		perNode[vm.NodeID] = perNode[vm.NodeID].Add(vm.Spec.Resources)
		if vm.State == model.VMStateMigrating {
			m := cp.moving[vm.ID]
			sc.failIf(m == nil, "[%s] migrating vm %s has no moveCtx", stage, vm.ID)
			if m != nil {
				sc.failIf(m.to != vm.NodeID,
					"[%s] migrating vm %s book drift: move.to=%s node=%s", stage, vm.ID, m.to, vm.NodeID)
			}
		}
	}
	for id, n := range cp.nodes {
		sc.failIf(perNode[id] != n.Allocated,
			"[%s] node %s books mismatch want=%+v got=%+v", stage, id, perNode[id], n.Allocated)
	}
}

// TestDrainConcurrentWithCreateAndDelete 排空与并发创建/删除交错：
// 账目恒等式始终成立，不出现负数/重复回收/双节点占账。
func TestDrainConcurrentWithCreateAndDelete(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 100000, 102400)
	h.addNode("n2", 100000, 102400)
	ctx := testCtx
	sc := newSafeChecks()

	const initial = 16
	var seed []string
	for i := 0; i < initial; i++ {
		vm := h.create(fmt.Sprintf("seed%d", i), 1000, 1024)
		seed = append(seed, vm.ID)
	}
	h.invariantCheck("seed", sc)
	sc.verify(h)

	const creators, deleters, drainRounds = 8, 6, 20
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for round := 0; round < drainRounds; round++ {
			res, err := h.cp.DrainNode(ctx, "n1")
			if err != nil {
				sc.addf("drain round %d error: %v", round, err)
				return
			}
			if res.Remaining() == 0 {
				if err := h.cp.EndMaintenance(ctx, "n1"); err != nil && CodeOf(err) != ReasonInvalidState {
					// INVALID_STATE：排空后、结束前又有并发创建落回 n1，属预期竞争，下一轮再排。
					sc.addf("end maintenance round %d: %v", round, err)
				}
			}
			h.invariantCheck(fmt.Sprintf("drain round %d", round), sc)
		}
	}()

	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 10; j++ {
				req := fmt.Sprintf("c%d-%d", i, j)
				vm, err := h.cp.CreateVM(ctx, spec(req, 500, 512))
				switch {
				case err == nil:
					h.invariantCheck(fmt.Sprintf("create %s on %s", req, vm.NodeID), sc)
				case CodeOf(err) == ReasonCapacity || CodeOf(err) == ReasonInvalidState:
					// 可接受的运行期拒绝。
				default:
					sc.addf("create %s unexpected error: %v", req, err)
				}
			}
		}(i)
	}

	for i := 0; i < deleters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := seed[i%len(seed)]
			for attempt := 0; attempt < 50; attempt++ {
				err := h.cp.DeleteVM(ctx, id)
				if err == nil {
					h.invariantCheck(fmt.Sprintf("delete %s", id), sc)
					return
				}
				if CodeOf(err) != ReasonInvalidState {
					sc.addf("delete %s unexpected error: %v", id, err)
					return
				}
				// Migrating 中：稍后重试。
			}
			sc.addf("delete %s gave up after migrating retries", id)
		}(i)
	}

	close(start)
	wg.Wait()
	h.invariantCheck("final", sc)
	sc.verify(h)

	// 无 Migrating 悬挂、无残留 moveCtx。
	cp := h.cp
	cp.mu.Lock()
	for id, vm := range cp.vms {
		h.check(vm.State != model.VMStateMigrating, "vm %s stuck Migrating", id)
		h.check(vm.NodeID == "n1" || vm.NodeID == "n2", "vm %s on unknown node %s", id, vm.NodeID)
	}
	h.check(len(cp.moving) == 0, "no in-flight move may remain, got %d", len(cp.moving))
	cp.mu.Unlock()

	// 每个未删除实例恰好存在于当前节点后端，绝不同时存在于两个后端。
	for id := range h.cp.vms {
		vm, _ := h.cp.GetVM(id)
		if vm.State == model.VMStateDeleted {
			continue
		}
		h.check(h.fakes[vm.NodeID].Exists(vm.BackendID),
			"vm %s must exist on its current node %s", id, vm.NodeID)
		other := "n1"
		if vm.NodeID == "n1" {
			other = "n2"
		}
		h.check(!h.fakes[other].Exists(vm.BackendID), "vm %s must not also exist on %s (dual residency)", id, other)
	}
}

// TestDrainTwoNodesConcurrent 两个节点同时排空，唯一接纳节点只放得下一个：
// 临时占用必须计入容量判定，恰好一个搬走、另一个被容量原因拒绝。
func TestDrainTwoNodesConcurrent(t *testing.T) {
	h := newMultiHarness(t, model.Resources{})
	h.addNode("n1", 16000, 16384)
	h.addNode("n2", 16000, 16384)
	h.addNode("n3", 1500, 2000)
	ctx := testCtx
	v1 := h.create("m1", 1000, 1024)
	v2 := h.create("m2", 1000, 1024)
	h.check(v1.NodeID == "n1" && v2.NodeID == "n2", "fixture: v1 on %s, v2 on %s",
		v1.NodeID, v2.NodeID)

	var wg sync.WaitGroup
	var r1, r2 *DrainResult
	var e1, e2 error
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		r1, e1 = h.cp.DrainNode(ctx, "n1")
	}()
	go func() {
		defer wg.Done()
		<-start
		r2, e2 = h.cp.DrainNode(ctx, "n2")
	}()
	close(start)
	wg.Wait()
	h.check(e1 == nil && e2 == nil, "both drains return: %v %v", e1, e2)

	// 两个排空可能串行交错：先排空的节点可能把实例临时放到另一个仍 Ready、
	// 但随后也进入排空的节点（链式疏散）。因此只断言不变量，不断言固定落点。
	moved := 0
	for source, r := range map[string]*DrainResult{"n1": r1, "n2": r2} {
		for _, x := range r.Results {
			if x.Evacuated {
				moved++
				h.check(x.ToNode != source && x.ToNode != "",
					"evacuated vm must land on another ready node, source=%s got %s", source, x.ToNode)
			} else {
				h.check(x.ReasonCode == ReasonCapacity || x.ReasonCode == ReasonNodeUnavailable,
					"stranded reason must be capacity/unavailable, got %s (%s)", x.ReasonCode, x.Reason)
			}
		}
	}
	h.check(moved >= 1 && moved <= 2, "chained evacuation moves 1~2 vms, got %d", moved)
	h.check(h.nodeAllocated("n3").MilliCPU <= 1000,
		"n3 must never oversell its 1500m capacity, got %+v", h.nodeAllocated("n3"))
	h.assertBooksConsistent("after concurrent drains")

	g1, _ := h.cp.GetVM(v1.ID)
	g2, _ := h.cp.GetVM(v2.ID)
	h.check(g1.NodeID != g2.NodeID, "two vms must never co-locate: %s %s", g1.NodeID, g2.NodeID)
	// 通用双后端共存检查：任一 backendID 在全部伪后端中至多存在一份。
	for _, bid := range []string{v1.BackendID, v2.BackendID} {
		count := 0
		for _, f := range h.fakes {
			if f.Exists(bid) {
				count++
			}
		}
		h.check(count <= 1, "backend id %s exists in %d backends (dual residency)", bid, count)
	}
}

// TestDrainConcurrentDeleteOfAlreadyEvacuated 已搬走实例的并发删除只回收一次账。
func TestDrainConcurrentDeleteOfAlreadyEvacuated(t *testing.T) {
	h := fixtureOnN1(t)
	ctx := testCtx
	vm := h.create("cd1", 1000, 1024)
	res, err := h.cp.DrainNode(ctx, "n1")
	h.check(err == nil && res.Evacuated() == 1, "drain setup: %v", err)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h.cp.DeleteVM(ctx, vm.ID)
		}()
	}
	wg.Wait()
	h.check(h.nodeAllocated("n2").IsZero(), "post-drain delete releases exactly once, got %+v",
		h.nodeAllocated("n2"))
	h.assertBooksConsistent("after concurrent post-drain delete")
}
