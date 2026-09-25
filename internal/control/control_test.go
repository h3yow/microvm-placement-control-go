package control

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// harness 记录操作序列，断言失败时打印操作序列与判定依据。
type harness struct {
	t    *testing.T
	cp   *ControlPlane
	fake *backend.Fake
	mu   sync.Mutex
	ops  []string
}

func newHarness(t *testing.T, quota model.Resources) *harness {
	t.Helper()
	fake := backend.NewFake(backend.CapabilitySet{
		Backend: "firecracker", Version: "1.7.0",
		Features: map[string]string{"snapshot": "1"},
	})
	h := &harness{t: t, fake: fake}
	h.cp = New(quota, func(*model.Node) backend.Backend { return fake })
	return h
}

func (h *harness) recordf(format string, args ...any) {
	h.mu.Lock()
	h.ops = append(h.ops, fmt.Sprintf(format, args...))
	h.mu.Unlock()
}

// check 判定断言；失败时打印完整操作序列、判定依据与审计日志。
func (h *harness) check(cond bool, rationale string, args ...any) {
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

func (h *harness) addNode(id string, cpu, mem int64, caps map[string]string) {
	h.recordf("RegisterNode(%s cpu=%d mem=%d caps=%v)", id, cpu, mem, caps)
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
}

func spec(reqID string, cpu, mem int64) model.VMSpec {
	return model.VMSpec{
		RequestID: reqID,
		Resources: model.Resources{MilliCPU: cpu, MemoryMB: mem},
	}
}

func TestCreateIdempotentReplay(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := context.Background()

	h.recordf("CreateVM(req=r1)")
	vm1, err := h.cp.CreateVM(ctx, spec("r1", 1000, 1024))
	h.check(err == nil, "first create failed: %v", err)
	h.recordf("CreateVM(req=r1) replay")
	vm2, err := h.cp.CreateVM(ctx, spec("r1", 1000, 1024))
	h.check(err == nil, "replay create failed: %v", err)

	h.check(vm1.ID == vm2.ID, "replay must return same instance, got %s vs %s", vm1.ID, vm2.ID)
	h.check(h.fake.CountOp("CreateVM") == 1,
		"backend CreateVM must be called once, got %d (idempotency violated)", h.fake.CountOp("CreateVM"))
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.MilliCPU == 1000,
		"allocated must be counted once (1000), got %d (double charge)", n.Allocated.MilliCPU)

	h.recordf("CreateVM(req=r1) with different spec")
	_, err = h.cp.CreateVM(ctx, spec("r1", 2000, 1024))
	h.check(CodeOf(err) == ReasonConflict, "same request_id with different spec must be CONFLICT, got %v", err)
}

func TestCapacityExhausted(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, nil)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		h.recordf("CreateVM(req=c%d)", i)
		_, err := h.cp.CreateVM(ctx, spec(fmt.Sprintf("c%d", i), 1000, 1024))
		h.check(err == nil, "create %d must succeed, got %v", i, err)
	}
	h.recordf("CreateVM(req=c4) expect capacity rejection")
	_, err := h.cp.CreateVM(ctx, spec("c4", 1000, 1024))
	h.check(CodeOf(err) == ReasonCapacity,
		"5th vm must be rejected with CAPACITY_EXHAUSTED, got %v", err)
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.MilliCPU == 4000 && n.Allocated.MemoryMB == 4096,
		"allocated must equal exactly 4 vms, got %+v (oversell or leak)", n.Allocated)
}

func TestQuotaExceeded(t *testing.T) {
	h := newHarness(t, model.Resources{MilliCPU: 1500, MemoryMB: 4096})
	h.addNode("n1", 8000, 8192, nil)
	ctx := context.Background()
	h.recordf("CreateVM(req=q0) 1000m")
	_, err := h.cp.CreateVM(ctx, spec("q0", 1000, 1024))
	h.check(err == nil, "first create failed: %v", err)
	h.recordf("CreateVM(req=q1) 1000m expect quota rejection")
	_, err = h.cp.CreateVM(ctx, spec("q1", 1000, 1024))
	h.check(CodeOf(err) == ReasonQuota,
		"second vm must be rejected with QUOTA_EXCEEDED, got %v", err)
	n, _ := h.cp.NodeSnapshot("n1")
	h.check(n.Allocated.MilliCPU == 1000,
		"rejected request must not charge node, allocated=%d", n.Allocated.MilliCPU)
}

func TestCapabilityMismatchAndDegradation(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 4000, 4096, map[string]string{"backend_version": "1.5.0"})
	ctx := context.Background()

	s := spec("cap1", 1000, 1024)
	s.RequiredCaps = map[string]string{"backend_version": "1.7.0"}
	h.recordf("CreateVM(require backend_version=1.7.0, node has 1.5.0)")
	_, err := h.cp.CreateVM(ctx, s)
	h.check(CodeOf(err) == ReasonCapability,
		"unavailable capability must be CAPABILITY_MISMATCH, got %v", err)

	// 显式声明安全降级：接受 1.5.0。
	s.RequestID = "cap2"
	s.AcceptDegraded = map[string]string{"backend_version": "1.5.0"}
	h.recordf("CreateVM(require 1.7.0, accept degraded 1.5.0)")
	vm, err := h.cp.CreateVM(ctx, s)
	h.check(err == nil, "declared degradation must be accepted, got %v", err)
	h.check(strings.Contains(vm.Reason, "degraded"),
		"degraded placement must be recorded, reason=%q", vm.Reason)

	// 版本兼容：1.* 与 1.5.0 主版本一致，无需降级声明。
	s2 := spec("cap3", 1000, 1024)
	s2.RequiredCaps = map[string]string{"backend_version": "1.*"}
	h.recordf("CreateVM(require backend_version=1.*)")
	_, err = h.cp.CreateVM(ctx, s2)
	h.check(err == nil, "major-version compatible requirement must succeed, got %v", err)
}

func TestAntiAffinity(t *testing.T) {
	h := newHarness(t, model.Resources{})
	h.addNode("n1", 8000, 8192, nil)
	ctx := context.Background()
	s := spec("aa1", 1000, 1024)
	s.AntiAffinityKey = "db"
	h.recordf("CreateVM(aa=db) on single node")
	_, err := h.cp.CreateVM(ctx, s)
	h.check(err == nil, "first anti-affinity vm failed: %v", err)
	s.RequestID = "aa2"
	h.recordf("CreateVM(aa=db) again on same single node")
	_, err = h.cp.CreateVM(ctx, s)
	h.check(CodeOf(err) == ReasonConstraint,
		"second vm with same anti-affinity key must be CONSTRAINT_VIOLATED, got %v", err)
}
