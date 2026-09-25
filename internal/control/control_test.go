package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

func res(cpu, mem int64) model.Resources { return model.Resources{MilliCPU: cpu, MemoryMB: mem} }

// dumpAudit 在断言失败时打印操作序列与判定依据。
func dumpAudit(t *testing.T, c *Controller) {
	t.Helper()
	t.Log("=== audit trail (op sequence & decision basis) ===")
	for _, e := range c.Audit().Events() {
		t.Logf("#%d op=%s vm=%s node=%s decision=%s reason=%s detail=%s",
			e.Seq, e.Op, e.VMID, e.NodeID, e.Decision, e.Reason, e.Detail)
	}
}

func newController(t *testing.T, quota model.Resources) (*Controller, map[string]*backend.Fake) {
	t.Helper()
	c := New(quota)
	fakes := map[string]*backend.Fake{}
	return c, fakes
}

func mustNode(t *testing.T, c *Controller, fakes map[string]*backend.Fake, id string, cap model.Resources, caps ...model.Capability) {
	t.Helper()
	f := backend.NewFake(2, caps...)
	if err := c.RegisterNode(id, cap, f); err != nil {
		t.Fatalf("register node %s: %v", id, err)
	}
	fakes[id] = f
}

func TestLifecycleHappyPath(t *testing.T) {
	c, fakes := newController(t, res(8000, 8192))
	mustNode(t, c, fakes, "n1", res(4000, 4096))

	vm, err := c.Create(context.Background(), CreateRequest{VMID: "vm1", Resources: res(1000, 1024)})
	if err != nil {
		dumpAudit(t, c)
		t.Fatalf("create: %v", err)
	}
	if vm.State != model.StateAllocated || vm.NodeID != "n1" {
		dumpAudit(t, c)
		t.Fatalf("unexpected vm after create: %+v", vm)
	}
	if err := c.Start(context.Background(), "vm1"); err != nil {
		dumpAudit(t, c)
		t.Fatalf("start: %v", err)
	}
	if got, _ := c.GetVM("vm1"); got.State != model.StateRunning {
		dumpAudit(t, c)
		t.Fatalf("want Running, got %s", got.State)
	}
	if err := c.Stop(context.Background(), "vm1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// 停止后资源保留
	if n, _ := c.NodeSnapshot("n1"); !n.Allocated.Fits(res(1000, 1024)) || n.Allocated.IsZero() {
		dumpAudit(t, c)
		t.Fatalf("stopped vm should retain resources, alloc=%+v", n.Allocated)
	}
	if err := c.Delete(context.Background(), "vm1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	n, _ := c.NodeSnapshot("n1")
	if !n.Allocated.IsZero() {
		dumpAudit(t, c)
		t.Fatalf("resources leaked after delete: %+v", n.Allocated)
	}
	if got, _ := c.GetVM("vm1"); got.State != model.StateDeleted {
		t.Fatalf("want Deleted, got %s", got.State)
	}
}

func TestDuplicateCreateIdempotent(t *testing.T) {
	c, fakes := newController(t, res(8000, 8192))
	mustNode(t, c, fakes, "n1", res(4000, 4096))
	ctx := context.Background()

	req := CreateRequest{VMID: "vm1", IdempotencyKey: "k1", Resources: res(1000, 1024)}
	v1, err := c.Create(ctx, req)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// 相同幂等键重放：返回同一实例，不重复占用
	v2, err := c.Create(ctx, CreateRequest{VMID: "vm1", IdempotencyKey: "k1", Resources: res(1000, 1024)})
	if err != nil || v2.ID != v1.ID {
		dumpAudit(t, c)
		t.Fatalf("idempotent replay failed: v2=%+v err=%v", v2, err)
	}
	if n, _ := c.NodeSnapshot("n1"); n.Allocated != res(1000, 1024) {
		dumpAudit(t, c)
		t.Fatalf("duplicate create double-allocated: %+v", n.Allocated)
	}
	// 相同 VMID 不同键：拒绝
	_, err = c.Create(ctx, CreateRequest{VMID: "vm1", IdempotencyKey: "k2", Resources: res(1000, 1024)})
	if model.ReasonOf(err) != model.ReasonAlreadyExists {
		t.Fatalf("want AlreadyExists, got %v", err)
	}
	// 重复 Start/Stop/Delete 幂等
	if err := c.Start(ctx, "vm1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Start(ctx, "vm1"); err != nil {
		t.Fatalf("re-start should be idempotent: %v", err)
	}
	if fakes["n1"].StartCalls("vm1") != 1 {
		t.Fatalf("backend start called %d times, want 1", fakes["n1"].StartCalls("vm1"))
	}
	if err := c.Stop(ctx, "vm1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := c.Stop(ctx, "vm1"); err != nil {
		t.Fatalf("re-stop should be idempotent: %v", err)
	}
	if err := c.Delete(ctx, "vm1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := c.Delete(ctx, "vm1"); err != nil {
		t.Fatalf("re-delete should be idempotent: %v", err)
	}
	if fakes["n1"].DeleteCalls("vm1") != 1 {
		t.Fatalf("backend delete called %d times, want 1", fakes["n1"].DeleteCalls("vm1"))
	}
}

func TestRejectReasonsDistinguishable(t *testing.T) {
	c, fakes := newController(t, res(2000, 2048))
	mustNode(t, c, fakes, "n1", res(4000, 4096))
	ctx := context.Background()

	// 配额不足
	_, err := c.Create(ctx, CreateRequest{VMID: "big", Resources: res(3000, 1024)})
	if model.ReasonOf(err) != model.ReasonQuotaExceeded {
		dumpAudit(t, c)
		t.Fatalf("want QuotaExceeded, got %v", err)
	}
	// 节点容量不足（配额够但单节点放不下）
	c2 := New(res(100000, 100000))
	mustNode(t, c2, fakes, "n2", res(1000, 1024))
	_, err = c2.Create(ctx, CreateRequest{VMID: "huge", Resources: res(2000, 1024)})
	if model.ReasonOf(err) != model.ReasonNoSuitableNode {
		dumpAudit(t, c2)
		t.Fatalf("want NoSuitableNode, got %v", err)
	}
	// 能力缺失
	_, err = c.Create(ctx, CreateRequest{VMID: "snap", Resources: res(500, 512),
		Constraints: Constraints{RequiredCaps: []model.Capability{model.CapSnapshot}}})
	if model.ReasonOf(err) != model.ReasonCapabilityMissing {
		dumpAudit(t, c)
		t.Fatalf("want CapabilityUnsupported, got %v", err)
	}
	// 版本不兼容
	if err := c.RegisterNode("oldnode", res(1000, 1024), backend.NewFake(0)); model.ReasonOf(err) != model.ReasonVersionIncompatible {
		t.Fatalf("want VersionIncompatible, got %v", err)
	}
	// 非法状态迁移
	if _, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(500, 512)}); err != nil {
		t.Fatalf("create vm1: %v", err)
	}
	if err := c.Stop(ctx, "vm1"); model.ReasonOf(err) != model.ReasonInvalidState {
		t.Fatalf("want InvalidState stopping Allocated vm, got %v", err)
	}
	if err := c.Start(ctx, "ghost"); model.ReasonOf(err) != model.ReasonNotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestBackendStartFailureRollsBack(t *testing.T) {
	c, fakes := newController(t, res(8000, 8192))
	mustNode(t, c, fakes, "n1", res(4000, 4096))
	ctx := context.Background()

	fakes["n1"].FailStart["vm1"] = errors.New("hypervisor exploded")
	if _, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(1000, 1024)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	err := c.Start(ctx, "vm1")
	if model.ReasonOf(err) != model.ReasonBackendFailure {
		dumpAudit(t, c)
		t.Fatalf("want BackendFailure, got %v", err)
	}
	vm, _ := c.GetVM("vm1")
	if vm.State != model.StateFailed || !vm.State.Terminal() {
		dumpAudit(t, c)
		t.Fatalf("want Failed terminal state, got %s", vm.State)
	}
	if n, _ := c.NodeSnapshot("n1"); !n.Allocated.IsZero() {
		dumpAudit(t, c)
		t.Fatalf("resources not rolled back: %+v", n.Allocated)
	}
	// 失败后配额应释放，可创建新实例
	if _, err := c.Create(ctx, CreateRequest{VMID: "vm2", Resources: res(4000, 4096)}); err != nil {
		dumpAudit(t, c)
		t.Fatalf("quota should be released after rollback: %v", err)
	}
}

func TestIndeterminateStartReconciles(t *testing.T) {
	c, fakes := newController(t, res(8000, 8192))
	mustNode(t, c, fakes, "n1", res(4000, 4096))
	ctx := context.Background()

	// 场景1：后端实际已启动但返回不确定 → 对账收敛到 Running，不重复扣减
	fakes["n1"].IndeterminateStart["vm1"] = true
	if _, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(1000, 1024)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Start(ctx, "vm1"); err != nil {
		dumpAudit(t, c)
		t.Fatalf("indeterminate start should reconcile to Running: %v", err)
	}
	if vm, _ := c.GetVM("vm1"); vm.State != model.StateRunning {
		dumpAudit(t, c)
		t.Fatalf("want Running after reconcile, got %s", vm.State)
	}
	if n, _ := c.NodeSnapshot("n1"); n.Allocated != res(1000, 1024) {
		dumpAudit(t, c)
		t.Fatalf("allocation must not double-count: %+v", n.Allocated)
	}
	// 重试 Start 幂等，不重复启动
	if err := c.Start(ctx, "vm1"); err != nil {
		t.Fatalf("retry start: %v", err)
	}
	if fakes["n1"].StartCalls("vm1") != 1 {
		t.Fatalf("backend start called %d times after retry, want 1", fakes["n1"].StartCalls("vm1"))
	}
}

func TestNodeLostConvergesToFailed(t *testing.T) {
	c, fakes := newController(t, res(8000, 8192))
	mustNode(t, c, fakes, "n1", res(4000, 4096))
	mustNode(t, c, fakes, "n2", res(4000, 4096))
	ctx := context.Background()

	if _, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(1000, 1024),
		Constraints: Constraints{AffinityNodes: []string{"n1"}}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Start(ctx, "vm1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.MarkNodeLost("n1"); err != nil {
		t.Fatalf("mark lost: %v", err)
	}
	vm, _ := c.GetVM("vm1")
	if vm.State != model.StateFailed {
		dumpAudit(t, c)
		t.Fatalf("vm on lost node must converge to Failed, got %s", vm.State)
	}
	if !strings.Contains(vm.LastError, "lost") {
		t.Fatalf("LastError should explain node loss, got %q", vm.LastError)
	}
	// 失联节点不再参与放置
	_, err := c.Create(ctx, CreateRequest{VMID: "vm2", Resources: res(1000, 1024),
		Constraints: Constraints{AffinityNodes: []string{"n1"}}})
	if model.ReasonOf(err) != model.ReasonNoSuitableNode {
		dumpAudit(t, c)
		t.Fatalf("lost node must be excluded from placement, got %v", err)
	}
	// 其他节点仍可放置
	if _, err := c.Create(ctx, CreateRequest{VMID: "vm3", Resources: res(1000, 1024),
		Constraints: Constraints{AffinityNodes: []string{"n2"}}}); err != nil {
		dumpAudit(t, c)
		t.Fatalf("healthy node should accept placement: %v", err)
	}
}

func TestCapabilityDegradationExplicit(t *testing.T) {
	c, fakes := newController(t, res(8000, 8192))
	mustNode(t, c, fakes, "n1", res(4000, 4096)) // 无 snapshot 能力
	ctx := context.Background()

	// 未声明降级：拒绝
	_, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(500, 512),
		Constraints: Constraints{RequiredCaps: []model.Capability{model.CapSnapshot}}})
	if model.ReasonOf(err) != model.ReasonCapabilityMissing {
		t.Fatalf("want CapabilityUnsupported, got %v", err)
	}
	// 显式声明降级：成功且结果稳定（Required 中不再含 snapshot）
	c.AllowDegraded[model.CapSnapshot] = true
	vm, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(500, 512),
		Constraints: Constraints{RequiredCaps: []model.Capability{model.CapSnapshot}}})
	if err != nil {
		dumpAudit(t, c)
		t.Fatalf("degraded create should succeed: %v", err)
	}
	if len(vm.Required) != 0 {
		t.Fatalf("degraded capability must be dropped from Required, got %v", vm.Required)
	}
	// 审计中必须有降级记录
	found := false
	for _, e := range c.Audit().Events() {
		if e.Decision == "capability degraded" {
			found = true
		}
	}
	if !found {
		dumpAudit(t, c)
		t.Fatal("degradation must be audited")
	}
}

func TestAntiAffinityAndSpread(t *testing.T) {
	c, fakes := newController(t, res(16000, 16384))
	mustNode(t, c, fakes, "n1", res(4000, 4096))
	mustNode(t, c, fakes, "n2", res(4000, 4096))
	ctx := context.Background()

	vm, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(1000, 1024),
		Constraints: Constraints{AntiAffinityNodes: []string{"n1"}}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if vm.NodeID != "n2" {
		dumpAudit(t, c)
		t.Fatalf("anti-affinity violated: placed on %s", vm.NodeID)
	}
	// spread：选择剩余容量最多的节点
	vm2, err := c.Create(ctx, CreateRequest{VMID: "vm2", Resources: res(1000, 1024),
		Constraints: Constraints{Spread: true}})
	if err != nil {
		t.Fatalf("create vm2: %v", err)
	}
	if vm2.NodeID != "n1" {
		dumpAudit(t, c)
		t.Fatalf("spread should pick emptier node n1, got %s", vm2.NodeID)
	}
}

func TestDeleteFailureKeepsStateAndRetry(t *testing.T) {
	c, fakes := newController(t, res(8000, 8192))
	mustNode(t, c, fakes, "n1", res(4000, 4096))
	ctx := context.Background()

	if _, err := c.Create(ctx, CreateRequest{VMID: "vm1", Resources: res(1000, 1024)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	fakes["n1"].FailDelete["vm1"] = fmt.Errorf("device busy")
	err := c.Delete(ctx, "vm1")
	if model.ReasonOf(err) != model.ReasonBackendFailure {
		t.Fatalf("want BackendFailure, got %v", err)
	}
	vm, _ := c.GetVM("vm1")
	if vm.State != model.StateAllocated {
		dumpAudit(t, c)
		t.Fatalf("failed delete must keep state, got %s", vm.State)
	}
	if n, _ := c.NodeSnapshot("n1"); n.Allocated != res(1000, 1024) {
		t.Fatalf("failed delete must not release resources: %+v", n.Allocated)
	}
	// 修复后端后重试成功
	delete(fakes["n1"].FailDelete, "vm1")
	if err := c.Delete(ctx, "vm1"); err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if n, _ := c.NodeSnapshot("n1"); !n.Allocated.IsZero() {
		dumpAudit(t, c)
		t.Fatalf("resources not released after retry: %+v", n.Allocated)
	}
}
