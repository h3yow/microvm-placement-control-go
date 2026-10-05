// control-demo 是一个纯本地、无外部依赖的控制面演示：
// 使用内存伪后端完成节点登记、放置、生命周期操作与故障注入，并打印审计日志。
package main

import (
	"context"
	"fmt"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/control"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

func main() {
	fake := backend.NewFake(backend.CapabilitySet{
		Backend: "firecracker", Version: "1.7.0",
		Features: map[string]string{"snapshot": "1"},
	})
	cp := control.New(model.Resources{MilliCPU: 8000, MemoryMB: 8192},
		func(*model.Node) backend.Backend { return fake })
	ctx := context.Background()

	must := func(err error) {
		if err != nil {
			fmt.Println("error:", err)
		}
	}

	caps := map[string]string{"backend": "firecracker", "backend_version": "1.7.0", "snapshot": "1"}
	must(cp.RegisterNode(model.Node{ID: "node-a", Capacity: model.Resources{MilliCPU: 4000, MemoryMB: 4096}, Capabilities: caps}))
	must(cp.RegisterNode(model.Node{ID: "node-b", Capacity: model.Resources{MilliCPU: 4000, MemoryMB: 4096}, Capabilities: caps}))

	spec := model.VMSpec{
		RequestID: "demo-1",
		Resources: model.Resources{MilliCPU: 1000, MemoryMB: 1024},
	}
	vm, err := cp.CreateVM(ctx, spec)
	must(err)
	fmt.Printf("created %s on %s state=%s\n", vm.ID, vm.NodeID, vm.State)

	// 幂等重放：同一 RequestID 返回同一实例，不重复扣减。
	vm2, _ := cp.CreateVM(ctx, spec)
	fmt.Printf("replay returned %s (same=%v)\n", vm2.ID, vm2.ID == vm.ID)

	vm, _ = cp.StartVM(ctx, vm.ID)
	fmt.Printf("started %s state=%s\n", vm.ID, vm.State)

	// 注入后端故障：停止失败，状态回到 Running。
	fake.FailOn("StopVM", fmt.Errorf("agent unreachable"))
	vm, err = cp.StopVM(ctx, vm.ID)
	fmt.Printf("stop with backend failure: err=%v state=%s\n", err, vm.State)
	fake.ClearFail("StopVM")

	vm, _ = cp.StopVM(ctx, vm.ID)
	fmt.Printf("stopped %s state=%s\n", vm.ID, vm.State)

	// 节点维护排空：node-a 进入维护，实例被疏散到其它节点。
	vm2spec := model.VMSpec{RequestID: "demo-2", Resources: model.Resources{MilliCPU: 1000, MemoryMB: 1024}}
	vmb, err := cp.CreateVM(ctx, vm2spec)
	must(err)
	fmt.Printf("\ncreated %s on %s\n", vmb.ID, vmb.NodeID)
	dr, err := cp.DrainNode(ctx, "node-a")
	must(err)
	for _, r := range dr.Results {
		fmt.Printf("evacuate %s: moved=%v to=%s code=%s %s\n", r.VMID, r.Moved, r.ToNode, r.Code, r.Detail)
	}
	fmt.Printf("drain node-a complete=%v\n", dr.Complete)
	na, _ := cp.NodeSnapshot("node-a")
	fmt.Printf("node-a state=%s allocated=%+v\n", na.State, na.Allocated)
	must(cp.EndMaintenance("node-a"))
	na, _ = cp.NodeSnapshot("node-a")
	fmt.Printf("node-a state=%s after maintenance\n", na.State)

	must(cp.DeleteVM(ctx, vm.ID))
	must(cp.DeleteVM(ctx, vmb.ID))

	n, _ := cp.NodeSnapshot(vm.NodeID)
	fmt.Printf("node %s allocated after delete: %+v\n", n.ID, n.Allocated)

	fmt.Println("\n=== audit log ===")
	for _, e := range cp.Audit() {
		fmt.Printf("#%-2d %-18s vm=%-10s node=%-7s %s\n", e.Seq, e.Op, e.VMID, e.NodeID, e.Detail)
	}
}
