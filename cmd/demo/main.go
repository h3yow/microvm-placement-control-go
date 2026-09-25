// demo 在本地完整演示控制面：登记节点、并发创建、失败回滚、节点失联与审计导出。
// 不依赖任何真实虚拟化平台、外部服务或凭据。
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/h3yow/microvm-placement-control-go/internal/backend"
	"github.com/h3yow/microvm-placement-control-go/internal/control"
	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

func main() {
	c := control.New(model.Resources{MilliCPU: 100000, MemoryMB: 102400})
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	must(c.RegisterNode("node-a", model.Resources{MilliCPU: 4000, MemoryMB: 4096},
		backend.NewFake(2, model.CapSnapshot)))
	must(c.RegisterNode("node-b", model.Resources{MilliCPU: 4000, MemoryMB: 4096},
		backend.NewFake(2)))

	ctx := context.Background()

	// 并发创建 8 个实例（每节点容量 4 个，第 9 个起会被拒绝）
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("vm-%d", i)
			_, err := c.Create(ctx, control.CreateRequest{
				VMID:        id,
				Resources:   model.Resources{MilliCPU: 1000, MemoryMB: 1024},
				Constraints: control.Constraints{Spread: true},
			})
			if err != nil {
				fmt.Printf("create %s rejected: %v\n", id, err)
				return
			}
			if err := c.Start(ctx, id); err != nil {
				fmt.Printf("start %s failed: %v\n", id, err)
			}
		}(i)
	}
	wg.Wait()

	// 容量耗尽后的拒绝（原因可区分）
	_, err := c.Create(ctx, control.CreateRequest{VMID: "vm-overflow",
		Resources: model.Resources{MilliCPU: 1000, MemoryMB: 1024}})
	fmt.Printf("overflow create -> %v\n", err)

	// 节点失联：其上实例收敛到 Failed
	must(c.MarkNodeLost("node-a"))

	fmt.Println("\n=== final state ===")
	for i := 0; i < 8; i++ {
		if vm, ok := c.GetVM(fmt.Sprintf("vm-%d", i)); ok {
			fmt.Printf("%s node=%s state=%s lastErr=%q\n", vm.ID, vm.NodeID, vm.State, vm.LastError)
		}
	}
	for _, nid := range []string{"node-a", "node-b"} {
		n, _ := c.NodeSnapshot(nid)
		fmt.Printf("node %s state=%s alloc=%+v capacity=%+v\n", nid, n.State, n.Allocated, n.Capacity)
	}

	fmt.Println("\n=== audit trail ===")
	for _, e := range c.Audit().Events() {
		fmt.Printf("#%-3d op=%-14s vm=%-8s node=%-7s decision=%-30s reason=%s %s\n",
			e.Seq, e.Op, e.VMID, e.NodeID, e.Decision, e.Reason, e.Detail)
	}
}
