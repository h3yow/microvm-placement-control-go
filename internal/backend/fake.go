// 本文件实现一个纯内存、可脚本化故障的伪后端，用于本地运行与测试，
// 不依赖真实虚拟化平台、外部服务或凭据。
package backend

import (
	"context"
	"sync"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// Call 记录一次后端调用，用于审计与测试断言。
type Call struct {
	Op        string
	BackendID string
}

// Fake 是一个确定性的内存后端。可通过 Fail 注入指定操作的错误，
// 通过 Uncertain 注入不确定结果。
type Fake struct {
	mu         sync.Mutex
	caps       CapabilitySet
	vms        map[string]bool // backendID -> running
	calls      []Call
	failOps    map[string]error // op -> 注入错误
	uncertain  map[string]bool  // op -> 该操作返回不确定（实际效果由 applyUncertainEffect 决定）
	applyUncer bool             // 不确定时是否仍然应用效果（模拟"实际成功但响应丢失"）
}

// NewFake 创建伪后端。
func NewFake(caps CapabilitySet) *Fake {
	return &Fake{
		caps:      caps,
		vms:       map[string]bool{},
		failOps:   map[string]error{},
		uncertain: map[string]bool{},
	}
}

// FailOn 使指定操作（CreateVM/StartVM/StopVM/DeleteVM）总是返回 err。
func (f *Fake) FailOn(op string, err error) { f.mu.Lock(); f.failOps[op] = err; f.mu.Unlock() }

// ClearFail 移除指定操作的故障注入。
func (f *Fake) ClearFail(op string) { f.mu.Lock(); delete(f.failOps, op); f.mu.Unlock() }

// UncertainOn 使指定操作返回 ErrUncertain；applyEffect 为 true 时实际效果仍生效。
func (f *Fake) UncertainOn(op string, applyEffect bool) {
	f.mu.Lock()
	f.uncertain[op] = true
	f.applyUncer = applyEffect
	f.mu.Unlock()
}

// ClearUncertain 移除指定操作的不确定注入。
func (f *Fake) ClearUncertain(op string) { f.mu.Lock(); delete(f.uncertain, op); f.mu.Unlock() }

// Calls 返回已记录调用的副本。
func (f *Fake) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Call, len(f.calls))
	copy(out, f.calls)
	return out
}

// CountOp 统计某操作被调用的次数。
func (f *Fake) CountOp(op string) int {
	n := 0
	for _, c := range f.Calls() {
		if c.Op == op {
			n++
		}
	}
	return n
}

// Exists 报告后端中是否存在该实例（测试辅助）。
func (f *Fake) Exists(backendID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.vms[backendID]
	return ok
}

// Capabilities 实现 Backend。
func (f *Fake) Capabilities(context.Context) (CapabilitySet, error) { return f.caps, nil }

func (f *Fake) gate(op, id string, apply func()) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, Call{Op: op, BackendID: id})
	if err, ok := f.failOps[op]; ok {
		return err
	}
	if f.uncertain[op] {
		if f.applyUncer {
			apply()
		}
		return &ErrUncertain{Op: op}
	}
	apply()
	return nil
}

// CreateVM 实现 Backend，幂等：重复创建同一 id 不报错。
func (f *Fake) CreateVM(_ context.Context, id string, _ model.VMSpec) error {
	return f.gate("CreateVM", id, func() {
		if _, ok := f.vms[id]; !ok {
			f.vms[id] = false
		}
	})
}

// StartVM 实现 Backend，幂等。
func (f *Fake) StartVM(_ context.Context, id string) error {
	return f.gate("StartVM", id, func() {
		if _, ok := f.vms[id]; ok {
			f.vms[id] = true
		}
	})
}

// StopVM 实现 Backend，幂等。
func (f *Fake) StopVM(_ context.Context, id string) error {
	return f.gate("StopVM", id, func() {
		if _, ok := f.vms[id]; ok {
			f.vms[id] = false
		}
	})
}

// DeleteVM 实现 Backend，幂等。
func (f *Fake) DeleteVM(_ context.Context, id string) error {
	return f.gate("DeleteVM", id, func() { delete(f.vms, id) })
}

// QueryVM 实现 Backend。
func (f *Fake) QueryVM(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.vms[id]
	return ok, nil
}
