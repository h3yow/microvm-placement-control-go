package backend

import (
	"context"
	"sync"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// Fake 是内存后端实现，支持失败注入与不确定结果注入，供本地验证使用。
type Fake struct {
	mu          sync.Mutex
	version     int
	caps        map[model.Capability]bool
	running     map[string]bool
	startCalls  map[string]int
	stopCalls   map[string]int
	deleteCalls map[string]int
	// FailStart/FailStop/FailDelete 注入确定性失败。
	FailStart  map[string]error
	FailStop   map[string]error
	FailDelete map[string]error
	// IndeterminateStart 注入“已生效但返回不确定”的场景。
	IndeterminateStart map[string]bool
}

// NewFake 构造指定版本与能力的内存后端。
func NewFake(version int, caps ...model.Capability) *Fake {
	f := &Fake{
		version:            version,
		caps:               map[model.Capability]bool{},
		running:            map[string]bool{},
		startCalls:         map[string]int{},
		stopCalls:          map[string]int{},
		deleteCalls:        map[string]int{},
		FailStart:          map[string]error{},
		FailStop:           map[string]error{},
		FailDelete:         map[string]error{},
		IndeterminateStart: map[string]bool{},
	}
	for _, c := range caps {
		f.caps[c] = true
	}
	return f
}

// ProtocolVersion 实现 Driver。
func (f *Fake) ProtocolVersion() int { return f.version }

// Capabilities 实现 Driver。
func (f *Fake) Capabilities() map[model.Capability]bool {
	out := make(map[model.Capability]bool, len(f.caps))
	for k, v := range f.caps {
		out[k] = v
	}
	return out
}

// Start 实现 Driver（幂等，可注入失败/不确定）。
func (f *Fake) Start(ctx context.Context, vmID string, req model.Resources) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startCalls[vmID]++
	if err, ok := f.FailStart[vmID]; ok {
		return err
	}
	if f.IndeterminateStart[vmID] {
		// 模拟“实际已生效但结果未知”：置为运行并返回不确定错误。
		f.running[vmID] = true
		return ErrIndeterminate
	}
	f.running[vmID] = true
	return nil
}

// Stop 实现 Driver（幂等）。
func (f *Fake) Stop(ctx context.Context, vmID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopCalls[vmID]++
	if err, ok := f.FailStop[vmID]; ok {
		return err
	}
	f.running[vmID] = false
	return nil
}

// Delete 实现 Driver（幂等）。
func (f *Fake) Delete(ctx context.Context, vmID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls[vmID]++
	if err, ok := f.FailDelete[vmID]; ok {
		return err
	}
	delete(f.running, vmID)
	return nil
}

// IsRunning 实现 Driver。
func (f *Fake) IsRunning(ctx context.Context, vmID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running[vmID], nil
}

// StartCalls 返回某实例的 Start 调用次数（审计用）。
func (f *Fake) StartCalls(vmID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startCalls[vmID]
}

// StopCalls 返回 Stop 调用次数。
func (f *Fake) StopCalls(vmID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopCalls[vmID]
}

// DeleteCalls 返回 Delete 调用次数。
func (f *Fake) DeleteCalls(vmID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteCalls[vmID]
}
