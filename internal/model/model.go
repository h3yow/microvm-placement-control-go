// Package model 定义控制面的核心资源与状态模型。
package model

// Resources 表示可计量的资源账目（CPU 毫核、内存 MiB）。
type Resources struct {
	MilliCPU int64
	MemoryMB int64
}

// Add 返回两者之和。
func (r Resources) Add(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU + o.MilliCPU, MemoryMB: r.MemoryMB + o.MemoryMB}
}

// Sub 返回两者之差。
func (r Resources) Sub(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU - o.MilliCPU, MemoryMB: r.MemoryMB - o.MemoryMB}
}

// Fits 判断 r 是否能容纳 o（各维度均不小于）。
func (r Resources) Fits(o Resources) bool {
	return r.MilliCPU >= o.MilliCPU && r.MemoryMB >= o.MemoryMB
}

// IsZero 判断是否为零资源。
func (r Resources) IsZero() bool { return r.MilliCPU == 0 && r.MemoryMB == 0 }

// Capability 表示后端能力标识。
type Capability string

const (
	CapSnapshot   Capability = "snapshot"
	CapLiveResize Capability = "live-resize"
	CapHugePages  Capability = "hugepages"
)

// VMState 是微虚拟机生命周期的显式状态机状态。
type VMState string

const (
	// StatePending 已登记、尚未占用资源。
	StatePending VMState = "Pending"
	// StateAllocated 已占用资源、尚未在后端启动。
	StateAllocated VMState = "Allocated"
	// StateRunning 后端已确认运行。
	StateRunning VMState = "Running"
	// StateStopped 已停止，资源仍保留。
	StateStopped VMState = "Stopped"
	// StateFailed 明确终态：创建/启动失败且资源已回收。
	StateFailed VMState = "Failed"
	// StateDeleted 明确终态：已删除，资源已回收。
	StateDeleted VMState = "Deleted"
)

// Terminal 判断是否为终态。
func (s VMState) Terminal() bool { return s == StateFailed || s == StateDeleted }

// NodeState 表示宿主节点可用性。
type NodeState string

const (
	NodeReady NodeState = "Ready"
	NodeLost  NodeState = "Lost"
	NodeDown  NodeState = "Down"
)

// Node 是宿主节点的账目与状态。
type Node struct {
	ID        string
	State     NodeState
	Capacity  Resources
	Allocated Resources
	// Caps 为节点后端声明支持的能力集合。
	Caps map[Capability]bool
	// BackendVersion 为节点后端协议版本。
	BackendVersion int
}

// Available 返回剩余可用资源。
func (n *Node) Available() Resources { return n.Capacity.Sub(n.Allocated) }

// MicroVM 是控制面登记的实例记录。
type MicroVM struct {
	ID       string
	NodeID   string
	State    VMState
	Request  Resources
	Required []Capability
	// IdempotencyKey 由调用方提供，用于去重。
	IdempotencyKey string
	// LastError 记录最近一次失败原因（可区分）。
	LastError string
	// Generation 单调递增，用于审计与并发判定。
	Generation uint64
}
