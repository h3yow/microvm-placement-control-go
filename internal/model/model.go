// Package model 定义控制面的核心领域模型：资源、节点、微虚拟机与状态机。
package model

// Resources 表示可计量的资源量。
type Resources struct {
	MilliCPU int64 `json:"milli_cpu"`
	MemoryMB int64 `json:"memory_mb"`
}

// Add 返回两者之和。
func (r Resources) Add(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU + o.MilliCPU, MemoryMB: r.MemoryMB + o.MemoryMB}
}

// Sub 返回两者之差（允许调用方保证非负）。
func (r Resources) Sub(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU - o.MilliCPU, MemoryMB: r.MemoryMB - o.MemoryMB}
}

// Fits 判断 r 是否不大于 limit。
func (r Resources) Fits(limit Resources) bool {
	return r.MilliCPU <= limit.MilliCPU && r.MemoryMB <= limit.MemoryMB
}

// IsZero 判断是否为零资源。
func (r Resources) IsZero() bool { return r.MilliCPU == 0 && r.MemoryMB == 0 }

// NodeState 表示宿主节点状态。
type NodeState string

const (
	NodeStateReady    NodeState = "Ready"
	NodeStateOffline  NodeState = "Offline"  // 失联或维护，不可放置
	NodeStateDraining NodeState = "Draining" // 维护排空：不接收新实例，正在疏散存量
)

// Node 表示一个宿主节点。
type Node struct {
	ID           string            `json:"id"`
	State        NodeState         `json:"state"`
	Capacity     Resources         `json:"capacity"`
	Allocated    Resources         `json:"allocated"`
	Capabilities map[string]string `json:"capabilities"` // 如 backend=firecracker, backend_version=1.7, feat_snapshot=1
}

// HasCapacity 判断节点剩余容量是否满足 req。
func (n *Node) HasCapacity(req Resources) bool {
	return n.Capacity.Sub(n.Allocated).Fits(n.Capacity) && req.Fits(n.Capacity.Sub(n.Allocated))
}

// VMState 表示微虚拟机生命周期状态。
type VMState string

const (
	VMStatePlacing   VMState = "Placing"  // 已记账，正在后端创建
	VMStateStopped   VMState = "Stopped"  // 已创建未运行
	VMStateStarting  VMState = "Starting" // 正在启动
	VMStateRunning   VMState = "Running"
	VMStateStopping  VMState = "Stopping"
	VMStateDeleting  VMState = "Deleting"  // 删除已认领、后端删除在途，阻止并发删除重复回收
	VMStateMigrating VMState = "Migrating" // 维护排空中：正在从源节点疏散到目标节点
	VMStateFailed    VMState = "Failed"    // 明确终态：后端结果不确定或失败且已回收资源
	VMStateDeleted   VMState = "Deleted"   // 明确终态：资源已释放
)

// Terminal 判断是否为终态。
func (s VMState) Terminal() bool { return s == VMStateFailed || s == VMStateDeleted }

// VMSpec 描述微虚拟机的创建请求。
type VMSpec struct {
	RequestID    string            `json:"request_id"` // 幂等键：同一 RequestID 重复请求返回同一实例
	RequiredCaps map[string]string `json:"required_caps"`
	// AcceptDegraded 显式声明可接受的安全降级：当 RequiredCaps[k] 与节点实际值
	// 不一致时，仅当节点实际值等于 AcceptDegraded[k] 才允许放置，并记录降级事实。
	AcceptDegraded  map[string]string `json:"accept_degraded"`
	AffinityLabels  map[string]string `json:"affinity_labels"`   // 节点 label 须匹配
	AntiAffinityKey string            `json:"anti_affinity_key"` // 同一节点上不允许存在相同 key 的其它 VM
	Resources       Resources         `json:"resources"`
}

// MicroVM 表示一个微虚拟机实例。
type MicroVM struct {
	ID        string  `json:"id"`
	RequestID string  `json:"request_id"`
	Spec      VMSpec  `json:"spec"`
	NodeID    string  `json:"node_id"`
	State     VMState `json:"state"`
	Reason    string  `json:"reason"` // 进入当前状态（尤其 Failed）的原因
	BackendID string  `json:"backend_id"`
}
