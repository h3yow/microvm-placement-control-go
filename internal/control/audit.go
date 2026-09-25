package control

import (
	"sync"
	"time"
)

// Event 是一条可审计的操作记录。
type Event struct {
	Seq      uint64
	Time     time.Time
	Op       string // register-node/create/start/stop/delete/mark-lost 等
	VMID     string
	NodeID   string
	Decision string // 判定依据，如放置打分、回滚原因
	Reason   string // 失败原因（可区分）
	Detail   string
}

// AuditLog 是线程安全的追加式审计日志。
type AuditLog struct {
	mu     sync.Mutex
	events []Event
}

// NewAuditLog 构造空日志。
func NewAuditLog() *AuditLog { return &AuditLog{} }

// Append 追加一条记录。
func (l *AuditLog) Append(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.Seq = uint64(len(l.events)) + 1
	e.Time = time.Now()
	l.events = append(l.events, e)
}

// Events 返回全部记录的快照。
func (l *AuditLog) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	return out
}
