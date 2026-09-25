// Package control 实现微虚拟机生命周期与资源放置控制面。
package control

import "fmt"

// ReasonCode 是可区分的拒绝/失败原因码。
type ReasonCode string

const (
	ReasonCapacity          ReasonCode = "CAPACITY_EXHAUSTED"  // 容量不足
	ReasonQuota             ReasonCode = "QUOTA_EXCEEDED"      // 配额超限
	ReasonNodeUnavailable   ReasonCode = "NODE_UNAVAILABLE"    // 无可用节点（失联/未登记）
	ReasonCapability        ReasonCode = "CAPABILITY_MISMATCH" // 能力不满足且未声明降级
	ReasonConstraint        ReasonCode = "CONSTRAINT_VIOLATED" // 亲和/反亲和不可满足
	ReasonConflict          ReasonCode = "REQUEST_CONFLICT"    // 幂等键冲突（同键不同规格）
	ReasonNotFound          ReasonCode = "NOT_FOUND"
	ReasonInvalidState      ReasonCode = "INVALID_STATE"
	ReasonBackend           ReasonCode = "BACKEND_ERROR"     // 后端明确失败
	ReasonBackendUncertain  ReasonCode = "BACKEND_UNCERTAIN" // 后端结果不确定，已收敛到明确终态
	ReasonDuplicateRejected ReasonCode = "DUPLICATE_REJECTED"
)

// Error 是控制面返回的可区分错误。
type Error struct {
	Code   ReasonCode
	Detail string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Detail) }

func newErr(code ReasonCode, format string, args ...any) *Error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf 从 error 提取 ReasonCode；非控制面错误返回 ReasonBackend。
func CodeOf(err error) ReasonCode {
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return ReasonBackend
}
