package model

import (
	"errors"
	"fmt"
)

// Reason 是可机器区分的失败原因。
type Reason string

const (
	ReasonAlreadyExists        Reason = "AlreadyExists"
	ReasonNotFound             Reason = "NotFound"
	ReasonInsufficientCap      Reason = "InsufficientCapacity"
	ReasonQuotaExceeded        Reason = "QuotaExceeded"
	ReasonNoSuitableNode       Reason = "NoSuitableNode"
	ReasonNodeUnavailable      Reason = "NodeUnavailable"
	ReasonCapabilityMissing    Reason = "CapabilityUnsupported"
	ReasonVersionIncompatible  Reason = "VersionIncompatible"
	ReasonInvalidState         Reason = "InvalidState"
	ReasonBackendFailure       Reason = "BackendFailure"
	ReasonBackendIndeterminate Reason = "BackendIndeterminate"
)

// Error 是带原因与上下文的控制面错误。
type Error struct {
	Reason  Reason
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Reason, e.Message) }

// NewError 构造错误。
func NewError(r Reason, format string, args ...any) *Error {
	return &Error{Reason: r, Message: fmt.Sprintf(format, args...)}
}

// ReasonOf 从 error 提取 Reason；非控制面错误返回空串。
func ReasonOf(err error) Reason {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Reason
	}
	return ""
}
