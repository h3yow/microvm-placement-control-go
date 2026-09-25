// Package backend 定义虚拟化后端抽象与能力模型。
package backend

import (
	"context"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// CapabilitySet 描述后端能力与版本。
type CapabilitySet struct {
	Backend  string            `json:"backend"`  // 后端类型，如 "firecracker"
	Version  string            `json:"version"`  // 后端版本，语义化版本
	Features map[string]string `json:"features"` // 特性开关，如 "snapshot":"1"
}

// Supports 判断能力集是否满足 required（key 与值完全一致）。
func (c CapabilitySet) Supports(required map[string]string) (missing map[string]string) {
	for k, v := range required {
		var got string
		switch k {
		case "backend":
			got = c.Backend
		case "backend_version":
			got = c.Version
		default:
			got = c.Features[k]
		}
		if got != v {
			if missing == nil {
				missing = map[string]string{}
			}
			missing[k] = got
		}
	}
	return missing
}

// ErrUncertain 表示后端调用结果不确定（如超时），实例可能已存在。
type ErrUncertain struct{ Op string }

func (e *ErrUncertain) Error() string { return "backend result uncertain during " + e.Op }

// Backend 是虚拟化后端的抽象。实现必须幂等：对同一 backendID 的重复调用安全。
type Backend interface {
	Capabilities(ctx context.Context) (CapabilitySet, error)
	CreateVM(ctx context.Context, backendID string, spec model.VMSpec) error
	StartVM(ctx context.Context, backendID string) error
	StopVM(ctx context.Context, backendID string) error
	DeleteVM(ctx context.Context, backendID string) error
	// QueryVM 用于不确定结果后的对账：返回后端是否确实存在该实例。
	QueryVM(ctx context.Context, backendID string) (exists bool, err error)
}
