// Package backend 定义微虚拟机后端驱动接口及版本兼容规则。
package backend

import (
	"context"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// MinProtocolVersion 与 MaxProtocolVersion 界定控制面支持的后端协议版本区间。
const (
	MinProtocolVersion = 1
	MaxProtocolVersion = 3
)

// ErrIndeterminate 表示后端结果不确定（如超时），调用方必须按“可能已生效”处理。
var ErrIndeterminate = model.NewError(model.ReasonBackendIndeterminate, "backend result indeterminate")

// Driver 是后端虚拟化平台抽象。实现必须保证幂等：
// 对同一 vmID 的重复 Start/Stop/Delete 不得产生副作用放大。
type Driver interface {
	// ProtocolVersion 返回后端协议版本。
	ProtocolVersion() int
	// Capabilities 返回后端支持的能力集合。
	Capabilities() map[model.Capability]bool
	// Start 启动实例。返回 ErrIndeterminate 表示结果未知。
	Start(ctx context.Context, vmID string, req model.Resources) error
	// Stop 停止实例（幂等）。
	Stop(ctx context.Context, vmID string) error
	// Delete 删除实例并释放后端资源（幂等）。
	Delete(ctx context.Context, vmID string) error
	// IsRunning 用于不确定结果后的对账。
	IsRunning(ctx context.Context, vmID string) (bool, error)
}

// Compatible 判定后端协议版本是否被控制面支持。
func Compatible(version int) bool {
	return version >= MinProtocolVersion && version <= MaxProtocolVersion
}
