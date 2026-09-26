package featurerollout

import "errors"

// 错误分类哨兵，调用方用 errors.Is 区分失败原因。
var (
	// ErrInvalidParam 参数错误：功能键为空、版本号非法、百分比越界等。
	ErrInvalidParam = errors.New("rollout: invalid parameter")
	// ErrDependencyNotFound 前置依赖指向的功能版本不存在（未发布且不在本次发布组内）。
	ErrDependencyNotFound = errors.New("rollout: dependency not found")
	// ErrDependencyCycle 发布组与已发布规则合并后依赖成环。
	ErrDependencyCycle = errors.New("rollout: dependency cycle detected")
	// ErrVersionConflict 规则版本已存在（版本发布后不可修改、不可重复占用）。
	ErrVersionConflict = errors.New("rollout: version conflict")
	// ErrChangeConflict 外部变更号已被不同内容的发布请求占用（幂等冲突）。
	ErrChangeConflict = errors.New("rollout: change id conflict")
	// ErrNotFound 草稿、功能或发布记录不存在。
	ErrNotFound = errors.New("rollout: not found")
)
