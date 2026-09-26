package featurerollout

import (
	"errors"
	"fmt"
)

// ErrorKind 区分不同的错误类别，便于调用方分别处理。
type ErrorKind string

const (
	// KindParam 参数错误：请求本身不合法（空功能名、非法百分比、版本号小于 1 等）。
	KindParam ErrorKind = "param"
	// KindDependency 依赖错误：依赖的功能或版本不存在、或当前版本不满足依赖要求。
	KindDependency ErrorKind = "dependency"
	// KindCycle 循环依赖错误：依赖图成环（含自依赖）。
	KindCycle ErrorKind = "cycle"
	// KindVersion 版本错误：版本已发布、版本未单调递增、回滚目标快照不存在等。
	KindVersion ErrorKind = "version"
	// KindConflict 幂等冲突：同一外部变更号提交了不同内容。
	KindConflict ErrorKind = "conflict"
	// KindNotFound 草拟、功能等对象不存在。
	KindNotFound ErrorKind = "not_found"
)

// Error 是服务返回的统一错误类型，携带类别信息。
type Error struct {
	Kind ErrorKind
	Msg  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Kind, e.Msg)
}

// KindOf 提取错误的类别；err 不是 *Error 时 ok 为 false。
func KindOf(err error) (kind ErrorKind, ok bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind, true
	}
	return "", false
}

func paramErr(format string, args ...any) error {
	return &Error{Kind: KindParam, Msg: fmt.Sprintf(format, args...)}
}

func dependencyErr(format string, args ...any) error {
	return &Error{Kind: KindDependency, Msg: fmt.Sprintf(format, args...)}
}

func cycleErr(format string, args ...any) error {
	return &Error{Kind: KindCycle, Msg: fmt.Sprintf(format, args...)}
}

func versionErr(format string, args ...any) error {
	return &Error{Kind: KindVersion, Msg: fmt.Sprintf(format, args...)}
}

func conflictErr(format string, args ...any) error {
	return &Error{Kind: KindConflict, Msg: fmt.Sprintf(format, args...)}
}

func notFoundErr(format string, args ...any) error {
	return &Error{Kind: KindNotFound, Msg: fmt.Sprintf(format, args...)}
}
