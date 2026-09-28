package featurerollout

import "time"

// Dependency 表达对另一个功能某个已发布版本的前置依赖：
// 要求该功能已发布过 Version，且判定当时其当前版本不低于 Version 并对用户放行。
type Dependency struct {
	Feature string
	Version int
}

// RuleInput 是草拟/发布接口接收的规则描述。
type RuleInput struct {
	Feature    string       // 功能标识
	Version    int          // 规则版本号，同一功能内必须单调递增且不可复用
	Percentage int          // 灰度百分比 0-100，基于确定性分桶
	Include    []string     // 明确放行的用户（优先于百分比）
	Exclude    []string     // 明确排除的用户（优先于 Include）
	Deps       []Dependency // 前置依赖
	Guard      *GuardConfig // 自动暂停保护；nil 表示不启用
}

// RuleVersion 是已发布的规则版本。发布后不可修改，任何字段都不应被改写。
type RuleVersion struct {
	Feature    string
	Version    int
	Percentage int
	Include    map[string]struct{}
	Exclude    map[string]struct{}
	Deps       []Dependency
	Guard      *GuardConfig
}

// Snapshot 是一份整组一致的已发布快照：每个功能至多一个当前版本。
// 在线判定沿依赖链读取时只使用同一份 Snapshot，绝不混用新旧版本。
type Snapshot struct {
	Seq   uint64
	Rules map[string]*RuleVersion // feature -> 当前版本
}

// PublishRecord 是发布历史中的一条记录，序列号单调递增。
type PublishRecord struct {
	Seq        uint64
	ChangeID   string
	Kind       string // "publish" 或 "rollback"
	Features   []string
	RollbackTo uint64 // 仅回滚记录：重新激活的历史快照序列号
	Time       time.Time
}

// PublishResult 是发布/回滚成功后的结果，同一变更号重复发布返回相同结果。
type PublishResult struct {
	Seq      uint64
	ChangeID string
	Features []string
}

// Draft 是一次尚未发布的草拟。
type Draft struct {
	ID       string
	ChangeID string
	Rules    []RuleInput
	Hash     string // 规则内容的规范化哈希，用于幂等校验
}
