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
}

// RuleVersion 是已发布的规则版本。发布后不可修改，任何字段都不应被改写。
type RuleVersion struct {
	Feature    string
	Version    int
	Percentage int
	Include    map[string]struct{}
	Exclude    map[string]struct{}
	Deps       []Dependency
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

// OverrideEffect 是人工受众覆盖的效果：紧急放行或临时屏蔽。
type OverrideEffect string

const (
	// OverrideAllow 放行覆盖：命中即放行，优先于依赖与灰度规则。
	OverrideAllow OverrideEffect = "allow"
	// OverrideDeny 屏蔽覆盖：命中即拒绝，优先级最高。
	OverrideDeny OverrideEffect = "deny"
)

// 覆盖（及普通规则）在判定结果中的命中来源。
const (
	// ReasonNoRule 功能在当前快照中不存在任何已发布规则。
	ReasonNoRule = "no_rule"
	// ReasonOverrideDeny 命中屏蔽覆盖（最高优先级）。
	ReasonOverrideDeny = "override_deny"
	// ReasonOverrideAllow 命中放行覆盖（优先于依赖与灰度规则）。
	ReasonOverrideAllow = "override_allow"
	// ReasonDependency 前置依赖未满足（依赖功能缺失、版本过低或对该用户不放行）。
	ReasonDependency = "dependency"
	// ReasonExclude 命中规则排除名单。
	ReasonExclude = "exclude"
	// ReasonInclude 命中规则包含名单。
	ReasonInclude = "include"
	// ReasonPercentage 命中确定性百分比分桶（放行或拒绝）。
	ReasonPercentage = "percentage"
)

// OverrideStatus 是覆盖的生命周期状态。
type OverrideStatus string

const (
	// OverrideActive 有效：可能尚未到生效时间，也可能正在生效。
	OverrideActive OverrideStatus = "active"
	// OverrideRevoked 被人工撤销；只影响撤销之后的判定。
	OverrideRevoked OverrideStatus = "revoked"
	// OverrideExpired 已到期（扫描任务或判定时按时钟确认）。
	OverrideExpired OverrideStatus = "expired"
	// OverrideSuperseded 所绑定的发布版本已不再是当前版本
	// （新版本发布或回滚产生了新的版本代际），覆盖自动失效且不会随回滚复活。
	OverrideSuperseded OverrideStatus = "superseded"
)

// OverrideInput 是创建人工受众覆盖的请求。
//
// Version 为覆盖绑定的规则版本：必须是创建时该功能的当前发布版本；
// 传 0 表示绑定创建时刻的当前版本。覆盖永远不会作用于其他版本。
type OverrideInput struct {
	Feature   string
	UserID    string
	Version   int // 0 表示创建时的当前版本
	Effect    OverrideEffect
	Effective time.Time // 生效时间（UTC）
	Expires   time.Time // 到期时间（不含，UTC），必须晚于生效时间与当前时间
	Reason    string
	Operator  string
}

// Override 是一条有期限的人工受众覆盖，创建后除到期时间外不可修改。
type Override struct {
	ID         string
	Feature    string
	UserID     string
	Version    int    // 绑定的规则版本
	Generation uint64 // 绑定的发布版本代际，发布/回滚切换版本后递增
	Effect     OverrideEffect
	Effective  time.Time
	Expires    time.Time
	Reason     string
	Operator   string
	Status     OverrideStatus
	CreatedAt  time.Time
}

// OverrideResult 是创建/撤销/延长覆盖成功后的结果。
// 同一外部操作号重放返回首次结果（含当时的到期时间与状态）。
type OverrideResult struct {
	OverrideID string
	OpID       string
	OpKind     string // "create" / "revoke" / "extend"
	Feature    string
	UserID     string
	Version    int
	Effect     OverrideEffect
	Status     OverrideStatus
	Effective  time.Time
	Expires    time.Time
}
