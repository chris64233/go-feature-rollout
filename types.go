package featurerollout

// FeatureKey 功能标识。
type FeatureKey string

// VersionID 规则版本号。规则版本一经发布即不可变。
type VersionID string

// ChangeID 外部变更号，发布请求按它幂等。
type ChangeID string

// UserID 在线判定的受众标识。
type UserID string

// Audience 明确包含或排除的受众。
//
// 命中 Exclude 的用户直接被拒；否则命中 Include 的用户直接放行，
// 不受百分比影响；两者都不命中时再走百分比分桶。
type Audience struct {
	Include []UserID
	Exclude []UserID
}

// VersionSpec 草拟阶段对一条功能规则版本的完整描述。
//
// Percentage 为稳定分桶百分比，取值 [0,100]：同一用户面对同一版本，
// 由 (版本身份, 用户) 的确定性哈希决定是否落在桶内，因此调大/调小
// 百分比只会确定性地扩大/收缩受众，且嵌套桶前缀保证已经在桶内的
// 用户在百分比调大时不会掉出。
type VersionSpec struct {
	Feature    FeatureKey
	Version    VersionID
	Percentage int
	Audience   Audience
	// DependsOn 前置依赖：本版本只在所列功能的当前版本对该用户也放行时才放行。
	DependsOn []FeatureKey
}

// RuleVersion 已发布、不可变的规则版本（快照内容）。
type RuleVersion struct {
	Feature    FeatureKey
	Version    VersionID
	Percentage int
	Audience   Audience
	DependsOn  []FeatureKey
}

// Draft 一份待发布草稿，可反复修改内容后重新校验、发布。
type Draft struct {
	ID       string
	Versions []VersionSpec
}

// ChangeRecord 一次成功的原子发布（或回滚）历史记录，永不被修改。
type ChangeRecord struct {
	ChangeID    ChangeID
	Kind        ChangeKind
	Versions    []RuleVersion // 本次成为各功能当前版本的规则快照（按功能键排序）
	RollbackOf  ChangeID      // 仅回滚：被重新激活的历史记录的变更号
	PrevCurrent map[FeatureKey]VersionID
	Seq         int64 // 单调发布序号
}

// ChangeKind 发布记录类型。
type ChangeKind int

const (
	// ChangePublish 常规发布。
	ChangePublish ChangeKind = iota + 1
	// ChangeRollback 回滚：重新激活一份历史一致快照。
	ChangeRollback
)
