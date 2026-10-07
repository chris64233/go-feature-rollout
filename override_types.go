package featurerollout

import "time"

// 覆盖类型：紧急放行或临时屏蔽。
const (
	// OverrideAllow 放行覆盖：命中时直接放行（优先于依赖与灰度规则）。
	OverrideAllow = "allow"
	// OverrideBlock 屏蔽覆盖：命中时直接拒绝（优先级最高）。
	OverrideBlock = "block"
)

// 覆盖生命周期状态。
const (
	// OverrideActive 有效（在生效窗口内、未撤销、未被版本切换淘汰）。
	OverrideActive = "active"
	// OverrideExpired 已到期。
	OverrideExpired = "expired"
	// OverrideRevoked 已被人工撤销。
	OverrideRevoked = "revoked"
	// OverrideSuperseded 因发布/回滚导致目标规则版本不再是当前版本而失效。
	OverrideSuperseded = "superseded"
)

// 判定结果来源：说明最终命中的规则来源。
const (
	// SourceBlockOverride 命中屏蔽覆盖（最高优先级）。
	SourceBlockOverride = "block_override"
	// SourceAllowOverride 命中放行覆盖。
	SourceAllowOverride = "allow_override"
	// SourceDependency 前置依赖未放行。
	SourceDependency = "dependency"
	// SourceExcludeList 命中规则排除名单。
	SourceExcludeList = "exclude_list"
	// SourceIncludeList 命中规则包含名单。
	SourceIncludeList = "include_list"
	// SourceBucket 确定性百分比分桶。
	SourceBucket = "bucket"
	// SourceFeatureMissing 功能尚未发布任何版本。
	SourceFeatureMissing = "feature_missing"
)

// Override 是一条有期限的人工受众覆盖。
//
// 覆盖只对创建时指定的规则版本（Version）与当时的发布代际（Epoch）生效：
// 该功能一旦发布新版本或回滚导致当前版本变化，旧覆盖立即失效
// （状态变为 superseded），绝不沿用到新版本。
type Override struct {
	ID        string
	Feature   string
	UserID    string
	Version   int       // 创建时指定的规则版本，必须是当时当前版本
	Epoch     uint64    // 创建时该功能的发布代际
	Kind      string    // OverrideAllow 或 OverrideBlock
	StartAt   time.Time // 生效时间（含）
	EndAt     time.Time // 到期时间（不含）：到点立即失效
	Reason    string
	Operator  string
	CreatedAt time.Time
	Status    string
}

// ActiveAt 报告覆盖在时刻 t 是否处于有效窗口（StartAt <= t < EndAt）。
func (o *Override) ActiveAt(t time.Time) bool {
	return (t.Equal(o.StartAt) || t.After(o.StartAt)) && t.Before(o.EndAt)
}

// OverrideResult 是创建/撤销/延长覆盖（均按外部操作号幂等）的返回结果。
type OverrideResult struct {
	Override *Override
	// Replayed 为 true 表示这是同一操作号的重放，返回的是首次操作的结果，
	// 没有产生任何新状态。
	Replayed bool
}

// Decision 是一条不可变的在线判定历史记录。
// 撤销覆盖只影响后续判定，已经产生的 Decision 永远不会被改写或删除。
type Decision struct {
	Feature    string
	UserID     string
	Allowed    bool
	Source     string
	Version    int
	Seq        uint64
	OverrideID string
	Time       time.Time
}
