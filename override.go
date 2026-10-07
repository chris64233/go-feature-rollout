package featurerollout

import (
	"fmt"
	"sort"
	"time"
)

// 人工受众覆盖（manual audience override）：用于紧急放行或临时屏蔽少量用户。
//
// 覆盖严格绑定"创建时指定的规则版本 + 发布版本代际"：
//   - 只对该版本生效，新版本发布、回滚后旧覆盖不会自动沿用；
//   - 版本即使通过回滚再次成为当前版本，代际已经变化，旧覆盖依旧失效；
//   - 撤销只影响后续判定，历史判定记录不可改写（历史回放 EvalAt 从不读取覆盖）。

const (
	opCreate = "create"
	opRevoke = "revoke"
	opExtend = "extend"
)

// opRecord 记录一个外部操作号的首次结果，用于幂等重放与冲突检测。
type opRecord struct {
	kind   string // opCreate / opRevoke / opExtend
	result OverrideResult
	input  OverrideInput // opCreate 时的原始请求，用于同号异内容冲突检测
}

// overrideKey 定位"同一功能、同一用户、同一发布版本"的唯一有效覆盖槽位。
type overrideKey struct {
	feature string
	userID  string
	version int
}

// ---------------- 创建 ----------------

// CreateOverride 为指定功能/用户创建一条有期限的放行或屏蔽覆盖。
//
// 覆盖只作用于 in.Version 指定的规则版本（必须是创建时该功能的当前发布版本，
// 传 0 表示当前版本）。同一功能、同一用户、同一发布版本最多有一条有效覆盖。
// 按 opID 幂等：相同操作重放返回首次结果；用户、功能、版本或时间变化返回 KindConflict。
func (s *Service) CreateOverride(opID string, in OverrideInput) (*OverrideResult, error) {
	if opID == "" {
		return nil, paramErr("opID is empty")
	}
	if err := validateOverrideInput(in); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, replay, err := s.replayCreateLocked(opID, in); err != nil {
		return nil, err
	} else if replay {
		return rec, nil
	}

	now := s.now()
	eff := in.Effective.UTC()
	exp := in.Expires.UTC()

	// 版本必须已发布，且覆盖只能针对创建时的当前发布版本。
	ver := in.Version
	if ver == 0 {
		cur, ok := s.current[in.Feature]
		if !ok {
			return nil, notFoundErr("feature %q has no published version", in.Feature)
		}
		ver = cur
	}
	if s.versions[in.Feature] == nil || s.versions[in.Feature][ver] == nil {
		if s.versions[in.Feature] == nil {
			return nil, notFoundErr("feature %q has no published version", in.Feature)
		}
		return nil, versionErr("feature %q version %d is not published", in.Feature, ver)
	}
	if cur := s.current[in.Feature]; cur != ver {
		return nil, versionErr("feature %q version %d is not the current published version (current %d)",
			in.Feature, ver, cur)
	}

	key := overrideKey{in.Feature, in.UserID, ver}
	if id, conflict := s.activeSlots[key]; conflict {
		return nil, conflictErr("feature %q user %q version %d already has an active override %q",
			in.Feature, in.UserID, ver, id)
	}

	gen := s.generation[in.Feature]
	s.overrideSeq++
	id := fmt.Sprintf("override-%d", s.overrideSeq)
	o := &Override{
		ID:         id,
		Feature:    in.Feature,
		UserID:     in.UserID,
		Version:    ver,
		Generation: gen,
		Effect:     in.Effect,
		Effective:  eff,
		Expires:    exp,
		Reason:     in.Reason,
		Operator:   in.Operator,
		Status:     OverrideActive,
		CreatedAt:  now,
	}
	s.overrides[id] = o
	s.activeSlots[key] = id

	res := resultOf(o, opCreate)
	res.OpID = opID
	s.opRecords[opID] = opRecord{kind: opCreate, result: res, input: in}
	return cloneResult(res), nil
}

// ---------------- 撤销 ----------------

// RevokeOverride 撤销一条覆盖。撤销只影响之后的判定：已经被撤销的覆盖
// 不再命中，也不会因为到期扫描或版本切换改变状态；撤销记录按 opID 幂等。
func (s *Service) RevokeOverride(opID, overrideID string) (*OverrideResult, error) {
	if opID == "" {
		return nil, paramErr("opID is empty")
	}
	if overrideID == "" {
		return nil, paramErr("overrideID is empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	o, ok := s.overrides[overrideID]
	if !ok {
		// 失败不占用操作号：先确认对象存在，再做幂等判断。
		if _, seen := s.opRecords[opID]; seen {
			return s.replayRevokeLocked(opID, overrideID)
		}
		return nil, notFoundErr("override %q not found", overrideID)
	}

	if _, seen := s.opRecords[opID]; seen {
		return s.replayRevokeLocked(opID, overrideID)
	}

	o.Status = OverrideRevoked
	if id := s.activeSlots[overrideKey{o.Feature, o.UserID, o.Version}]; id == overrideID {
		delete(s.activeSlots, overrideKey{o.Feature, o.UserID, o.Version})
	}

	res := resultOf(o, opRevoke)
	res.OpID = opID
	s.opRecords[opID] = opRecord{kind: opRevoke, result: res}
	return cloneResult(res), nil
}

// ---------------- 延长 ----------------

// ExtendOverride 延长覆盖的有效期。newExpires 必须晚于当前到期时间，
// 且覆盖必须仍然有效（未撤销、未到期、所绑定版本仍是当前版本代际）。
// 按 opID 幂等：相同操作重放返回首次结果；新到期时间变化返回 KindConflict。
func (s *Service) ExtendOverride(opID, overrideID string, newExpires time.Time) (*OverrideResult, error) {
	if opID == "" {
		return nil, paramErr("opID is empty")
	}
	if overrideID == "" {
		return nil, paramErr("overrideID is empty")
	}
	newExpires = newExpires.UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, _, err := s.replayExtendLocked(opID, overrideID, newExpires); err != nil {
		return nil, err
	} else if rec != nil {
		return rec, nil
	}

	o, ok := s.overrides[overrideID]
	if !ok {
		return nil, notFoundErr("override %q not found", overrideID)
	}
	if o.Status != OverrideActive {
		return nil, versionErr("override %q is not active (status %s)", overrideID, o.Status)
	}
	if s.generation[o.Feature] != o.Generation {
		return nil, versionErr("override %q targets a superseded release and cannot be extended", overrideID)
	}
	now := s.now()
	if !o.Expires.After(now) {
		o.Status = OverrideExpired
		s.clearSlotLocked(o)
		return nil, versionErr("override %q already expired at %s", overrideID, o.Expires.Format(time.RFC3339))
	}
	if !newExpires.After(o.Expires) {
		return nil, paramErr("new expiry %s must be later than current expiry %s",
			newExpires.Format(time.RFC3339), o.Expires.Format(time.RFC3339))
	}

	o.Expires = newExpires
	res := resultOf(o, opExtend)
	res.OpID = opID
	s.opRecords[opID] = opRecord{kind: opExtend, result: res}
	return cloneResult(res), nil
}

// ---------------- 到期扫描 ----------------

// SweepExpired 按时钟扫描并停用所有已到期覆盖，返回本次被停用的覆盖 ID。
// 判定路径本身也会检查到期时间，因此扫描是否及时不影响正确性；
// 扫描的作用是及时释放"唯一有效覆盖"槽位并让状态对查询可见。
func (s *Service) SweepExpired() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	var expired []string
	for _, o := range s.overrides {
		if o.Status == OverrideActive && !o.Expires.After(now) {
			o.Status = OverrideExpired
			s.clearSlotLocked(o)
			expired = append(expired, o.ID)
		}
	}
	sort.Strings(expired)
	return expired
}

// ---------------- 查询 ----------------

// GetOverride 按 ID 查询覆盖当前状态（返回副本）。
func (s *Service) GetOverride(id string) (*Override, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.overrides[id]
	if !ok {
		return nil, notFoundErr("override %q not found", id)
	}
	return cloneOverride(o), nil
}

// ListOverrides 返回某功能下的覆盖历史（含已撤销/已到期/已被版本切换作废的），按 ID 升序。
func (s *Service) ListOverrides(feature string) []*Override {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ids []string
	byID := map[string]*Override{}
	for _, o := range s.overrides {
		if o.Feature == feature {
			ids = append(ids, o.ID)
			byID[o.ID] = o
		}
	}
	sort.Strings(ids)
	out := make([]*Override, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneOverride(byID[id]))
	}
	return out
}

// ---------------- 内部 ----------------

// validateOverrideInput 校验创建请求字段与时间范围的合理性。
func validateOverrideInput(in OverrideInput) error {
	if in.Feature == "" {
		return paramErr("feature name is empty")
	}
	if in.UserID == "" {
		return paramErr("userID is empty")
	}
	if in.Version < 0 {
		return paramErr("feature %q override version must be >= 0 (0 means current)", in.Feature)
	}
	if in.Effect != OverrideAllow && in.Effect != OverrideDeny {
		return paramErr("feature %q override effect must be %q or %q, got %q",
			in.Feature, OverrideAllow, OverrideDeny, in.Effect)
	}
	if in.Operator == "" {
		return paramErr("operator is empty")
	}
	if in.Reason == "" {
		return paramErr("reason is empty")
	}
	eff := in.Effective.UTC()
	exp := in.Expires.UTC()
	if !exp.After(eff) {
		return paramErr("expires %s must be later than effective %s",
			exp.Format(time.RFC3339), eff.Format(time.RFC3339))
	}
	return nil
}

// replayCreateLocked 处理创建操作的幂等与冲突：
// 同号必须是同一操作类型，且用户、功能、版本、效果或时间与首次完全一致才回放。
func (s *Service) replayCreateLocked(opID string, in OverrideInput) (*OverrideResult, bool, error) {
	prev, seen := s.opRecords[opID]
	if !seen {
		return nil, false, nil
	}
	if prev.kind != opCreate {
		return nil, false, conflictErr("opID %q already used for a %s operation, not %s",
			opID, prev.kind, opCreate)
	}
	if !sameOverrideInput(prev.input, in) {
		return nil, false, conflictErr("opID %q already used with different override content", opID)
	}
	return cloneResult(prev.result), true, nil
}

// replayRevokeLocked 处理撤销重放：同号必须作用于同一条覆盖。
func (s *Service) replayRevokeLocked(opID, overrideID string) (*OverrideResult, error) {
	prev := s.opRecords[opID]
	if prev.kind != opRevoke {
		return nil, conflictErr("opID %q already used for a %s operation, not %s",
			opID, prev.kind, opRevoke)
	}
	if prev.result.OverrideID != overrideID {
		return nil, conflictErr("opID %q revoked override %q, not %q",
			opID, prev.result.OverrideID, overrideID)
	}
	return cloneResult(prev.result), nil
}

func sameOverrideInput(a, b OverrideInput) bool {
	return a.Feature == b.Feature &&
		a.UserID == b.UserID &&
		a.Version == b.Version &&
		a.Effect == b.Effect &&
		a.Effective.UTC().Equal(b.Effective.UTC()) &&
		a.Expires.UTC().Equal(b.Expires.UTC()) &&
		a.Reason == b.Reason &&
		a.Operator == b.Operator
}

// replayExtendLocked 处理延长操作的幂等：同号必须作用于同一覆盖且新到期时间一致。
// 返回非 nil result 表示调用方应直接返回（重放成功或冲突已处理）。
func (s *Service) replayExtendLocked(opID, overrideID string, newExpires time.Time) (*OverrideResult, bool, error) {
	prev, seen := s.opRecords[opID]
	if !seen {
		return nil, false, nil
	}
	if prev.kind != opExtend {
		return nil, false, conflictErr("opID %q already used for a %s operation, not %s",
			opID, prev.kind, opExtend)
	}
	if prev.result.OverrideID != overrideID {
		return nil, false, conflictErr("opID %q extends override %q, not %q",
			opID, prev.result.OverrideID, overrideID)
	}
	if !prev.result.Expires.Equal(newExpires) {
		return nil, false, conflictErr("opID %q set expiry %s, replay got %s",
			opID, prev.result.Expires.Format(time.RFC3339), newExpires.Format(time.RFC3339))
	}
	return cloneResult(prev.result), true, nil
}

func (s *Service) clearSlotLocked(o *Override) {
	key := overrideKey{o.Feature, o.UserID, o.Version}
	if s.activeSlots[key] == o.ID {
		delete(s.activeSlots, key)
	}
}

// effectiveOverrideLocked 返回当前时刻对某功能/用户真正生效的指定效果覆盖。
// 调用方必须持有读锁或写锁。判定顺序：屏蔽覆盖 > 放行覆盖 > 依赖与灰度规则。
func (s *Service) effectiveOverrideLocked(rule *RuleVersion, userID string, now time.Time, want OverrideEffect) *Override {
	id, ok := s.activeSlots[overrideKey{rule.Feature, userID, rule.Version}]
	if !ok {
		return nil
	}
	o := s.overrides[id]
	if o == nil {
		return nil
	}
	if o.Effect != want {
		return nil
	}
	// 代际不符说明该版本已被新版本/回滚取代（回滚复活也会产生新代际），不沿用。
	if s.generation[rule.Feature] != o.Generation {
		return nil
	}
	// 临界到期：到期时刻起（含边界，Expires 为排他边界）即不再生效。
	if now.Before(o.Effective) || !now.Before(o.Expires) {
		return nil
	}
	return o
}

func resultOf(o *Override, kind string) OverrideResult {
	return OverrideResult{
		OverrideID: o.ID,
		OpKind:     kind,
		Feature:    o.Feature,
		UserID:     o.UserID,
		Version:    o.Version,
		Effect:     o.Effect,
		Status:     o.Status,
		Effective:  o.Effective,
		Expires:    o.Expires,
	}
}

func cloneResult(r OverrideResult) *OverrideResult {
	cp := r
	return &cp
}

func cloneOverride(o *Override) *Override {
	cp := *o
	return &cp
}
