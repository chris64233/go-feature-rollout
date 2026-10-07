package featurerollout

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// overrideState 是覆盖集合的不可变快照（copy-on-write）。
//
// byID 保存全部历史覆盖（含已失效）；active 只索引当前仍为 active 的覆盖。
// 任何修改都产生一份新的 overrideState 并整体替换 Service.ovState 指针，
// 在线判定取出旧指针后可在锁外安全求值，与覆盖变更互不干扰。
type overrideState struct {
	byID   map[string]*Override
	active map[string]*Override // key: feature\0user\0version\0epoch
}

func newOverrideState() *overrideState {
	return &overrideState{
		byID:   map[string]*Override{},
		active: map[string]*Override{},
	}
}

func (st *overrideState) clone() *overrideState {
	cp := &overrideState{
		byID:   make(map[string]*Override, len(st.byID)),
		active: make(map[string]*Override, len(st.active)),
	}
	for k, v := range st.byID {
		cp.byID[k] = v
	}
	for k, v := range st.active {
		cp.active[k] = v
	}
	return cp
}

func overrideKey(feature, userID string, version int, epoch uint64) string {
	var b strings.Builder
	b.WriteString(feature)
	b.WriteByte(0)
	b.WriteString(userID)
	b.WriteByte(0)
	b.WriteString(strconv.Itoa(version))
	b.WriteByte(0)
	b.WriteString(strconv.FormatUint(epoch, 10))
	return b.String()
}

func (st *overrideState) find(feature, userID string, version int, epoch uint64) *Override {
	return st.active[overrideKey(feature, userID, version, epoch)]
}

// overrideOp 记录一个外部操作号的首次操作，用于幂等重放与冲突检测。
type overrideOp struct {
	action  string // "create" / "revoke" / "extend"
	ovID    string // 首次操作对应的覆盖 ID
	payload string // 规范化的请求内容，重放时逐字节比对
}

// OverrideInput 是创建人工覆盖的请求。
type OverrideInput struct {
	Feature  string
	UserID   string
	Version  int       // 目标规则版本，必须是该功能当前已发布版本
	Kind     string    // OverrideAllow 或 OverrideBlock
	StartAt  time.Time // 生效时间（含）
	EndAt    time.Time // 到期时间（不含）
	Reason   string
	Operator string
}

// RevokeOverrideInput 是撤销覆盖的请求。
type RevokeOverrideInput struct {
	Feature string
	UserID  string
	Version int
}

// ExtendOverrideInput 是延长覆盖到期时间的请求。
type ExtendOverrideInput struct {
	Feature string
	UserID  string
	Version int
	NewEnd  time.Time // 新的到期时间（不含），必须严格晚于当前到期时间
	Reason  string    // 延长原因
}

func (s *Service) nowUTC() time.Time {
	return s.now().UTC()
}

func validateOverrideInput(in OverrideInput) error {
	if in.Feature == "" {
		return paramErr("override feature is empty")
	}
	if in.UserID == "" {
		return paramErr("override userID is empty")
	}
	if in.Version < 1 {
		return paramErr("override version must be >= 1, got %d", in.Version)
	}
	if in.Kind != OverrideAllow && in.Kind != OverrideBlock {
		return paramErr("override kind must be %q or %q, got %q", OverrideAllow, OverrideBlock, in.Kind)
	}
	if in.Operator == "" {
		return paramErr("override operator is empty")
	}
	if in.Reason == "" {
		return paramErr("override reason is empty")
	}
	start := in.StartAt.UTC()
	end := in.EndAt.UTC()
	if !end.After(start) {
		return paramErr("override time range invalid: end %v must be after start %v", end, start)
	}
	return nil
}

func overrideCreatePayload(in OverrideInput) string {
	return fmt.Sprintf("create\x00%s\x00%s\x00%d\x00%s\x00%d\x00%d\x00%s\x00%s",
		in.Feature, in.UserID, in.Version, in.Kind,
		in.StartAt.UnixNano(), in.EndAt.UnixNano(), in.Reason, in.Operator)
}

func cloneOverride(o *Override) *Override {
	cp := *o
	return &cp
}

// CreateOverride 为指定功能与用户创建一条有期限的放行/屏蔽覆盖。
//
// 覆盖绑定创建时该功能的当前规则版本与发布代际：版本切换（发布/回滚）后
// 立即失效，不会自动沿用。同一功能+用户在同一发布版本至多一条有效覆盖。
// 按 opID 幂等：同号重放且内容一致返回首次结果；任何字段变化返回 KindConflict。
func (s *Service) CreateOverride(opID string, in OverrideInput) (*OverrideResult, error) {
	if err := validateOverrideInput(in); err != nil {
		return nil, err
	}
	if opID == "" {
		return nil, paramErr("opID is empty")
	}
	in.StartAt = in.StartAt.UTC()
	in.EndAt = in.EndAt.UTC()
	payload := overrideCreatePayload(in)

	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, seen := s.overrideOps[opID]; seen {
		return s.replayOverride(prev, payload, "create")
	}

	rule := s.currentSnap.Rules[in.Feature]
	if rule == nil {
		return nil, notFoundErr("feature %q has no published version", in.Feature)
	}
	if rule.Version != in.Version {
		return nil, versionErr("override for feature %q must target current version %d, got %d",
			in.Feature, rule.Version, in.Version)
	}
	now := s.nowUTC()
	if !in.EndAt.After(now) {
		return nil, paramErr("override end %v must be in the future (now %v)", in.EndAt, now)
	}

	epoch := s.epoch[in.Feature]
	key := overrideKey(in.Feature, in.UserID, in.Version, epoch)
	if existing := s.ovState.active[key]; existing != nil {
		return nil, conflictErr("active override %q already exists for feature %q user %q version %d",
			existing.ID, in.Feature, in.UserID, in.Version)
	}

	s.ovSeq++
	ov := &Override{
		ID:        "ov-" + strconv.FormatUint(s.ovSeq, 10),
		Feature:   in.Feature,
		UserID:    in.UserID,
		Version:   in.Version,
		Epoch:     epoch,
		Kind:      in.Kind,
		StartAt:   in.StartAt,
		EndAt:     in.EndAt,
		Reason:    in.Reason,
		Operator:  in.Operator,
		CreatedAt: now,
		Status:    OverrideActive,
	}

	next := s.ovState.clone()
	next.byID[ov.ID] = ov
	next.active[key] = ov
	s.ovState = next
	s.overrideOps[opID] = overrideOp{action: "create", ovID: ov.ID, payload: payload}

	return &OverrideResult{Override: cloneOverride(ov), Replayed: false}, nil
}

// RevokeOverride 撤销一条覆盖。撤销只影响后续判定，已经产生的判定记录
// （Decisions）不会被改写。按 opID 幂等。
func (s *Service) RevokeOverride(opID string, in RevokeOverrideInput) (*OverrideResult, error) {
	if opID == "" {
		return nil, paramErr("opID is empty")
	}
	if in.Feature == "" || in.UserID == "" || in.Version < 1 {
		return nil, paramErr("revoke override requires non-empty feature, user and version >= 1")
	}
	payload := fmt.Sprintf("revoke\x00%s\x00%s\x00%d", in.Feature, in.UserID, in.Version)

	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, seen := s.overrideOps[opID]; seen {
		return s.replayOverride(prev, payload, "revoke")
	}

	epoch := s.epoch[in.Feature]
	ov := s.ovState.find(in.Feature, in.UserID, in.Version, epoch)
	if ov == nil {
		return nil, notFoundErr("no active override for feature %q user %q version %d",
			in.Feature, in.UserID, in.Version)
	}

	updated := *ov
	updated.Status = OverrideRevoked
	next := s.ovState.clone()
	next.byID[ov.ID] = &updated
	delete(next.active, overrideKey(ov.Feature, ov.UserID, ov.Version, ov.Epoch))
	s.ovState = next
	s.overrideOps[opID] = overrideOp{action: "revoke", ovID: ov.ID, payload: payload}

	return &OverrideResult{Override: cloneOverride(&updated), Replayed: false}, nil
}

// SweepExpiredOverrides 将所有 EndAt <= now 的有效覆盖标记为过期。
// 在线判定本身也按同一时间视图检查到期，因此扫描与判定/发布并发时
// 过期覆盖绝不会继续放行；扫描只是把过期状态物化，便于查询。
// 返回被标记过期的覆盖 ID 列表（按 ID 排序）。
func (s *Service) SweepExpiredOverrides() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sweepExpiredLocked(s.nowUTC())
}

func (s *Service) sweepExpiredLocked(now time.Time) []string {
	var expired []string
	for _, ov := range s.ovState.active {
		if !ov.EndAt.After(now) {
			expired = append(expired, ov.ID)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	sort.Strings(expired)

	next := s.ovState.clone()
	for _, id := range expired {
		ov := next.byID[id]
		updated := *ov
		updated.Status = OverrideExpired
		next.byID[id] = &updated
		delete(next.active, overrideKey(ov.Feature, ov.UserID, ov.Version, ov.Epoch))
	}
	s.ovState = next
	return expired
}

// retireSupersededOverrides 将所有绑定版本/代际已不是当前状态的有效覆盖
// 标记为 superseded。在发布与回滚的同一写临界区内调用，
// 与代际递增一起完成，保证旧版本覆盖不可能影响新的发布指针。
func (s *Service) retireSupersededOverrides() {
	var stale []string
	for _, ov := range s.ovState.active {
		rule := s.currentSnap.Rules[ov.Feature]
		if rule == nil || rule.Version != ov.Version || s.epoch[ov.Feature] != ov.Epoch {
			stale = append(stale, ov.ID)
		}
	}
	if len(stale) == 0 {
		return
	}
	next := s.ovState.clone()
	for _, id := range stale {
		ov := next.byID[id]
		updated := *ov
		updated.Status = OverrideSuperseded
		next.byID[id] = &updated
		delete(next.active, overrideKey(ov.Feature, ov.UserID, ov.Version, ov.Epoch))
	}
	s.ovState = next
}

// replayOverride 处理覆盖操作的幂等重放：内容一致返回首次结果，否则冲突。
func (s *Service) replayOverride(prev overrideOp, payload, action string) (*OverrideResult, error) {
	if prev.action != action || prev.payload != payload {
		return nil, conflictErr("opID already used with a different %s request", prev.action)
	}
	ov, ok := s.ovState.byID[prev.ovID]
	if !ok {
		return nil, notFoundErr("override %q referenced by previous operation not found", prev.ovID)
	}
	return &OverrideResult{Override: cloneOverride(ov), Replayed: true}, nil
}

// GetOverride 按 ID 查询覆盖（含已过期、已撤销、已被版本切换作废的）。
func (s *Service) GetOverride(id string) (*Override, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ov, ok := s.ovState.byID[id]
	if !ok {
		return nil, notFoundErr("override %q not found", id)
	}
	return cloneOverride(ov), nil
}

// ListOverrides 返回某功能+用户的全部覆盖历史，按 ID 排序。
// version > 0 时进一步限定规则版本。
func (s *Service) ListOverrides(feature, userID string, version int) []*Override {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Override
	for _, ov := range s.ovState.byID {
		if ov.Feature != feature || ov.UserID != userID {
			continue
		}
		if version > 0 && ov.Version != version {
			continue
		}
		out = append(out, cloneOverride(ov))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ActiveOverrides 返回当前仍处于生效窗口内的覆盖，按 ID 排序。
func (s *Service) ActiveOverrides() []*Override {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.nowUTC()
	out := make([]*Override, 0, len(s.ovState.active))
	for _, ov := range s.ovState.active {
		if ov.ActiveAt(now) {
			out = append(out, cloneOverride(ov))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Decisions 返回不可变的在线判定历史（按发生顺序），撤销覆盖不会改写它们。
func (s *Service) Decisions() []Decision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Decision, len(s.decisions))
	copy(out, s.decisions)
	return out
}

// ExtendOverride 将一条覆盖的到期时间延长到 NewEnd（必须严格晚于当前到期时间）。
// 覆盖的其他字段（含目标版本、生效时间）不变。按 opID 幂等。
func (s *Service) ExtendOverride(opID string, in ExtendOverrideInput) (*OverrideResult, error) {
	if opID == "" {
		return nil, paramErr("opID is empty")
	}
	if in.Feature == "" || in.UserID == "" || in.Version < 1 {
		return nil, paramErr("extend override requires non-empty feature, user and version >= 1")
	}
	in.NewEnd = in.NewEnd.UTC()
	payload := fmt.Sprintf("extend\x00%s\x00%s\x00%d\x00%d",
		in.Feature, in.UserID, in.Version, in.NewEnd.UnixNano())

	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, seen := s.overrideOps[opID]; seen {
		return s.replayOverride(prev, payload, "extend")
	}

	epoch := s.epoch[in.Feature]
	ov := s.ovState.find(in.Feature, in.UserID, in.Version, epoch)
	if ov == nil {
		return nil, notFoundErr("no active override for feature %q user %q version %d",
			in.Feature, in.UserID, in.Version)
	}
	if !in.NewEnd.After(ov.EndAt) {
		return nil, paramErr("new end %v must be later than current end %v", in.NewEnd, ov.EndAt)
	}

	updated := *ov
	updated.EndAt = in.NewEnd
	if in.Reason != "" {
		updated.Reason = in.Reason
	}
	next := s.ovState.clone()
	next.byID[ov.ID] = &updated
	next.active[overrideKey(ov.Feature, ov.UserID, ov.Version, ov.Epoch)] = &updated
	s.ovState = next
	s.overrideOps[opID] = overrideOp{action: "extend", ovID: ov.ID, payload: payload}

	return &OverrideResult{Override: cloneOverride(&updated), Replayed: false}, nil
}
