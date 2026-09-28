package featurerollout

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Service 是功能灰度发布服务。
//
// 所有方法对并发安全。内部用一把读写锁保证：
//   - 发布、回滚整体串行，序列号单调递增；
//   - 在线判定在锁内拷贝出一份不可变 Snapshot 后，在锁外沿依赖链求值，
//     期间读到的始终是同一个已发布快照，看不到任何中间态。
type Service struct {
	mu sync.RWMutex

	drafts map[string]*Draft

	// 每个功能所有已发布版本（不可变）。
	versions map[string]map[int]*RuleVersion
	// 每个功能当前版本号。
	current map[string]int

	// 当前生效的整组快照；currentSnap.Rules 中的指针与 versions 中的相同。
	currentSnap *Snapshot
	// 按序列号保存每一份曾经生效的快照，用于回滚时重新激活历史一致快照。
	snapshots map[uint64]*Snapshot
	// 单调递增的发布序列号。
	seq     uint64
	history []PublishRecord

	// 外部变更号 -> 该变更号首次成功发布产生的记录。
	// 同号重复提交时比对内容哈希：一致则幂等返回，不一致报冲突。
	changeSeq  map[string]uint64
	changeHash map[string]string

	// ---- 线上结果守卫 ----

	// stats 按 "feature@version" 保存每个版本的去重计数，版本之间完全隔离。
	stats map[string]*versionStats
	// resultIndex 是全局结果去重索引：ResultID -> 所属统计键，
	// 重复上报不重复计数，同一 ID 改挂别的版本按冲突拒绝。
	resultIndex map[string]string
	// paused 记录每个功能已被自动暂停的版本及其事件：同一版本只允许暂停一次，
	// 之后的重复上报、重复检查都不会再产生状态变化和通知。
	pauseEventsByVersion map[string]map[int]*PauseEvent
	// pauseEvents 永久保留每次自动暂停的统计依据，顺序即发生顺序。
	pauseEvents []*PauseEvent
	// notifier 在每次"首次"自动暂停成功后、锁外被调用；为 nil 则不通知。
	notifier func(PauseEvent)
}

// versionStats 是某个功能某个版本的去重结果集合。
type versionStats struct {
	feature    string
	version    int
	resultIDs  map[string]struct{}
	failureIDs map[string]struct{}
}

func statsKey(feature string, version int) string {
	return fmt.Sprintf("%s@%d", feature, version)
}

// NewService 创建空服务。
func NewService() *Service {
	s := &Service{
		drafts:               map[string]*Draft{},
		versions:             map[string]map[int]*RuleVersion{},
		current:              map[string]int{},
		snapshots:            map[uint64]*Snapshot{},
		changeSeq:            map[string]uint64{},
		changeHash:           map[string]string{},
		stats:                map[string]*versionStats{},
		resultIndex:          map[string]string{},
		pauseEventsByVersion: map[string]map[int]*PauseEvent{},
	}
	empty := &Snapshot{Seq: 0, Rules: map[string]*RuleVersion{}}
	s.currentSnap = empty
	s.snapshots[0] = empty
	return s
}

// SetNotifier 设置自动暂停通知回调。回调在暂停提交成功后、服务锁外被调用，
// 因此每次自动暂停至多通知一次（与 ReportResult.Paused 一一对应）；
// 重复上报、重复检查、过期拒绝都不会触发回调。回调内应避免再调用本服务的加锁方法。
func (s *Service) SetNotifier(f func(PauseEvent)) {
	s.mu.Lock()
	s.notifier = f
	s.mu.Unlock()
}

// ---------------- 草拟 ----------------

// CreateDraft 保存一组规则草拟。草拟本身不做发布语义校验，
// 但会校验字段基本取值与组内一致性（同一功能不能出现两次）。
func (s *Service) CreateDraft(changeID string, rules []RuleInput) (string, error) {
	if err := validateRules(rules); err != nil {
		return "", err
	}
	id := "draft-" + changeID
	d := &Draft{
		ID:       id,
		ChangeID: changeID,
		Rules:    cloneInputs(rules),
		Hash:     rulesHash(rules),
	}
	s.mu.Lock()
	s.drafts[id] = d
	s.mu.Unlock()
	return id, nil
}

// GetDraft 读取草拟内容。
func (s *Service) GetDraft(id string) (*Draft, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.drafts[id]
	if !ok {
		return nil, notFoundErr("draft %q not found", id)
	}
	return cloneDraft(d), nil
}

// ValidateDraft 对草拟做完整的发布前校验（依赖真实存在且不成环等），
// 不产生任何发布效果。返回 nil 表示当前状态下可以发布。
func (s *Service) ValidateDraft(id string) error {
	s.mu.RLock()
	d, ok := s.drafts[id]
	if !ok {
		s.mu.RUnlock()
		return notFoundErr("draft %q not found", id)
	}
	rules := cloneInputs(d.Rules)
	s.mu.RUnlock()

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validateLocked(rules)
}

// ---------------- 发布 ----------------

// PublishDraft 原子发布一个草拟中的多功能版本组：整组成功或整组失败。
// 按 changeID 幂等：同一变更号重复发布且内容一致时返回首次结果；
// 同号异内容返回 KindConflict。
func (s *Service) PublishDraft(id string) (*PublishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.drafts[id]
	if !ok {
		return nil, notFoundErr("draft %q not found", id)
	}
	return s.publishLocked(d.ChangeID, d.Rules, d.Hash, "publish", 0)
}

// Publish 直接原子发布一组规则（内部先做校验）。
// 按 changeID 幂等，语义同 PublishDraft。
func (s *Service) Publish(changeID string, rules []RuleInput) (*PublishResult, error) {
	if err := validateRules(rules); err != nil {
		return nil, err
	}
	hash := rulesHash(rules)
	rules = cloneInputs(rules)

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishLocked(changeID, rules, hash, "publish", 0)
}

// publishLocked 执行发布。调用方必须持有写锁。
// 任何一步校验失败都直接返回错误，不修改任何状态，保证整组失败。
func (s *Service) publishLocked(changeID string, rules []RuleInput, hash, kind string, rollbackTo uint64) (*PublishResult, error) {
	if changeID == "" {
		return nil, paramErr("changeID is empty")
	}

	// 幂等：同号成功过。
	if prevSeq, seen := s.changeSeq[changeID]; seen {
		if s.changeHash[changeID] == hash {
			rec := s.recordBySeq(prevSeq)
			return &PublishResult{Seq: prevSeq, ChangeID: changeID, Features: rec.Features}, nil
		}
		return nil, conflictErr("changeID %q already used with different content", changeID)
	}

	// 回滚只切换指针：版本库中没有任何新版本产生，目标快照原样重新激活。
	if kind == "rollback" {
		target := s.snapshots[rollbackTo]
		return s.commitRollbackLocked(changeID, hash, target)
	}
	if err := s.validateLocked(rules); err != nil {
		return nil, err
	}

	// 先在局部构建下一状态，全部成功后再一次性切换，避免任何中间态。
	nextVersions := map[string]map[int]*RuleVersion{}
	for f, m := range s.versions {
		cp := make(map[int]*RuleVersion, len(m))
		for v, r := range m {
			cp[v] = r
		}
		nextVersions[f] = cp
	}
	published := make([]*RuleVersion, 0, len(rules))
	for _, in := range rules {
		m := nextVersions[in.Feature]
		if m == nil {
			m = map[int]*RuleVersion{}
			nextVersions[in.Feature] = m
		}
		rv := toRuleVersion(in)
		m[in.Version] = rv
		published = append(published, rv)
	}

	nextRules := make(map[string]*RuleVersion, len(s.currentSnap.Rules)+len(rules))
	for f, r := range s.currentSnap.Rules {
		nextRules[f] = r
	}
	for _, rv := range published {
		nextRules[rv.Feature] = rv
	}

	s.seq++
	newSeq := s.seq
	snap := &Snapshot{Seq: newSeq, Rules: nextRules}

	features := make([]string, 0, len(rules))
	for _, rv := range published {
		features = append(features, rv.Feature)
	}
	sort.Strings(features)

	rec := PublishRecord{
		Seq:        newSeq,
		ChangeID:   changeID,
		Kind:       kind,
		Features:   features,
		RollbackTo: rollbackTo,
		Time:       time.Now().UTC(),
	}

	// 单点切换。
	s.versions = nextVersions
	for _, rv := range published {
		s.current[rv.Feature] = rv.Version
	}
	s.currentSnap = snap
	s.snapshots[newSeq] = snap
	s.history = append(s.history, rec)
	s.changeSeq[changeID] = newSeq
	s.changeHash[changeID] = hash

	return &PublishResult{Seq: newSeq, ChangeID: changeID, Features: features}, nil
}

func (s *Service) recordBySeq(seq uint64) PublishRecord {
	for _, r := range s.history { // 历史量通常可控
		if r.Seq == seq {
			return r
		}
	}
	return PublishRecord{Seq: seq}
}

// ---------------- 回滚 ----------------

// Rollback 重新激活一份历史一致快照（序列号 snapshotSeq）。
// 历史快照永不被修改，回滚只是把当前指针切回它并追加一条新的发布记录，
// 发布序列号继续单调递增。同样按 changeID 幂等。
func (s *Service) Rollback(changeID string, snapshotSeq uint64) (*PublishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if changeID == "" {
		return nil, paramErr("changeID is empty")
	}
	if _, ok := s.snapshots[snapshotSeq]; !ok {
		return nil, versionErr("snapshot seq %d does not exist", snapshotSeq)
	}
	if snapshotSeq == s.currentSnap.Seq {
		return nil, versionErr("snapshot seq %d is already current", snapshotSeq)
	}

	// 用回滚目标序列号构造幂等哈希，保证同号重放回滚能命中。
	hash := "rollback:" + strconv.FormatUint(snapshotSeq, 10)
	return s.publishLocked(changeID, nil, hash, "rollback", snapshotSeq)
}

// commitRollbackLocked 执行回滚的单点切换：不产生任何新版本，
// 只把当前指针切回历史快照（规则指针原样共享，历史快照永不修改），
// 并追加一条序列号单调递增的发布记录。调用方持有写锁。
func (s *Service) commitRollbackLocked(changeID, hash string, target *Snapshot) (*PublishResult, error) {
	s.seq++
	newSeq := s.seq
	snap := &Snapshot{Seq: newSeq, Rules: target.Rules}

	features := make([]string, 0, len(target.Rules))
	for f := range target.Rules {
		features = append(features, f)
	}
	sort.Strings(features)

	rec := PublishRecord{
		Seq:        newSeq,
		ChangeID:   changeID,
		Kind:       "rollback",
		Features:   features,
		RollbackTo: target.Seq,
		Time:       time.Now().UTC(),
	}

	nextCurrent := make(map[string]int, len(target.Rules))
	for f, r := range target.Rules {
		nextCurrent[f] = r.Version
	}

	s.currentSnap = snap
	s.snapshots[newSeq] = snap
	s.current = nextCurrent
	s.history = append(s.history, rec)
	s.changeSeq[changeID] = newSeq
	s.changeHash[changeID] = hash

	return &PublishResult{Seq: newSeq, ChangeID: changeID, Features: features}, nil
}

// ---------------- 判定 ----------------

// EvalResult 是一次在线判定的结果。
type EvalResult struct {
	Allowed bool
	Version int    // 命中的当前规则版本；功能不存在时为 0
	Seq     uint64 // 本次判定使用的已发布快照序列号
}

// Evaluate 在当前已发布快照上判定某用户对某功能是否放行。
//
// 整个判定沿依赖链使用同一份不可变 Snapshot：判定期间发生的发布/回滚
// 不会影响本次结果，也不会出现主功能读到新版本、依赖读到旧版本的混合态。
func (s *Service) Evaluate(feature, userID string) EvalResult {
	snap := s.snapshot()
	return s.evalSnapshot(snap, feature, userID, map[string]bool{})
}

// EvalAt 在指定历史快照上判定，主要用于回放/审计。
func (s *Service) EvalAt(seq uint64, feature, userID string) (EvalResult, error) {
	s.mu.RLock()
	snap, ok := s.snapshots[seq]
	s.mu.RUnlock()
	if !ok {
		return EvalResult{}, versionErr("snapshot seq %d does not exist", seq)
	}
	return s.evalSnapshot(snap, feature, userID, map[string]bool{}), nil
}

func (s *Service) snapshot() *Snapshot {
	s.mu.RLock()
	snap := s.currentSnap
	s.mu.RUnlock()
	return snap
}

func (s *Service) evalSnapshot(snap *Snapshot, feature, userID string, visiting map[string]bool) EvalResult {
	rule, ok := snap.Rules[feature]
	if !ok {
		return EvalResult{Allowed: false, Version: 0, Seq: snap.Seq}
	}

	// 先沿依赖链判定。任一依赖：不存在 / 当前版本低于要求 / 对用户不放行，则整体拒绝。
	for _, dep := range rule.Deps {
		depRule, ok := snap.Rules[dep.Feature]
		if !ok || depRule.Version < dep.Version {
			return EvalResult{Allowed: false, Version: rule.Version, Seq: snap.Seq}
		}
		if visiting[dep.Feature] {
			// 发布时已保证不成环；此处是防御性处理。
			return EvalResult{Allowed: false, Version: rule.Version, Seq: snap.Seq}
		}
		visiting[dep.Feature] = true
		res := s.evalSnapshot(snap, dep.Feature, userID, visiting)
		delete(visiting, dep.Feature)
		if !res.Allowed {
			return EvalResult{Allowed: false, Version: rule.Version, Seq: snap.Seq}
		}
	}

	// 排除优先，其次明确包含，最后确定性百分比分桶。
	if _, excluded := rule.Exclude[userID]; excluded {
		return EvalResult{Allowed: false, Version: rule.Version, Seq: snap.Seq}
	}
	if _, included := rule.Include[userID]; included {
		return EvalResult{Allowed: true, Version: rule.Version, Seq: snap.Seq}
	}
	allowed := rule.Percentage > 0 && bucket(feature, userID) < rule.Percentage
	return EvalResult{Allowed: allowed, Version: rule.Version, Seq: snap.Seq}
}

// ---------------- 查询 ----------------

// CurrentSnapshotSeq 返回当前生效快照的序列号。
func (s *Service) CurrentSnapshotSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentSnap.Seq
}

// History 返回发布历史（含回滚），按序列号升序。
func (s *Service) History() []PublishRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]PublishRecord, len(s.history))
	copy(out, s.history)
	return out
}

// CurrentRule 返回某功能当前已发布版本；不存在返回 nil。
func (s *Service) CurrentRule(feature string) *RuleVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if r, ok := s.currentSnap.Rules[feature]; ok {
		return cloneRule(r)
	}
	return nil
}

// PublishedVersions 返回某功能所有已发布版本号，升序。
func (s *Service) PublishedVersions(feature string) []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.versions[feature]
	out := make([]int, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// ---------------- 校验 ----------------

// validateLocked 在"当前已发布状态 + 本组新规则"构成的候选状态上做完整校验。
// 调用方持有读锁或写锁均可，但校验期间不得有写操作（写路径在写锁内调用）。
func (s *Service) validateLocked(rules []RuleInput) error {
	inGroup := map[string]RuleInput{}
	for _, r := range rules {
		if other, dup := inGroup[r.Feature]; dup {
			return paramErr("feature %q appears multiple times in one publication (versions %d and %d)",
				r.Feature, other.Version, r.Version)
		}
		inGroup[r.Feature] = r

		// 版本单调：发布后不可修改、版本号不可复用。
		if existing := s.versions[r.Feature]; existing != nil {
			if _, published := existing[r.Version]; published {
				return versionErr("feature %q version %d already published and is immutable", r.Feature, r.Version)
			}
			if r.Version <= s.current[r.Feature] {
				return versionErr("feature %q version %d must be greater than current version %d",
					r.Feature, r.Version, s.current[r.Feature])
			}
		}
	}

	// 组装候选状态下每个功能的当前版本，用于依赖解析与成环检测。
	candidateCurrent := map[string]int{}
	candidateDeps := map[string][]Dependency{}
	for f, v := range s.current {
		candidateCurrent[f] = v
		if r := s.currentSnap.Rules[f]; r != nil {
			candidateDeps[f] = r.Deps
		}
	}
	for _, r := range rules {
		candidateCurrent[r.Feature] = r.Version
		candidateDeps[r.Feature] = r.Deps
	}

	// 依赖必须真实存在（组内新发布版本或已发布版本），且要求版本不高于候选当前版本。
	for _, r := range rules {
		seen := map[string]bool{}
		for _, dep := range r.Deps {
			if dep.Feature == r.Feature {
				return cycleErr("feature %q version %d depends on itself", r.Feature, r.Version)
			}
			if seen[dep.Feature] {
				return paramErr("duplicate dependency on feature %q in feature %q version %d",
					dep.Feature, r.Feature, r.Version)
			}
			seen[dep.Feature] = true

			depVersion, exists := candidateCurrent[dep.Feature]
			if !exists {
				return dependencyErr("feature %q version %d depends on %q version %d, but feature %q has no published version",
					r.Feature, r.Version, dep.Feature, dep.Version, dep.Feature)
			}
			if dep.Version > depVersion {
				return dependencyErr("feature %q version %d requires %q version >= %d, but candidate current version is %d",
					r.Feature, r.Version, dep.Feature, dep.Version, depVersion)
			}
			if dep.Version < 1 {
				return paramErr("dependency version for %q must be >= 1", dep.Feature)
			}
		}
	}

	// 成环检测：候选整体依赖图上 DFS（含组外已发布功能的依赖边）。
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var visit func(string) error
	visit = func(f string) error {
		if color[f] == black {
			return nil
		}
		if color[f] == gray {
			return cycleErr("dependency cycle detected involving feature %q", f)
		}
		color[f] = gray
		for _, dep := range candidateDeps[f] {
			if err := visit(dep.Feature); err != nil {
				return err
			}
		}
		color[f] = black
		return nil
	}
	for f := range candidateCurrent {
		if err := visit(f); err != nil {
			return err
		}
	}
	return nil
}

// validateRules 校验请求字段本身的合法性，与当前发布状态无关。
func validateRules(rules []RuleInput) error {
	if len(rules) == 0 {
		return paramErr("rules are empty")
	}
	for _, r := range rules {
		if r.Feature == "" {
			return paramErr("feature name is empty")
		}
		if r.Version < 1 {
			return paramErr("feature %q version must be >= 1, got %d", r.Feature, r.Version)
		}
		if r.Percentage < 0 || r.Percentage > 100 {
			return paramErr("feature %q percentage must be in [0,100], got %d", r.Feature, r.Percentage)
		}
		if r.MinObservations < 0 {
			return paramErr("feature %q version %d MinObservations must be >= 0, got %d",
				r.Feature, r.Version, r.MinObservations)
		}
		if r.MaxFailureRate < 0 || r.MaxFailureRate > 1 {
			return paramErr("feature %q version %d MaxFailureRate must be in [0,1], got %v",
				r.Feature, r.Version, r.MaxFailureRate)
		}
		depFeatures := map[string]bool{}
		for _, dep := range r.Deps {
			if dep.Feature == "" {
				return paramErr("feature %q version %d has a dependency with empty feature name", r.Feature, r.Version)
			}
			if dep.Version < 1 {
				return paramErr("feature %q version %d depends on %q with invalid version %d",
					r.Feature, r.Version, dep.Feature, dep.Version)
			}
			if depFeatures[dep.Feature] {
				return paramErr("feature %q version %d has duplicate dependency on %q", r.Feature, r.Version, dep.Feature)
			}
			depFeatures[dep.Feature] = true
		}
	}
	return nil
}

// ---------------- 辅助函数 ----------------

func toRuleVersion(in RuleInput) *RuleVersion {
	rv := &RuleVersion{
		Feature:         in.Feature,
		Version:         in.Version,
		Percentage:      in.Percentage,
		Include:         map[string]struct{}{},
		Exclude:         map[string]struct{}{},
		Deps:            append([]Dependency(nil), in.Deps...),
		MinObservations: in.MinObservations,
		MaxFailureRate:  in.MaxFailureRate,
	}
	for _, u := range in.Include {
		rv.Include[u] = struct{}{}
	}
	for _, u := range in.Exclude {
		rv.Exclude[u] = struct{}{}
	}
	return rv
}

func cloneInputs(rules []RuleInput) []RuleInput {
	out := make([]RuleInput, len(rules))
	for i, r := range rules {
		cp := r
		cp.Include = append([]string(nil), r.Include...)
		cp.Exclude = append([]string(nil), r.Exclude...)
		cp.Deps = append([]Dependency(nil), r.Deps...)
		out[i] = cp
	}
	return out
}

func cloneDraft(d *Draft) *Draft {
	return &Draft{
		ID:       d.ID,
		ChangeID: d.ChangeID,
		Rules:    cloneInputs(d.Rules),
		Hash:     d.Hash,
	}
}

func cloneRule(r *RuleVersion) *RuleVersion {
	cp := *r
	cp.Include = map[string]struct{}{}
	cp.Exclude = map[string]struct{}{}
	for u := range r.Include {
		cp.Include[u] = struct{}{}
	}
	for u := range r.Exclude {
		cp.Exclude[u] = struct{}{}
	}
	cp.Deps = append([]Dependency(nil), r.Deps...)
	return &cp
}

// rulesHash 计算规则组内容的规范化哈希，用于"同号异内容"冲突检测。
func rulesHash(rules []RuleInput) string {
	cp := cloneInputs(rules)
	sort.SliceStable(cp, func(i, j int) bool {
		if cp[i].Feature != cp[j].Feature {
			return cp[i].Feature < cp[j].Feature
		}
		return cp[i].Version < cp[j].Version
	})
	h := sha256.New()
	for _, r := range cp {
		inc := append([]string(nil), r.Include...)
		exc := append([]string(nil), r.Exclude...)
		sort.Strings(inc)
		sort.Strings(exc)
		fmt.Fprintf(h, "%s@%d pct=%d guard=%d/%g", r.Feature, r.Version, r.Percentage,
			r.MinObservations, r.MaxFailureRate)
		for _, u := range inc {
			fmt.Fprintf(h, "|+%s", u)
		}
		for _, u := range exc {
			fmt.Fprintf(h, "|-%s", u)
		}
		deps := append([]Dependency(nil), r.Deps...)
		sort.Slice(deps, func(i, j int) bool { return deps[i].Feature < deps[j].Feature })
		for _, d := range deps {
			fmt.Fprintf(h, "|dep=%s@%d", d.Feature, d.Version)
		}
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}
