package featurerollout

import (
	"fmt"
	"sort"
	"sync"
)

// snapshot 一份已发布、不可变的全量当前版本视图。
// 在线判定沿依赖链读取的始终是同一份 snapshot，绝不混用新旧版本。
type snapshot struct {
	seq     int64
	current map[FeatureKey]*RuleVersion
}

// Service 功能灰度发布服务：草拟、校验、原子发布、回滚与在线判定。
//
// 并发模型：写路径（发布/回滚）由互斥锁串行化，发布序号严格单调；
// 读路径（判定）只读一份原子替换的 snapshot 指针，发布与判定并发时
// 判定要么看到旧快照整体、要么看到新快照整体，永远看不到中间态。
type Service struct {
	mu sync.Mutex

	// 写路径状态（仅持锁访问）。
	drafts   map[string]*Draft
	versions map[FeatureKey]map[VersionID]*RuleVersion // 全部已发布版本，不可变
	changes  map[ChangeID]*ChangeRecord                // 幂等键 -> 发布记录
	snaps    map[ChangeID]*snapshot                    // 每条发布记录对应的整份一致快照
	history  []*ChangeRecord                           // 发布历史，只追加
	seq      int64
	draftSeq int

	// current 当前生效快照。发布/回滚时整体替换指针，判定方无锁读取。
	current atomicSnapshot
}

// NewService 创建空服务。
func NewService() *Service {
	s := &Service{
		drafts:   make(map[string]*Draft),
		versions: make(map[FeatureKey]map[VersionID]*RuleVersion),
		changes:  make(map[ChangeID]*ChangeRecord),
		snaps:    make(map[ChangeID]*snapshot),
	}
	s.current.Store(&snapshot{current: map[FeatureKey]*RuleVersion{}})
	return s
}

// NewDraft 草拟一组功能版本（尚未校验、未发布）。
func (s *Service) NewDraft(specs []VersionSpec) *Draft {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draftSeq++
	d := &Draft{
		ID:       fmt.Sprintf("draft-%d", s.draftSeq),
		Versions: cloneSpecs(specs),
	}
	s.drafts[d.ID] = d
	return d
}

// UpdateDraft 修改草稿内容（发布前可反复修改）。
func (s *Service) UpdateDraft(draftID string, specs []VersionSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.drafts[draftID]
	if !ok {
		return fmt.Errorf("%w: draft %q", ErrNotFound, draftID)
	}
	d.Versions = cloneSpecs(specs)
	return nil
}

// ValidateDraft 校验草稿：参数、版本占用、依赖存在性与无环。
// 校验通过不代表发布成功，发布时会以当时状态重新完整校验。
func (s *Service) ValidateDraft(draftID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.drafts[draftID]
	if !ok {
		return fmt.Errorf("%w: draft %q", ErrNotFound, draftID)
	}
	_, err := s.validateLocked(d.Versions)
	return err
}

// Publish 把草稿整组原子发布：同组版本要么全部生效、要么全部失败。
//
// 幂等：同一 changeID 且内容相同的重复请求返回首次发布的记录；
// 同一 changeID 内容不同报 ErrChangeConflict。
func (s *Service) Publish(changeID ChangeID, draftID string) (*ChangeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.drafts[draftID]
	if !ok {
		return nil, fmt.Errorf("%w: draft %q", ErrNotFound, draftID)
	}
	if err := checkChangeID(changeID); err != nil {
		return nil, err
	}
	if rec, ok := s.changes[changeID]; ok {
		if rec.Kind == ChangePublish && specsEqual(rec.Versions, normalizeSpecs(d.Versions)) {
			return rec, nil // 同号同内容：幂等重放
		}
		return nil, fmt.Errorf("%w: change id %q reused with different content", ErrChangeConflict, changeID)
	}

	versions, err := s.validateLocked(d.Versions)
	if err != nil {
		return nil, err
	}
	// 草稿发布后保留：同号重放需据草稿内容比对幂等；
	// 用新变更号重复发布同一草稿会因版本不可变报 ErrVersionConflict。
	return s.commitLocked(changeID, ChangePublish, "", versions), nil
}

// Rollback 回滚到某个历史发布记录的生效状态。
//
// 回滚不修改历史：它把目标记录发布后的整份一致快照重新激活为当前快照，
// 并追加一条 ChangeRollback 记录，发布序号照常单调递增。
// 幂等语义与 Publish 相同。
func (s *Service) Rollback(changeID ChangeID, targetChangeID ChangeID) (*ChangeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := checkChangeID(changeID); err != nil {
		return nil, err
	}
	if _, ok := s.changes[targetChangeID]; !ok {
		return nil, fmt.Errorf("%w: change id %q", ErrNotFound, targetChangeID)
	}
	if rec, ok := s.changes[changeID]; ok {
		if rec.Kind == ChangeRollback && rec.RollbackOf == targetChangeID {
			return rec, nil
		}
		return nil, fmt.Errorf("%w: change id %q reused with different content", ErrChangeConflict, changeID)
	}

	// 回滚 = 把目标记录对应的整份历史一致快照重新激活为当前快照，
	// 不修改任何历史记录。快照内容不可变，按值拷贝成新的版本组提交。
	targetSnap := s.snaps[targetChangeID]
	versions := make([]RuleVersion, 0, len(targetSnap.current))
	for _, rv := range targetSnap.current {
		versions = append(versions, *rv)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Feature < versions[j].Feature })
	return s.commitLocked(changeID, ChangeRollback, targetChangeID, versions), nil
}

// Evaluate 在线判定：用户对某功能当前版本是否放行。
//
// 整次判定（含沿依赖链的递归判定）只读取进入时的那一份快照。
// 判定顺序：显式排除 > 显式包含 > 前置依赖 > 百分比分桶。
func (s *Service) Evaluate(feature FeatureKey, user UserID) (bool, error) {
	if feature == "" || user == "" {
		return false, fmt.Errorf("%w: feature and user must be non-empty", ErrInvalidParam)
	}
	snap := s.current.Load()
	return evaluateInSnapshot(snap, feature, user, map[FeatureKey]bool{})
}

// evaluateInSnapshot 在同一份快照内沿依赖链递归判定。
// visiting 用于防御性环检测（发布时已保证无环，这里兜底避免死循环）。
func evaluateInSnapshot(snap *snapshot, feature FeatureKey, user UserID, visiting map[FeatureKey]bool) (bool, error) {
	rv, ok := snap.current[feature]
	if !ok {
		return false, fmt.Errorf("%w: feature %q has no published version", ErrNotFound, feature)
	}
	if visiting[feature] {
		return false, fmt.Errorf("%w: feature %q", ErrDependencyCycle, feature)
	}
	visiting[feature] = true
	defer delete(visiting, feature)

	for _, u := range rv.Audience.Exclude {
		if u == user {
			return false, nil
		}
	}
	for _, u := range rv.Audience.Include {
		if u == user {
			return true, nil
		}
	}
	for _, dep := range rv.DependsOn {
		ok, err := evaluateInSnapshot(snap, dep, user, visiting)
		if err != nil || !ok {
			return false, nil // 依赖未放行（含依赖功能不存在）：本功能不放行
		}
	}
	return inBucket(rv.Feature, user, rv.Percentage), nil
}

// History 返回按发布序号升序的发布历史（含回滚记录）。
func (s *Service) History() []*ChangeRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*ChangeRecord, len(s.history))
	copy(out, s.history)
	return out
}

// CurrentVersions 返回当前快照中各功能的当前版本号。
func (s *Service) CurrentVersions() map[FeatureKey]VersionID {
	snap := s.current.Load()
	out := make(map[FeatureKey]VersionID, len(snap.current))
	for f, rv := range snap.current {
		out[f] = rv.Version
	}
	return out
}

// GetVersion 读取某个已发布规则版本（只读视图）。
func (s *Service) GetVersion(feature FeatureKey, version VersionID) (*RuleVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if vs, ok := s.versions[feature]; ok {
		if rv, ok := vs[version]; ok {
			cp := *rv
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("%w: version %q of feature %q", ErrNotFound, version, feature)
}

// commitLocked 在锁内完成一次原子发布：构建新快照、整体换指针、追加历史。
// 前置条件：versions 已通过 validateLocked。
func (s *Service) commitLocked(changeID ChangeID, kind ChangeKind, rollbackOf ChangeID, versions []RuleVersion) *ChangeRecord {
	old := s.current.Load()
	next := &snapshot{
		seq:     old.seq + 1,
		current: make(map[FeatureKey]*RuleVersion, len(old.current)+len(versions)),
	}
	for f, rv := range old.current {
		next.current[f] = rv
	}
	prev := make(map[FeatureKey]VersionID, len(versions))
	for i := range versions {
		rv := versions[i] // 拷贝，RuleVersion 自此不可变
		next.current[rv.Feature] = &rv
		if oldRV, ok := old.current[rv.Feature]; ok {
			prev[rv.Feature] = oldRV.Version
		}
		if s.versions[rv.Feature] == nil {
			s.versions[rv.Feature] = map[VersionID]*RuleVersion{}
		}
		s.versions[rv.Feature][rv.Version] = &rv
	}

	s.seq++
	rec := &ChangeRecord{
		ChangeID:    changeID,
		Kind:        kind,
		Versions:    versions,
		RollbackOf:  rollbackOf,
		PrevCurrent: prev,
		Seq:         s.seq,
	}
	s.changes[changeID] = rec
	s.snaps[changeID] = next
	s.history = append(s.history, rec)
	s.current.Store(next) // 整组原子生效：判定方从此只见新快照整体
	return rec
}

// validateLocked 校验一组待发布版本，返回规范化后的不可变规则版本列表。
func (s *Service) validateLocked(specs []VersionSpec) ([]RuleVersion, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: empty version group", ErrInvalidParam)
	}
	seen := make(map[FeatureKey]bool, len(specs))
	versions := make([]RuleVersion, 0, len(specs))
	for _, sp := range specs {
		if sp.Feature == "" || sp.Version == "" {
			return nil, fmt.Errorf("%w: feature and version must be non-empty", ErrInvalidParam)
		}
		if sp.Percentage < 0 || sp.Percentage > 100 {
			return nil, fmt.Errorf("%w: percentage %d out of [0,100]", ErrInvalidParam, sp.Percentage)
		}
		if seen[sp.Feature] {
			return nil, fmt.Errorf("%w: duplicate feature %q in one group", ErrInvalidParam, sp.Feature)
		}
		seen[sp.Feature] = true
		if _, ok := s.versions[sp.Feature][sp.Version]; ok {
			return nil, fmt.Errorf("%w: version %q of feature %q already published", ErrVersionConflict, sp.Version, sp.Feature)
		}
		for _, dep := range sp.DependsOn {
			if dep == "" {
				return nil, fmt.Errorf("%w: empty dependency of feature %q", ErrInvalidParam, sp.Feature)
			}
		}
		versions = append(versions, RuleVersion{
			Feature:    sp.Feature,
			Version:    sp.Version,
			Percentage: sp.Percentage,
			Audience: Audience{
				Include: append([]UserID(nil), sp.Audience.Include...),
				Exclude: append([]UserID(nil), sp.Audience.Exclude...),
			},
			DependsOn: append([]FeatureKey(nil), sp.DependsOn...),
		})
	}
	if err := s.checkDepsLocked(versions); err != nil {
		return nil, err
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Feature < versions[j].Feature })
	return versions, nil
}

// checkDepsLocked 校验依赖真实存在且发布后与现有当前版本合图无环。
//
// 依赖按"发布后该功能的当前版本"解析：同组内的新版本优先，
// 否则取已发布的当前版本；依赖的功能没有任何已发布版本时报
// ErrDependencyNotFound。
func (s *Service) checkDepsLocked(versions []RuleVersion) error {
	inGroup := make(map[FeatureKey]*RuleVersion, len(versions))
	for i := range versions {
		inGroup[versions[i].Feature] = &versions[i]
	}
	// 解析后的依赖图：功能 -> 其当前版本依赖的功能集合。
	graph := make(map[FeatureKey][]FeatureKey)
	var resolve func(f FeatureKey) error
	resolved := make(map[FeatureKey]bool)
	resolve = func(f FeatureKey) error {
		if resolved[f] {
			return nil
		}
		resolved[f] = true
		var deps []FeatureKey
		if rv, ok := inGroup[f]; ok {
			deps = rv.DependsOn
		} else if cur, ok := s.current.Load().current[f]; ok {
			deps = cur.DependsOn
		} else {
			return fmt.Errorf("%w: feature %q", ErrDependencyNotFound, f)
		}
		graph[f] = deps
		for _, d := range deps {
			if err := resolve(d); err != nil {
				return err
			}
		}
		return nil
	}
	for _, rv := range versions {
		if err := resolve(rv.Feature); err != nil {
			return err
		}
	}
	// 三色标记 DFS 判环。
	const (
		white = iota // 未访问
		gray         // 在栈上
		black        // 已完成
	)
	color := make(map[FeatureKey]int, len(graph))
	var visit func(f FeatureKey) error
	visit = func(f FeatureKey) error {
		color[f] = gray
		for _, d := range graph[f] {
			switch color[d] {
			case gray:
				return fmt.Errorf("%w: %q -> %q", ErrDependencyCycle, f, d)
			case white:
				if err := visit(d); err != nil {
					return err
				}
			}
		}
		color[f] = black
		return nil
	}
	for f := range graph {
		if color[f] == white {
			if err := visit(f); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkChangeID(id ChangeID) error {
	if id == "" {
		return fmt.Errorf("%w: change id must be non-empty", ErrInvalidParam)
	}
	return nil
}

// normalizeSpecs 把草稿内容规范化为 RuleVersion 列表，用于幂等内容比较。
func normalizeSpecs(specs []VersionSpec) []RuleVersion {
	out := make([]RuleVersion, 0, len(specs))
	for _, sp := range specs {
		out = append(out, RuleVersion{
			Feature:    sp.Feature,
			Version:    sp.Version,
			Percentage: sp.Percentage,
			Audience: Audience{
				Include: append([]UserID(nil), sp.Audience.Include...),
				Exclude: append([]UserID(nil), sp.Audience.Exclude...),
			},
			DependsOn: append([]FeatureKey(nil), sp.DependsOn...),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Feature < out[j].Feature })
	return out
}

// specsEqual 幂等比较：两组规则版本内容完全一致（与顺序无关的字段已规范化）。
func specsEqual(a, b []RuleVersion) bool {
	if len(a) != len(b) {
		return false
	}
	norm := func(vs []RuleVersion) []RuleVersion {
		out := make([]RuleVersion, len(vs))
		for i, v := range vs {
			v.Audience.Include = sortedCopy(v.Audience.Include)
			v.Audience.Exclude = sortedCopy(v.Audience.Exclude)
			v.DependsOn = sortedCopy(v.DependsOn)
			out[i] = v
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Feature < out[j].Feature })
		return out
	}
	na, nb := norm(a), norm(b)
	for i := range na {
		x, y := na[i], nb[i]
		if x.Feature != y.Feature || x.Version != y.Version || x.Percentage != y.Percentage {
			return false
		}
		if !strSliceEqual(x.Audience.Include, y.Audience.Include) ||
			!strSliceEqual(x.Audience.Exclude, y.Audience.Exclude) ||
			!strSliceEqual(x.DependsOn, y.DependsOn) {
			return false
		}
	}
	return true
}

func cloneSpecs(specs []VersionSpec) []VersionSpec {
	out := make([]VersionSpec, len(specs))
	for i, sp := range specs {
		sp.Audience.Include = append([]UserID(nil), sp.Audience.Include...)
		sp.Audience.Exclude = append([]UserID(nil), sp.Audience.Exclude...)
		sp.DependsOn = append([]FeatureKey(nil), sp.DependsOn...)
		out[i] = sp
	}
	return out
}

func sortedCopy[T ~string](in []T) []T {
	out := append([]T(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func strSliceEqual[T ~string](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
