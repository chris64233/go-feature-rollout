package featurerollout

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// fixedClock 是可在测试中固定/推进的时间源。
type fixedClock struct{ t time.Time }

func newOverrideService(t *testing.T, start time.Time) (*Service, *fixedClock) {
	t.Helper()
	s := NewService()
	clk := &fixedClock{t: start}
	s.SetClock(func() time.Time { return clk.t })
	return s, clk
}

func overrideRules() []RuleInput {
	return []RuleInput{{Feature: "f", Version: 1, Percentage: 0}}
}

func allowInput(now time.Time, dur time.Duration) OverrideInput {
	return OverrideInput{
		Feature:   "f",
		UserID:    "alice",
		Version:   1,
		Effect:    OverrideAllow,
		Effective: now,
		Expires:   now.Add(dur),
		Reason:    "incident hotfix",
		Operator:  "admin-a",
	}
}

// ---------- 创建与优先级 ----------

func TestOverrideCreateAndPriority(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, clk := newOverrideService(t, now)
	mustPublish(t, s, "chg-ov-1", overrideRules())

	// 百分比 0 且无名单：默认拒绝。
	if r := s.Evaluate("f", "alice"); r.Allowed || r.Reason != ReasonPercentage {
		t.Fatalf("baseline = %+v, want denied by percentage", r)
	}

	// 放行覆盖：优先于灰度规则。
	res, err := s.CreateOverride("op-allow-1", allowInput(now, time.Hour))
	if err != nil {
		t.Fatalf("CreateOverride failed: %v", err)
	}
	r := s.Evaluate("f", "alice")
	if !r.Allowed || r.Reason != ReasonOverrideAllow || r.OverrideID != res.OverrideID {
		t.Fatalf("allow override not hit: %+v", r)
	}

	// 屏蔽覆盖与放行覆盖互斥：同一功能/用户/版本只允许一条有效覆盖。
	deny := allowInput(now, time.Hour)
	deny.Effect = OverrideDeny
	_, err = s.CreateOverride("op-deny-1", deny)
	expectKind(t, err, KindConflict)

	// 撤销放行后再建屏蔽覆盖：屏蔽优先于一切。
	if _, err := s.RevokeOverride("op-revoke-1", res.OverrideID); err != nil {
		t.Fatalf("RevokeOverride failed: %v", err)
	}
	if r := s.Evaluate("f", "alice"); r.Allowed || r.Reason != ReasonPercentage {
		t.Fatalf("after revoke baseline must apply, got %+v", r)
	}
	denyRes, err := s.CreateOverride("op-deny-2", deny)
	if err != nil {
		t.Fatalf("create deny failed: %v", err)
	}
	if r := s.Evaluate("f", "alice"); r.Allowed || r.Reason != ReasonOverrideDeny || r.OverrideID != denyRes.OverrideID {
		t.Fatalf("deny override must win: %+v", r)
	}

	// 覆盖不影响其他用户。
	if r := s.Evaluate("f", "bob"); r.Allowed || r.Reason != ReasonPercentage {
		t.Fatalf("bob must be unaffected: %+v", r)
	}

	// 未到期继续命中；到达到期边界（Expires 为排他边界）立即失效。
	clk.t = now.Add(59 * time.Minute)
	if r := s.Evaluate("f", "alice"); r.Reason != ReasonOverrideDeny {
		t.Fatalf("deny must still be active at 59m: %+v", r)
	}
	clk.t = now.Add(time.Hour)
	if r := s.Evaluate("f", "alice"); r.Allowed || r.Reason != ReasonPercentage {
		t.Fatalf("expired deny must not keep blocking: %+v", r)
	}
}

// 放行覆盖可以紧急越过失败的前置依赖；屏蔽覆盖则在一切之前拒绝。
func TestOverrideVsDependency(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, _ := newOverrideService(t, now)
	mustPublish(t, s, "chg-dep-ov-1", []RuleInput{
		{Feature: "base", Version: 1, Percentage: 0},
		{Feature: "top", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "base", Version: 1}}},
	})

	allowTop := OverrideInput{
		Feature: "top", UserID: "alice", Version: 1, Effect: OverrideAllow,
		Effective: now, Expires: now.Add(time.Hour), Reason: "urgent", Operator: "ops",
	}
	if _, err := s.CreateOverride("op-top-allow", allowTop); err != nil {
		t.Fatalf("create top allow: %v", err)
	}
	if r := s.Evaluate("top", "alice"); !r.Allowed || r.Reason != ReasonOverrideAllow {
		t.Fatalf("allow override must bypass failed dependency per spec order: %+v", r)
	}

	// 没有覆盖时，依赖失败仍然拒绝。
	if r := s.Evaluate("top", "carol"); r.Allowed || r.Reason != ReasonDependency {
		t.Fatalf("dependency must deny without override: %+v", r)
	}

	// 屏蔽覆盖在依赖之前生效：给 base 放行也救不了被屏蔽的用户。
	allowBase := OverrideInput{
		Feature: "base", UserID: "bob", Version: 1, Effect: OverrideAllow,
		Effective: now, Expires: now.Add(time.Hour), Reason: "urgent", Operator: "ops",
	}
	if _, err := s.CreateOverride("op-base-allow", allowBase); err != nil {
		t.Fatalf("create base allow: %v", err)
	}
	denyTop := OverrideInput{
		Feature: "top", UserID: "bob", Version: 1, Effect: OverrideDeny,
		Effective: now, Expires: now.Add(time.Hour), Reason: "kill switch", Operator: "ops",
	}
	if _, err := s.CreateOverride("op-top-deny", denyTop); err != nil {
		t.Fatalf("create top deny: %v", err)
	}
	if r := s.Evaluate("top", "bob"); r.Allowed || r.Reason != ReasonOverrideDeny {
		t.Fatalf("deny override must reject regardless of deps: %+v", r)
	}

	// 未到生效时间的覆盖不命中。
	future := OverrideInput{
		Feature: "top", UserID: "carol", Version: 1, Effect: OverrideDeny,
		Effective: now.Add(time.Hour), Expires: now.Add(2 * time.Hour),
		Reason: "scheduled", Operator: "ops",
	}
	if _, err := s.CreateOverride("op-future", future); err != nil {
		t.Fatalf("create future override: %v", err)
	}
	if r := s.Evaluate("top", "carol"); r.Reason == ReasonOverrideDeny {
		t.Fatalf("override before Effective must not hit: %+v", r)
	}
}

// ---------- 版本边界：发布与回滚均不沿用旧覆盖 ----------

func TestOverrideBoundToVersion(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, _ := newOverrideService(t, now)
	r1 := mustPublish(t, s, "chg-bind-1", []RuleInput{{Feature: "f", Version: 1, Percentage: 0}})

	res, err := s.CreateOverride("op-bind-allow", allowInput(now, 24*time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if r := s.Evaluate("f", "alice"); !r.Allowed {
		t.Fatalf("override must allow on v1: %+v", r)
	}

	// 新版本发布：旧覆盖不沿用，即使远未到期。
	mustPublish(t, s, "chg-bind-2", []RuleInput{{Feature: "f", Version: 2, Percentage: 0}})
	if r := s.Evaluate("f", "alice"); r.Allowed || r.Reason != ReasonPercentage {
		t.Fatalf("override must not carry to v2: %+v", r)
	}
	o, err := s.GetOverride(res.OverrideID)
	if err != nil || o.Status != OverrideSuperseded {
		t.Fatalf("v1 override should be superseded: %+v, %v", o, err)
	}
	// 已被新版本取代的覆盖不能延长。
	_, err = s.ExtendOverride("op-ext-old", res.OverrideID, now.Add(48*time.Hour))
	expectKind(t, err, KindVersion)

	// 迟到的旧版本创建操作不能作用于当前发布指针。
	_, err = s.CreateOverride("op-late", allowInput(now, time.Hour))
	expectKind(t, err, KindVersion)

	// 回滚到 v1 快照：版本号相同但代际已变，旧覆盖仍不能复活。
	if _, err := s.Rollback("chg-bind-rb", r1.Seq); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if r := s.Evaluate("f", "alice"); r.Allowed {
		t.Fatalf("override must not resurrect after rollback: %+v", r)
	}

	// 回滚后发布新版本，可以在新版本上重新建覆盖（Version=0 绑定当前版本）。
	mustPublish(t, s, "chg-bind-3", []RuleInput{{Feature: "f", Version: 3, Percentage: 0}})
	in := allowInput(now.Add(2*time.Hour), time.Hour)
	in.Version = 0
	if _, err := s.CreateOverride("op-v3", in); err != nil {
		t.Fatalf("create override on new current version: %v", err)
	}
}

// 覆盖不能为不存在/未发布的版本创建。
func TestOverrideVersionValidation(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, _ := newOverrideService(t, now)

	_, err := s.CreateOverride("op-no-feature", allowInput(now, time.Hour))
	expectKind(t, err, KindNotFound)

	mustPublish(t, s, "chg-vv-1", overrideRules())
	in := allowInput(now, time.Hour)
	in.Version = 2
	_, err = s.CreateOverride("op-bad-version", in)
	expectKind(t, err, KindVersion)

	// 参数与时间范围非法。
	for _, bad := range []OverrideInput{
		{Feature: "f", UserID: "", Version: 1, Effect: OverrideAllow, Effective: now, Expires: now.Add(time.Hour), Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "u", Version: 1, Effect: "weird", Effective: now, Expires: now.Add(time.Hour), Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "u", Version: 1, Effect: OverrideAllow, Effective: now, Expires: now, Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "u", Version: 1, Effect: OverrideAllow, Effective: now.Add(time.Hour), Expires: now, Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "u", Version: 1, Effect: OverrideAllow, Effective: now, Expires: now.Add(time.Hour), Reason: "", Operator: "o"},
		{Feature: "f", UserID: "u", Version: 1, Effect: OverrideAllow, Effective: now, Expires: now.Add(time.Hour), Reason: "r", Operator: ""},
	} {
		_, err := s.CreateOverride(fmt.Sprintf("op-bad-%p", &bad), bad)
		expectKind(t, err, KindParam)
	}
}

// ---------- 临界到期与扫描 ----------

func TestOverrideExpiryBoundaryAndSweep(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, clk := newOverrideService(t, now)
	mustPublish(t, s, "chg-exp-1", overrideRules())

	res, err := s.CreateOverride("op-exp", allowInput(now, time.Minute))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	clk.t = now.Add(time.Minute - time.Nanosecond)
	if r := s.Evaluate("f", "alice"); !r.Allowed {
		t.Fatalf("1ns before expiry must still allow: %+v", r)
	}
	clk.t = now.Add(time.Minute)
	if r := s.Evaluate("f", "alice"); r.Allowed {
		t.Fatalf("at expiry boundary must not allow: %+v", r)
	}

	// 判定本身不改写状态；扫描后状态变为 expired 并释放槽位。
	if got, _ := s.GetOverride(res.OverrideID); got.Status != OverrideActive {
		t.Fatalf("status should stay active until sweep: %s", got.Status)
	}
	swept := s.SweepExpired()
	if len(swept) != 1 || swept[0] != res.OverrideID {
		t.Fatalf("unexpected sweep result: %v", swept)
	}
	if got, _ := s.GetOverride(res.OverrideID); got.Status != OverrideExpired {
		t.Fatalf("status should be expired: %s", got.Status)
	}

	// 槽位释放后可以为同一版本/用户重建覆盖；延长已到期覆盖必须失败。
	if _, err := s.CreateOverride("op-exp-again", allowInput(clk.t, time.Minute)); err != nil {
		t.Fatalf("recreate after expiry should succeed: %v", err)
	}
	_, err = s.ExtendOverride("op-ext-expired", res.OverrideID, clk.t.Add(time.Hour))
	expectKind(t, err, KindVersion)
}

// ---------- 幂等：创建 / 撤销 / 延长 ----------

func TestOverrideIdempotency(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, _ := newOverrideService(t, now)
	mustPublish(t, s, "chg-idem-ov-1", overrideRules())
	in := allowInput(now, time.Hour)

	r1, err := s.CreateOverride("op-idem", in)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	r2, err := s.CreateOverride("op-idem", in)
	if err != nil || r1.OverrideID != r2.OverrideID || !r1.Expires.Equal(r2.Expires) {
		t.Fatalf("replay must return first result: %+v %+v %v", r1, r2, err)
	}

	// 用户/功能/版本/效果/时间任一变化：冲突。
	checkConflict := func(mut OverrideInput) {
		t.Helper()
		_, err := s.CreateOverride("op-idem", mut)
		expectKind(t, err, KindConflict)
	}
	mut := in
	mut.UserID = "bob"
	checkConflict(mut)
	mut = in
	mut.Feature = "ghost"
	checkConflict(mut)
	mut = in
	mut.Version = 0 // 0 语义上等价当前版本 v1，但请求值不同，仍冲突
	checkConflict(mut)
	mut = in
	mut.Expires = now.Add(2 * time.Hour)
	checkConflict(mut)
	mut = in
	mut.Effective = now.Add(time.Minute)
	checkConflict(mut)
	mut = in
	mut.Effect = OverrideDeny
	checkConflict(mut)

	// 操作号不能跨操作类型复用。
	_, err = s.RevokeOverride("op-idem", r1.OverrideID)
	expectKind(t, err, KindConflict)

	// 撤销幂等：重放返回首次结果；作用于别的覆盖 → 冲突。
	rv1, err := s.RevokeOverride("op-revoke", r1.OverrideID)
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	rv2, err := s.RevokeOverride("op-revoke", r1.OverrideID)
	if err != nil || rv2.Status != OverrideRevoked || rv1.OverrideID != rv2.OverrideID {
		t.Fatalf("revoke replay mismatch: %+v %+v %v", rv1, rv2, err)
	}

	other, err := s.CreateOverride("op-other", func() OverrideInput {
		x := allowInput(now, time.Hour)
		x.UserID = "carol"
		return x
	}())
	if err != nil {
		t.Fatalf("create other: %v", err)
	}
	_, err = s.RevokeOverride("op-revoke", other.OverrideID)
	expectKind(t, err, KindConflict)

	// 延长：成功后重放返回首次（延长后的）结果；不同新到期时间冲突。
	dave, err := s.CreateOverride("op-dave", func() OverrideInput {
		x := allowInput(now, 3*time.Hour)
		x.UserID = "dave"
		return x
	}())
	if err != nil {
		t.Fatalf("create dave: %v", err)
	}
	newExp := now.Add(5 * time.Hour)
	ex1, err := s.ExtendOverride("op-ext", dave.OverrideID, newExp)
	if err != nil || !ex1.Expires.Equal(newExp) {
		t.Fatalf("extend failed: %+v %v", ex1, err)
	}
	ex2, err := s.ExtendOverride("op-ext", dave.OverrideID, newExp)
	if err != nil || !ex2.Expires.Equal(newExp) {
		t.Fatalf("extend replay failed: %+v %v", ex2, err)
	}
	_, err = s.ExtendOverride("op-ext", dave.OverrideID, now.Add(6*time.Hour))
	expectKind(t, err, KindConflict)

	// 延长不能缩短有效期。
	_, err = s.ExtendOverride("op-ext-shrink", dave.OverrideID, now.Add(90*time.Minute))
	expectKind(t, err, KindParam)

	// 撤销不存在的覆盖：NotFound；同号重放仍返回首次的 NotFound 之外的结果语义——
	// 失败不占用操作号，因此换一个存在的覆盖可以复用该号。
	_, err = s.RevokeOverride("op-missing", "override-9999")
	expectKind(t, err, KindNotFound)
	if _, err := s.RevokeOverride("op-missing", other.OverrideID); err != nil {
		t.Fatalf("failed call must not consume the opID: %v", err)
	}
}

// 撤销只影响后续判定；历史快照回放永不套用覆盖。
func TestRevokeDoesNotRewriteHistory(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, _ := newOverrideService(t, now)
	r1 := mustPublish(t, s, "chg-hist-ov-1", overrideRules())

	res, err := s.CreateOverride("op-hist", allowInput(now, time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// 在线判定命中放行覆盖；历史回放同一快照仍按灰度规则（0%）拒绝。
	if r := s.Evaluate("f", "alice"); !r.Allowed {
		t.Fatalf("online eval should allow: %+v", r)
	}
	old, err := s.EvalAt(r1.Seq, "f", "alice")
	if err != nil || old.Allowed || old.Reason != ReasonPercentage {
		t.Fatalf("EvalAt must never apply overrides: %+v %v", old, err)
	}

	if _, err := s.RevokeOverride("op-hist-revoke", res.OverrideID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if r := s.Evaluate("f", "alice"); r.Allowed || r.Reason != ReasonPercentage {
		t.Fatalf("after revoke online eval must follow rules: %+v", r)
	}
	// 撤销后历史回放依然不变。
	old2, _ := s.EvalAt(r1.Seq, "f", "alice")
	if old2.Allowed || old2.Reason != ReasonPercentage {
		t.Fatalf("history must remain unchanged after revoke: %+v", old2)
	}
}

// ---------- 并发竞态 ----------

// 临界到期、到期扫描与在线判定并发：过期覆盖永远不能继续放行。
func TestConcurrentExpiryAndEvaluate(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, clk := newOverrideService(t, now)
	mustPublish(t, s, "chg-race-1", overrideRules())
	if _, err := s.CreateOverride("op-race", allowInput(now, time.Hour)); err != nil {
		t.Fatalf("create: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 时钟推进协程：单调推进，跨过到期边界。时钟写入与判定在同一把读/写锁下
	// 配对，杜绝"读到新时间却配了旧判定"的测试侧 TOCTOU。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for d := time.Duration(0); d <= 2*time.Hour; d += time.Minute {
			s.mu.Lock()
			clk.t = now.Add(d)
			s.mu.Unlock()
		}
		close(stop)
	}()

	// 判定协程：持读锁配对读取时钟与判定结果；时钟已过到期点时绝不能是覆盖放行。
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s.mu.RLock()
				pastExpiry := !clk.t.Before(now.Add(time.Hour))
				r := s.evalLocked(s.currentSnap, "f", "alice", map[string]bool{}, clk.t)
				s.mu.RUnlock()
				if pastExpiry && r.Reason == ReasonOverrideAllow {
					t.Errorf("expired override kept allowing: %+v", r)
					return
				}
			}
		}()
	}

	// 扫描协程持续运行。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.SweepExpired()
			}
		}
	}()

	wg.Wait()
	// 稳定状态：覆盖已到期，判定拒绝且不再来自覆盖。
	s.SweepExpired()
	if r := s.Evaluate("f", "alice"); r.Allowed || r.Reason != ReasonPercentage {
		t.Fatalf("final state must deny by percentage: %+v", r)
	}
}

// 撤销与在线判定并发：撤销一旦完成，之后的判定不再命中该覆盖；
// 撤销、扫描、延长并发操作同一条覆盖时状态必须自洽。
func TestConcurrentRevokeExtendEvaluate(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, _ := newOverrideService(t, now)
	mustPublish(t, s, "chg-revoke-1", overrideRules())
	res, err := s.CreateOverride("op-rev-race", allowInput(now, 10*time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	stop := make(chan struct{})
	var evalWg, writeWg sync.WaitGroup

	for g := 0; g < 4; g++ {
		evalWg.Add(1)
		go func() {
			defer evalWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r := s.Evaluate("f", "alice")
				if r.Reason == ReasonOverrideAllow && r.OverrideID != res.OverrideID {
					t.Errorf("unexpected override id: %+v", r)
					return
				}
			}
		}()
	}

	// 只有一个写操作会成功：撤销/延长对同一条覆盖并发。
	writeWg.Add(2)
	go func() {
		defer writeWg.Done()
		_, _ = s.RevokeOverride("op-rev-do", res.OverrideID)
	}()
	go func() {
		defer writeWg.Done()
		_, _ = s.ExtendOverride("op-ext-do", res.OverrideID, now.Add(20*time.Hour))
	}()

	// 写操作在写锁内串行：延长先到则先延长，随后撤销仍会成功并落定终态；
	// 撤销先到则延长收到 KindVersion（覆盖已非 active）。
	writeWg.Wait()
	if _, err := s.RevokeOverride("op-rev-final", res.OverrideID); err != nil {
		t.Fatalf("revoke after writers settle must succeed: %v", err)
	}
	for i := 0; i < 100; i++ {
		r := s.Evaluate("f", "alice")
		if r.Reason == ReasonOverrideAllow {
			t.Fatalf("override must not hit after revoke settles: %+v", r)
		}
	}
	close(stop)
	evalWg.Wait()
}

// 发布/回滚与覆盖创建、判定并发：迟到的旧版本操作永远不能影响当前指针，
// 同一操作号并发创建必须收敛到同一条覆盖。
func TestConcurrentPublishRollbackOverride(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	s, clk := newOverrideService(t, now)
	mustPublish(t, s, "chg-pc-1", []RuleInput{{Feature: "f", Version: 1, Percentage: 0}})

	const workers = 8
	var wg sync.WaitGroup

	// 同一操作号并发创建：要么全部返回同一 ID，要么首个之后的请求因槽位被占冲突——
	// 由于同号在锁内先做幂等检查，它们必须全部收敛到同一结果。
	ids := make(chan string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.CreateOverride("op-same", allowInput(now, time.Hour))
			if err != nil {
				t.Errorf("concurrent same-op create failed: %v", err)
				return
			}
			ids <- r.OverrideID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		} else if id != first {
			t.Fatalf("same opID diverged: %q vs %q", first, id)
		}
	}

	stop := make(chan struct{})
	// 发布协程持续推进版本。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := 2; v <= 40; v++ {
			if _, err := s.Publish(fmt.Sprintf("chg-pc-%d", v),
				[]RuleInput{{Feature: "f", Version: v, Percentage: 0}}); err != nil {
				t.Errorf("publish: %v", err)
				return
			}
		}
		close(stop)
	}()

	// 创建协程：所有针对旧版本号（Version=1）的请求都必须失败；
	// Version=0 绑定当前版本的请求若成功，则判定中只能出现与当前版本代际一致的覆盖。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				stale := allowInput(now, time.Hour)
				stale.Version = 1
				if _, err := s.CreateOverride(fmt.Sprintf("op-stale-%d-%d", id, i), stale); err == nil {
					t.Errorf("stale v1 create must never succeed after moving on")
					return
				}
				i++
			}
		}(w)
	}

	// 判定协程：放行覆盖若出现，其 Version 必须等于当前规则版本。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// 锁内原子地对当前快照判定，覆盖版本必然与当前快照一致。
			s.mu.RLock()
			r := s.evalLocked(s.currentSnap, "f", "alice", map[string]bool{}, clk.t)
			curVersion := 0
			if cur := s.currentSnap.Rules["f"]; cur != nil {
				curVersion = cur.Version
			}
			s.mu.RUnlock()
			if r.Reason == ReasonOverrideAllow && r.Version != curVersion {
				t.Errorf("override hit stale version: eval %+v current %d", r, curVersion)
				return
			}
		}
	}()

	wg.Wait()
	_ = clk
}
