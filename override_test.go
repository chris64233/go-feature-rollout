package featurerollout

import (
	"sync"
	"testing"
	"time"
)

// fakeClock 是可在测试中固定/推进的时间源，不需要真实等待。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newServiceWithClock() (*Service, *fakeClock) {
	c := newFakeClock()
	return NewServiceWithClock(c.now), c
}

func overrideSvc(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	s, clock := newServiceWithClock()
	mustPublish(t, s, "pub-base", []RuleInput{
		{Feature: "search", Version: 1, Percentage: 0,
			Deps: []Dependency{{Feature: "dep", Version: 1}}},
		{Feature: "dep", Version: 1, Percentage: 100},
	})
	return s, clock
}

func TestOverrideCreateAndSources(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()

	res, err := s.CreateOverride("op-allow", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "emergency enable", Operator: "admin1",
	})
	if err != nil {
		t.Fatalf("CreateOverride: %v", err)
	}
	if res.Replayed || res.Override.ID == "" {
		t.Fatalf("unexpected create result: %+v", res)
	}

	ev := s.Evaluate("search", "alice")
	if !ev.Allowed || ev.Source != SourceAllowOverride || ev.OverrideID != res.Override.ID {
		t.Fatalf("alice eval = %+v, want allow via allow override", ev)
	}

	if ev := s.Evaluate("search", "bob"); ev.Allowed || ev.Source != SourceBucket {
		t.Fatalf("bob eval = %+v, want bucket deny", ev)
	}

	mustPublish(t, s, "pub-inc", []RuleInput{
		{Feature: "cfg", Version: 1, Percentage: 0, Include: []string{"carol"}},
	})
	_, err = s.CreateOverride("op-block", OverrideInput{
		Feature: "cfg", UserID: "carol", Version: 1, Kind: OverrideBlock,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "temporary block", Operator: "admin1",
	})
	if err != nil {
		t.Fatalf("CreateOverride block: %v", err)
	}
	ev = s.Evaluate("cfg", "carol")
	if ev.Allowed || ev.Source != SourceBlockOverride {
		t.Fatalf("carol eval = %+v, want block override", ev)
	}
}

func TestOverridePriority(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()

	mustPublish(t, s, "pub-dep2", []RuleInput{
		{Feature: "dep", Version: 2, Percentage: 0, Exclude: []string{"dave"}},
	})
	_, err := s.CreateOverride("op-dep-allow", OverrideInput{
		Feature: "search", UserID: "dave", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "bypass dep", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if ev := s.Evaluate("search", "dave"); !ev.Allowed || ev.Source != SourceAllowOverride {
		t.Fatalf("eval = %+v, allow override should beat dependency", ev)
	}

	_, err = s.CreateOverride("op-dup", OverrideInput{
		Feature: "search", UserID: "dave", Version: 1, Kind: OverrideBlock,
		StartAt: now, EndAt: now.Add(2 * time.Hour),
		Reason: "dup", Operator: "admin",
	})
	expectKind(t, err, KindConflict)
}

func TestOverrideVersionBound(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()

	res, err := s.CreateOverride("op-v1", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(24 * time.Hour),
		Reason: "hotfix", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	baseSeq := s.CurrentSnapshotSeq()

	mustPublish(t, s, "pub-v2", []RuleInput{
		{Feature: "search", Version: 2, Percentage: 0,
			Deps: []Dependency{{Feature: "dep", Version: 1}}},
	})
	if ev := s.Evaluate("search", "alice"); ev.Allowed || ev.Version != 2 || ev.Source != SourceBucket {
		t.Fatalf("after publish eval = %+v, old override must not carry over", ev)
	}
	stored, _ := s.GetOverride(res.Override.ID)
	if stored.Status != OverrideSuperseded {
		t.Fatalf("old override status = %q, want superseded", stored.Status)
	}

	_, err = s.CreateOverride("op-stale-version", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "late", Operator: "admin",
	})
	expectKind(t, err, KindVersion)

	if _, err := s.Rollback("rb-v1", baseSeq); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if ev := s.Evaluate("search", "alice"); ev.Allowed {
		t.Fatalf("eval after rollback = %+v, old override must not reactivate", ev)
	}
	if got, _ := s.GetOverride(res.Override.ID); got.Status != OverrideSuperseded {
		t.Fatalf("override status after rollback = %q", got.Status)
	}

	_, err = s.CreateOverride("op-v1-reborn", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "re-issue after rollback", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create after rollback: %v", err)
	}
	if ev := s.Evaluate("search", "alice"); !ev.Allowed || ev.Source != SourceAllowOverride {
		t.Fatalf("eval = %+v, want fresh override allow", ev)
	}

	// 发布不影响未涉及功能的覆盖；但回滚若让某功能版本变化，其覆盖同样作废。
	other, err := s.CreateOverride("op-dep-v1", OverrideInput{
		Feature: "dep", UserID: "zoe", Version: 1, Kind: OverrideBlock,
		StartAt: now, EndAt: now.Add(3 * time.Hour),
		Reason: "block dep user", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create dep override: %v", err)
	}
	mustPublish(t, s, "pub-unrelated", []RuleInput{
		{Feature: "unrelated", Version: 1, Percentage: 0},
	})
	if got, _ := s.GetOverride(other.Override.ID); got.Status != OverrideActive {
		t.Fatalf("unrelated publish must not retire other-feature override, got %q", got.Status)
	}
	if ev := s.Evaluate("dep", "zoe"); ev.Allowed || ev.Source != SourceBlockOverride {
		t.Fatalf("dep override should still block, got %+v", ev)
	}
}

func TestOverrideExpiryBoundaries(t *testing.T) {
	s, clock := overrideSvc(t)
	start := clock.now()

	res, err := s.CreateOverride("op-exp", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: start, EndAt: start.Add(time.Hour),
		Reason: "window", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if ev := s.Evaluate("search", "alice"); !ev.Allowed {
		t.Fatalf("at start: %+v", ev)
	}

	clock.advance(time.Hour - time.Nanosecond)
	if ev := s.Evaluate("search", "alice"); !ev.Allowed || ev.Source != SourceAllowOverride {
		t.Fatalf("just before end: %+v", ev)
	}

	clock.advance(time.Nanosecond)
	if ev := s.Evaluate("search", "alice"); ev.Allowed || ev.OverrideID != "" {
		t.Fatalf("at end: %+v, expired override must not allow", ev)
	}

	expired := s.SweepExpiredOverrides()
	if len(expired) != 1 || expired[0] != res.Override.ID {
		t.Fatalf("swept = %v, want %s", expired, res.Override.ID)
	}
	stored, _ := s.GetOverride(res.Override.ID)
	if stored.Status != OverrideExpired {
		t.Fatalf("status = %q, want expired", stored.Status)
	}
	if again := s.SweepExpiredOverrides(); len(again) != 0 {
		t.Fatalf("second sweep should be empty, got %v", again)
	}

	s2, clock2 := overrideSvc(t)
	future := clock2.now().Add(time.Hour)
	fres, err := s2.CreateOverride("op-future", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: future, EndAt: future.Add(time.Hour),
		Reason: "scheduled", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create future: %v", err)
	}
	if ev := s2.Evaluate("search", "alice"); ev.Allowed {
		t.Fatalf("future override must not allow before start: %+v", ev)
	}
	if swept := s2.SweepExpiredOverrides(); len(swept) != 0 {
		t.Fatalf("future override must not be swept: %v", swept)
	}
	clock2.advance(time.Hour)
	if ev := s2.Evaluate("search", "alice"); !ev.Allowed || ev.OverrideID != fres.Override.ID {
		t.Fatalf("at future start: %+v", ev)
	}
}

func TestOverrideRevokeAndHistory(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()
	res, err := s.CreateOverride("op-rev-me", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "temp", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if ev := s.Evaluate("search", "alice"); !ev.Allowed {
		t.Fatalf("before revoke: %+v", ev)
	}

	rev, err := s.RevokeOverride("op-revoke", RevokeOverrideInput{
		Feature: "search", UserID: "alice", Version: 1,
	})
	if err != nil || rev.Replayed {
		t.Fatalf("revoke = %+v, %v", rev, err)
	}
	if ev := s.Evaluate("search", "alice"); ev.Allowed || ev.Source != SourceBucket {
		t.Fatalf("after revoke: %+v", ev)
	}
	stored, _ := s.GetOverride(res.Override.ID)
	if stored.Status != OverrideRevoked {
		t.Fatalf("status = %q, want revoked", stored.Status)
	}

	_, err = s.RevokeOverride("op-revoke-again", RevokeOverrideInput{
		Feature: "search", UserID: "alice", Version: 1,
	})
	expectKind(t, err, KindNotFound)

	decisions := s.Decisions()
	if len(decisions) != 2 {
		t.Fatalf("decisions = %d, want 2", len(decisions))
	}
	if !decisions[0].Allowed || decisions[0].OverrideID != res.Override.ID {
		t.Fatalf("historical decision rewritten: %+v", decisions[0])
	}
	if decisions[1].Allowed || decisions[1].OverrideID != "" {
		t.Fatalf("post-revoke decision wrong: %+v", decisions[1])
	}

	// 撤销、扫描与判定并发：重放幂等，结果自洽。
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); s.SweepExpiredOverrides() }()
		go func() { defer wg.Done(); s.Evaluate("search", "alice") }()
		go func() {
			defer wg.Done()
			r, err := s.RevokeOverride("op-revoke", RevokeOverrideInput{
				Feature: "search", UserID: "alice", Version: 1,
			})
			if err != nil || !r.Replayed {
				t.Errorf("replayed revoke = %+v, %v", r, err)
			}
		}()
	}
	wg.Wait()

	hist := decisions[0]
	if got := s.Decisions()[0]; got != hist {
		t.Fatalf("historical decision changed after concurrent ops: before=%+v after=%+v", hist, got)
	}
}

func TestOverrideExtend(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()
	_, err := s.CreateOverride("op-ext-me", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "temp", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	clock.advance(time.Hour)
	if ev := s.Evaluate("search", "alice"); ev.Allowed {
		t.Fatalf("should be expired: %+v", ev)
	}

	// 覆盖到期后不再可延长（先扫描或直接查找都找不到 active）。
	s.SweepExpiredOverrides()
	_, err = s.ExtendOverride("op-ext-late", ExtendOverrideInput{
		Feature: "search", UserID: "alice", Version: 1,
		NewEnd: now.Add(3 * time.Hour),
	})
	expectKind(t, err, KindNotFound)

	// 在到期前延长：重置时钟场景重建，覆盖跨过原到期点仍然有效。
	s2, clock2 := overrideSvc(t)
	n2 := clock2.now()
	r2, err := s2.CreateOverride("op-ext2", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: n2, EndAt: n2.Add(time.Hour),
		Reason: "temp", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	ext, err := s2.ExtendOverride("op-extend", ExtendOverrideInput{
		Feature: "search", UserID: "alice", Version: 1,
		NewEnd: n2.Add(3 * time.Hour),
		Reason: "need more time",
	})
	if err != nil || ext.Replayed {
		t.Fatalf("extend = %+v, %v", ext, err)
	}
	if !ext.Override.EndAt.Equal(n2.Add(3*time.Hour)) || ext.Override.Reason != "need more time" {
		t.Fatalf("extended override = %+v", ext.Override)
	}
	clock2.advance(2 * time.Hour)
	if ev := s2.Evaluate("search", "alice"); !ev.Allowed || ev.OverrideID != r2.Override.ID {
		t.Fatalf("extended override should still allow past original end: %+v", ev)
	}

	// 新到期时间不能早于/等于当前到期时间。
	_, err = s2.ExtendOverride("op-extend-shrink", ExtendOverrideInput{
		Feature: "search", UserID: "alice", Version: 1,
		NewEnd: n2.Add(90 * time.Minute),
	})
	expectKind(t, err, KindParam)

	// 延长操作号重放幂等。
	replay, err := s2.ExtendOverride("op-extend", ExtendOverrideInput{
		Feature: "search", UserID: "alice", Version: 1,
		NewEnd: n2.Add(3 * time.Hour),
	})
	if err != nil || !replay.Replayed || replay.Override.ID != ext.Override.ID {
		t.Fatalf("extend replay = %+v, %v", replay, err)
	}
}

func TestOverrideIdempotencyAndConflicts(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()
	in := OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "hot", Operator: "admin",
	}
	first, err := s.CreateOverride("op-same", in)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// 完全相同重放：返回首次结果，不产生第二条覆盖。
	replay, err := s.CreateOverride("op-same", in)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.Override.ID != first.Override.ID {
		t.Fatalf("replay = %+v, want same override %s", replay, first.Override.ID)
	}
	if len(s.ListOverrides("search", "alice", 1)) != 1 {
		t.Fatalf("replay must not create a second override")
	}

	// 任一字段变化（用户、功能、版本、时间、原因、操作人、种类）都冲突。
	cases := []func(o *OverrideInput){
		func(o *OverrideInput) { o.UserID = "bob" },
		func(o *OverrideInput) { o.Feature = "dep" },
		func(o *OverrideInput) { o.Version = 2 },
		func(o *OverrideInput) { o.Kind = OverrideBlock },
		func(o *OverrideInput) { o.StartAt = now.Add(time.Minute) },
		func(o *OverrideInput) { o.EndAt = now.Add(2 * time.Hour) },
		func(o *OverrideInput) { o.Reason = "other" },
		func(o *OverrideInput) { o.Operator = "root" },
	}
	for i, mutate := range cases {
		bad := in
		mutate(&bad)
		if bad.Feature == "dep" {
			bad.Version = 1
		}
		_, err := s.CreateOverride("op-same", bad)
		if kind, ok := KindOf(err); !ok || kind != KindConflict {
			t.Fatalf("case %d: expected conflict, got %v", i, err)
		}
	}

	// 同一操作号不能换一种操作类型重放。
	_, err = s.RevokeOverride("op-same", RevokeOverrideInput{
		Feature: "search", UserID: "alice", Version: 1,
	})
	expectKind(t, err, KindConflict)
}

func TestOverrideParamAndStateValidation(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()

	base := OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(time.Hour),
		Reason: "ok", Operator: "admin",
	}
	badCases := []OverrideInput{
		{Feature: "", UserID: "a", Version: 1, Kind: OverrideAllow, StartAt: now, EndAt: now.Add(time.Hour), Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "", Version: 1, Kind: OverrideAllow, StartAt: now, EndAt: now.Add(time.Hour), Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "a", Version: 0, Kind: OverrideAllow, StartAt: now, EndAt: now.Add(time.Hour), Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "a", Version: 1, Kind: "weird", StartAt: now, EndAt: now.Add(time.Hour), Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "a", Version: 1, Kind: OverrideAllow, StartAt: now, EndAt: now, Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "a", Version: 1, Kind: OverrideAllow, StartAt: now.Add(time.Hour), EndAt: now, Reason: "r", Operator: "o"},
		{Feature: "f", UserID: "a", Version: 1, Kind: OverrideAllow, StartAt: now, EndAt: now.Add(time.Hour), Reason: "", Operator: "o"},
		{Feature: "f", UserID: "a", Version: 1, Kind: OverrideAllow, StartAt: now, EndAt: now.Add(time.Hour), Reason: "r", Operator: ""},
	}
	for i, in := range badCases {
		_, err := s.CreateOverride("", in)
		if kind, ok := KindOf(err); !ok || kind != KindParam {
			t.Errorf("bad case %d: want param error, got %v", i, err)
		}
	}

	// 未发布的功能 / 不存在的版本不能挂覆盖。
	ghost := base
	ghost.Feature = "ghost"
	_, err := s.CreateOverride("op-ghost", ghost)
	expectKind(t, err, KindNotFound)

	old := base
	old.Version = 99
	_, err = s.CreateOverride("op-old", old)
	expectKind(t, err, KindVersion)

	// 到期时间必须晚于当前时间。
	expired := base
	expired.EndAt = now.Add(-time.Minute)
	_, err = s.CreateOverride("op-past-end", expired)
	expectKind(t, err, KindParam)
}

func TestOverridePublishConcurrency(t *testing.T) {
	s, clock := overrideSvc(t)
	now := clock.now()

	_, err := s.CreateOverride("op-conc", OverrideInput{
		Feature: "search", UserID: "alice", Version: 1, Kind: OverrideAllow,
		StartAt: now, EndAt: now.Add(24 * time.Hour),
		Reason: "hot", Operator: "admin",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(4)
		go func() { defer wg.Done(); _ = s.SweepExpiredOverrides() }()
		go func() {
			defer wg.Done()
			r := s.Evaluate("search", "alice")
			// 结果只可能是：v1+覆盖放行，或 v2+分桶拒绝；绝不允许 v2 被旧覆盖放行。
			if r.Version == 2 && (r.Allowed || r.OverrideID != "") {
				t.Errorf("stale override leaked onto v2: %+v", r)
			}
			if r.Version == 1 && !r.Allowed {
				t.Errorf("v1 with active override must allow: %+v", r)
			}
		}()
		go func() {
			defer wg.Done()
			_, _ = s.Publish("pub-v2-once", []RuleInput{
				{Feature: "search", Version: 2, Percentage: 0,
					Deps: []Dependency{{Feature: "dep", Version: 1}}},
			})
		}()
		go func() {
			defer wg.Done()
			// 迟到的旧版本覆盖操作：当前已不是 v1 时必须失败，绝不能覆盖发布指针。
			_, _ = s.CreateOverride("op-late-v1", OverrideInput{
				Feature: "search", UserID: "bob", Version: 1, Kind: OverrideAllow,
				StartAt: now, EndAt: now.Add(24 * time.Hour),
				Reason: "late", Operator: "admin",
			})
		}()
	}
	wg.Wait()

	if s.CurrentRule("search").Version != 2 {
		t.Fatalf("current version = %d, want 2", s.CurrentRule("search").Version)
	}
	if ev := s.Evaluate("search", "alice"); ev.Allowed || ev.Source != SourceBucket {
		t.Fatalf("final eval = %+v, want bucket deny on v2", ev)
	}
}
