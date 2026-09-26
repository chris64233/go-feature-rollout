package featurerollout

import (
	"fmt"
	"sync"
	"testing"
)

func mustPublish(t *testing.T, s *Service, changeID string, rules []RuleInput) *PublishResult {
	t.Helper()
	res, err := s.Publish(changeID, rules)
	if err != nil {
		t.Fatalf("Publish(%q) failed: %v", changeID, err)
	}
	return res
}

func expectKind(t *testing.T, err error, kind ErrorKind) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error of kind %q, got nil", kind)
	}
	got, ok := KindOf(err)
	if !ok || got != kind {
		t.Fatalf("expected error kind %q, got %v", kind, err)
	}
}

// ---------- 参数校验 ----------

func TestParamValidation(t *testing.T) {
	s := NewService()

	cases := []struct {
		name  string
		rules []RuleInput
	}{
		{"empty rules", nil},
		{"empty feature", []RuleInput{{Feature: "", Version: 1, Percentage: 50}}},
		{"version zero", []RuleInput{{Feature: "a", Version: 0, Percentage: 50}}},
		{"percentage negative", []RuleInput{{Feature: "a", Version: 1, Percentage: -1}}},
		{"percentage over 100", []RuleInput{{Feature: "a", Version: 1, Percentage: 101}}},
		{"dep empty feature", []RuleInput{{Feature: "a", Version: 1, Deps: []Dependency{{Feature: "", Version: 1}}}}},
		{"dep version zero", []RuleInput{{Feature: "a", Version: 1, Deps: []Dependency{{Feature: "b", Version: 0}}}}},
		{"duplicate dep", []RuleInput{{Feature: "a", Version: 1, Deps: []Dependency{
			{Feature: "b", Version: 1}, {Feature: "b", Version: 2},
		}}}},
		{"duplicate feature in group", []RuleInput{
			{Feature: "a", Version: 1},
			{Feature: "a", Version: 2},
		}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Publish(fmt.Sprintf("chg-param-%d", i), tc.rules)
			expectKind(t, err, KindParam)
		})
	}

	// 空变更号。
	_, err := s.Publish("", []RuleInput{{Feature: "a", Version: 1}})
	expectKind(t, err, KindParam)
}

// ---------- 依赖校验 ----------

func TestDependencyValidation(t *testing.T) {
	s := NewService()

	// 依赖的功能从未发布。
	_, err := s.Publish("chg-dep-1", []RuleInput{
		{Feature: "a", Version: 1, Deps: []Dependency{{Feature: "ghost", Version: 1}}},
	})
	expectKind(t, err, KindDependency)

	// 依赖的版本不存在（只发布过 v1，却依赖 v2）。
	mustPublish(t, s, "chg-dep-2", []RuleInput{{Feature: "b", Version: 1, Percentage: 100}})
	_, err = s.Publish("chg-dep-3", []RuleInput{
		{Feature: "a", Version: 1, Deps: []Dependency{{Feature: "b", Version: 2}}},
	})
	expectKind(t, err, KindDependency)

	// 依赖可以在同一发布组内满足。
	res := mustPublish(t, s, "chg-dep-4", []RuleInput{
		{Feature: "c", Version: 1, Percentage: 100},
		{Feature: "d", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "c", Version: 1}}},
	})
	if len(res.Features) != 2 {
		t.Fatalf("expected 2 features published, got %v", res.Features)
	}
}

// ---------- 循环依赖 ----------

func TestDependencyCycle(t *testing.T) {
	s := NewService()

	// 自依赖。
	_, err := s.Publish("chg-cyc-1", []RuleInput{
		{Feature: "a", Version: 1, Deps: []Dependency{{Feature: "a", Version: 1}}},
	})
	expectKind(t, err, KindCycle)

	// 同组内互相依赖。
	_, err = s.Publish("chg-cyc-2", []RuleInput{
		{Feature: "a", Version: 1, Deps: []Dependency{{Feature: "b", Version: 1}}},
		{Feature: "b", Version: 1, Deps: []Dependency{{Feature: "a", Version: 1}}},
	})
	expectKind(t, err, KindCycle)

	// 跨多次发布形成的环：a -> b -> c -> a。
	mustPublish(t, s, "chg-cyc-3", []RuleInput{
		{Feature: "x", Version: 1, Percentage: 100},
	})
	mustPublish(t, s, "chg-cyc-4", []RuleInput{
		{Feature: "y", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "x", Version: 1}}},
	})
	mustPublish(t, s, "chg-cyc-5", []RuleInput{
		{Feature: "z", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "y", Version: 1}}},
	})
	_, err = s.Publish("chg-cyc-6", []RuleInput{
		{Feature: "x", Version: 2, Percentage: 100, Deps: []Dependency{{Feature: "z", Version: 1}}},
	})
	expectKind(t, err, KindCycle)
}

// ---------- 版本不可变与单调 ----------

func TestVersionImmutability(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-ver-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 10}})
	mustPublish(t, s, "chg-ver-2", []RuleInput{{Feature: "a", Version: 3, Percentage: 20}})

	// 已发布版本不可复用（即使内容不同）。
	_, err := s.Publish("chg-ver-3", []RuleInput{{Feature: "a", Version: 1, Percentage: 99}})
	expectKind(t, err, KindVersion)
	_, err = s.Publish("chg-ver-4", []RuleInput{{Feature: "a", Version: 3, Percentage: 99}})
	expectKind(t, err, KindVersion)

	// 版本号必须大于当前版本。
	_, err = s.Publish("chg-ver-5", []RuleInput{{Feature: "a", Version: 2, Percentage: 30}})
	expectKind(t, err, KindVersion)

	// 历史版本仍然可查且未被修改。
	vs := s.PublishedVersions("a")
	if len(vs) != 2 || vs[0] != 1 || vs[1] != 3 {
		t.Fatalf("unexpected published versions: %v", vs)
	}
	if got := s.CurrentRule("a").Percentage; got != 20 {
		t.Fatalf("current percentage = %d, want 20", got)
	}
}

// ---------- 整组原子性 ----------

func TestAtomicGroupPublish(t *testing.T) {
	s := NewService()

	// 组内第二条规则依赖不存在的功能，整组必须失败，第一条也不能生效。
	_, err := s.Publish("chg-atomic-1", []RuleInput{
		{Feature: "ok", Version: 1, Percentage: 100},
		{Feature: "bad", Version: 1, Deps: []Dependency{{Feature: "ghost", Version: 1}}},
	})
	expectKind(t, err, KindDependency)

	if r := s.CurrentRule("ok"); r != nil {
		t.Fatalf("feature ok must not be visible after failed group publish, got %+v", r)
	}
	if got := s.Evaluate("ok", "anyone"); got.Allowed {
		t.Fatal("evaluation must not see partially published group")
	}
	if len(s.History()) != 0 {
		t.Fatalf("history must be empty after failed publish, got %+v", s.History())
	}
	if s.CurrentSnapshotSeq() != 0 {
		t.Fatalf("snapshot seq must stay 0, got %d", s.CurrentSnapshotSeq())
	}
}

// ---------- 幂等与冲突 ----------

func TestIdempotentPublish(t *testing.T) {
	s := NewService()
	rules := []RuleInput{{Feature: "a", Version: 1, Percentage: 50}}

	res1 := mustPublish(t, s, "chg-idem-1", rules)
	// 同号同内容：幂等返回首次结果，不产生新序列号。
	res2 := mustPublish(t, s, "chg-idem-1", rules)
	if res1.Seq != res2.Seq {
		t.Fatalf("idempotent publish produced new seq: %d vs %d", res1.Seq, res2.Seq)
	}
	if len(s.History()) != 1 {
		t.Fatalf("history should have exactly 1 record, got %d", len(s.History()))
	}

	// 同号异内容：冲突。
	_, err := s.Publish("chg-idem-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 60}})
	expectKind(t, err, KindConflict)
	_, err = s.Publish("chg-idem-1", []RuleInput{{Feature: "b", Version: 1, Percentage: 50}})
	expectKind(t, err, KindConflict)

	// 内容哈希与规则顺序、名单顺序无关。
	res3 := mustPublish(t, s, "chg-idem-2", []RuleInput{
		{Feature: "x", Version: 1, Include: []string{"u1", "u2"}},
		{Feature: "y", Version: 1},
	})
	res4 := mustPublish(t, s, "chg-idem-2", []RuleInput{
		{Feature: "y", Version: 1},
		{Feature: "x", Version: 1, Include: []string{"u2", "u1"}},
	})
	if res3.Seq != res4.Seq {
		t.Fatalf("canonical hash should ignore ordering: %d vs %d", res3.Seq, res4.Seq)
	}
}

// ---------- 草拟 / 校验 / 发布 ----------

func TestDraftValidatePublish(t *testing.T) {
	s := NewService()

	// 草拟只做基本校验，依赖缺失在 ValidateDraft 才暴露。
	id, err := s.CreateDraft("chg-draft-1", []RuleInput{
		{Feature: "a", Version: 1, Deps: []Dependency{{Feature: "ghost", Version: 1}}},
	})
	if err != nil {
		t.Fatalf("CreateDraft failed: %v", err)
	}
	expectKind(t, s.ValidateDraft(id), KindDependency)

	// 补上依赖后校验通过，再原子发布。
	mustPublish(t, s, "chg-draft-2", []RuleInput{{Feature: "ghost", Version: 1, Percentage: 100}})
	if err := s.ValidateDraft(id); err != nil {
		t.Fatalf("ValidateDraft should pass after dependency published: %v", err)
	}
	res, err := s.PublishDraft(id)
	if err != nil {
		t.Fatalf("PublishDraft failed: %v", err)
	}
	// 草拟发布同样按变更号幂等。
	res2, err := s.PublishDraft(id)
	if err != nil || res2.Seq != res.Seq {
		t.Fatalf("republishing same draft must be idempotent: %v, %+v", err, res2)
	}

	// 草拟不存在。
	expectKind(t, s.ValidateDraft("nope"), KindNotFound)
	_, err = s.PublishDraft("nope")
	expectKind(t, err, KindNotFound)
}

// ---------- 判定稳定性与百分比扩缩 ----------

func TestDeterministicEvaluation(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-det-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 30}})

	// 同一用户面对同一规则版本，结果始终稳定。
	first := s.Evaluate("a", "user-42")
	for i := 0; i < 100; i++ {
		if got := s.Evaluate("a", "user-42"); got != first {
			t.Fatalf("evaluation not stable: %+v vs %+v", first, got)
		}
	}

	// 记录 30% 下的受众。
	users := make([]string, 2000)
	for i := range users {
		users[i] = fmt.Sprintf("user-%d", i)
	}
	in30 := map[string]bool{}
	for _, u := range users {
		if s.Evaluate("a", u).Allowed {
			in30[u] = true
		}
	}

	// 上调到 60%：原受众必须全部保留（只扩大，不洗牌）。
	mustPublish(t, s, "chg-det-2", []RuleInput{{Feature: "a", Version: 2, Percentage: 60}})
	in60 := map[string]bool{}
	for _, u := range users {
		if s.Evaluate("a", u).Allowed {
			in60[u] = true
		}
	}
	for u := range in30 {
		if !in60[u] {
			t.Fatalf("user %s was allowed at 30%% but lost at 60%%", u)
		}
	}
	if len(in60) <= len(in30) {
		t.Fatalf("expected audience to grow: %d -> %d", len(in30), len(in60))
	}

	// 下调到 10%：受众只能是 30% 受众的子集（只收缩）。
	mustPublish(t, s, "chg-det-3", []RuleInput{{Feature: "a", Version: 3, Percentage: 10}})
	for _, u := range users {
		if s.Evaluate("a", u).Allowed && !in30[u] {
			t.Fatalf("user %s allowed at 10%% but was not allowed at 30%%", u)
		}
	}
}

// ---------- 包含 / 排除名单 ----------

func TestIncludeExclude(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-inc-1", []RuleInput{{
		Feature:    "a",
		Version:    1,
		Percentage: 0, // 百分比为 0，只有名单生效
		Include:    []string{"vip", "both"},
		Exclude:    []string{"banned", "both"},
	}})

	if !s.Evaluate("a", "vip").Allowed {
		t.Fatal("included user must be allowed even at 0%")
	}
	if s.Evaluate("a", "banned").Allowed {
		t.Fatal("excluded user must be denied")
	}
	if s.Evaluate("a", "both").Allowed {
		t.Fatal("exclude must win over include")
	}
	if s.Evaluate("a", "random").Allowed {
		t.Fatal("user outside lists must follow percentage (0%)")
	}
}

// ---------- 依赖链判定与快照一致性 ----------

func TestDependencyChainEvaluation(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-chain-1", []RuleInput{
		{Feature: "base", Version: 1, Percentage: 100},
		{Feature: "mid", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "base", Version: 1}}},
		{Feature: "top", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "mid", Version: 1}}},
	})

	if !s.Evaluate("top", "u1").Allowed {
		t.Fatal("top should be allowed when whole chain allows")
	}

	// 底层功能关闭后，整条链上的功能都必须关闭。
	mustPublish(t, s, "chg-chain-2", []RuleInput{
		{Feature: "base", Version: 2, Percentage: 0},
	})
	for _, f := range []string{"base", "mid", "top"} {
		if s.Evaluate(f, "u1").Allowed {
			t.Fatalf("%s must be denied after base is turned off", f)
		}
	}

	// 未发布的功能判定为不放行，而不是报错或 panic。
	if s.Evaluate("missing", "u1").Allowed {
		t.Fatal("missing feature must evaluate to false")
	}
}

// ---------- 回滚 ----------

func TestRollback(t *testing.T) {
	s := NewService()
	r1 := mustPublish(t, s, "chg-rb-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 100}})
	mustPublish(t, s, "chg-rb-2", []RuleInput{{Feature: "a", Version: 2, Percentage: 0}})

	if s.Evaluate("a", "u1").Allowed {
		t.Fatal("a should be off at v2")
	}

	// 回滚到 v1 生效时的快照：重新激活历史一致快照，而不是修改历史。
	rb, err := s.Rollback("chg-rb-3", r1.Seq)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if rb.Seq <= 2 {
		t.Fatalf("rollback must allocate a new monotonic seq, got %d", rb.Seq)
	}
	if !s.Evaluate("a", "u1").Allowed {
		t.Fatal("a should be on again after rollback")
	}
	if got := s.CurrentRule("a").Version; got != 1 {
		t.Fatalf("current version after rollback = %d, want 1", got)
	}

	// 历史记录完整保留，回滚只是追加。
	h := s.History()
	if len(h) != 3 || h[2].Kind != "rollback" || h[2].RollbackTo != r1.Seq {
		t.Fatalf("unexpected history: %+v", h)
	}
	for i := 1; i < len(h); i++ {
		if h[i].Seq <= h[i-1].Seq {
			t.Fatalf("history seq not monotonic: %+v", h)
		}
	}

	// 回滚同样按变更号幂等。
	rb2, err := s.Rollback("chg-rb-3", r1.Seq)
	if err != nil || rb2.Seq != rb.Seq {
		t.Fatalf("rollback must be idempotent: %v, %+v", err, rb2)
	}
	// 同号不同目标：冲突。
	_, err = s.Rollback("chg-rb-3", 2)
	expectKind(t, err, KindConflict)

	// 回滚目标不存在 / 就是当前快照：版本错误。
	_, err = s.Rollback("chg-rb-4", 999)
	expectKind(t, err, KindVersion)
	_, err = s.Rollback("chg-rb-5", s.CurrentSnapshotSeq())
	expectKind(t, err, KindVersion)

	// 回滚后可以在旧版本之上继续单调发布新版本。
	mustPublish(t, s, "chg-rb-6", []RuleInput{{Feature: "a", Version: 3, Percentage: 50}})
	if got := s.CurrentRule("a").Version; got != 3 {
		t.Fatalf("current version = %d, want 3", got)
	}
	// 已发布过的版本依然不可复用。
	_, err = s.Publish("chg-rb-7", []RuleInput{{Feature: "a", Version: 2, Percentage: 50}})
	expectKind(t, err, KindVersion)
}

// ---------- 历史快照回放 ----------

func TestEvalAtHistoricalSnapshot(t *testing.T) {
	s := NewService()
	r1 := mustPublish(t, s, "chg-hist-1", []RuleInput{{Feature: "a", Version: 1, Percentage: 100}})
	mustPublish(t, s, "chg-hist-2", []RuleInput{{Feature: "a", Version: 2, Percentage: 0}})

	res, err := s.EvalAt(r1.Seq, "a", "u1")
	if err != nil || !res.Allowed || res.Version != 1 {
		t.Fatalf("EvalAt(seq=%d) = %+v, %v", r1.Seq, res, err)
	}
	if _, err := s.EvalAt(999, "a", "u1"); err == nil {
		t.Fatal("EvalAt on missing snapshot must fail")
	}
}

// ---------- 并发：发布 / 回滚 / 判定 ----------

// 并发发布时序列号必须严格单调且无重复；同号并发发布幂等收敛。
func TestConcurrentPublishMonotonicSeq(t *testing.T) {
	s := NewService()

	const workers = 8
	const perWorker = 20

	var wg sync.WaitGroup
	seqs := make(chan uint64, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				res, err := s.Publish(fmt.Sprintf("chg-conc-%d-%d", w, i), []RuleInput{
					{Feature: fmt.Sprintf("f-%d-%d", w, i), Version: 1, Percentage: 50},
				})
				if err != nil {
					t.Errorf("publish failed: %v", err)
					return
				}
				seqs <- res.Seq
			}
		}(w)
	}
	wg.Wait()
	close(seqs)

	seen := map[uint64]bool{}
	count := 0
	for seq := range seqs {
		if seen[seq] {
			t.Fatalf("duplicate seq %d", seq)
		}
		seen[seq] = true
		count++
	}
	if count != workers*perWorker {
		t.Fatalf("expected %d publishes, got %d", workers*perWorker, count)
	}
	if got := s.CurrentSnapshotSeq(); got != uint64(workers*perWorker) {
		t.Fatalf("final snapshot seq = %d, want %d", got, workers*perWorker)
	}

	// 同一变更号并发发布：全部收敛到同一序列号。
	const sameChange = "chg-conc-same"
	results := make(chan uint64, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Publish(sameChange, []RuleInput{{Feature: "shared", Version: 1, Percentage: 10}})
			if err != nil {
				t.Errorf("idempotent concurrent publish failed: %v", err)
				return
			}
			results <- res.Seq
		}()
	}
	wg.Wait()
	close(results)
	var first uint64
	for seq := range results {
		if first == 0 {
			first = seq
		} else if seq != first {
			t.Fatalf("concurrent same-change publishes diverged: %d vs %d", first, seq)
		}
	}
}

// 发布、回滚、在线判定并发进行时：判定沿依赖链读到的必须是同一份快照，
// 不允许出现"主功能已开、依赖已关"这类混合中间态。
func TestConcurrentPublishRollbackEvaluate(t *testing.T) {
	s := NewService()
	mustPublish(t, s, "chg-mix-0", []RuleInput{
		{Feature: "base", Version: 1, Percentage: 100},
		{Feature: "top", Version: 1, Percentage: 100, Deps: []Dependency{{Feature: "base", Version: 1}}},
	})
	seqV1 := s.CurrentSnapshotSeq()

	stop := make(chan struct{})
	var evalWg, writerWg sync.WaitGroup

	// 判定协程：top 放行时 base 必须也放行（同一份快照内）。
	for g := 0; g < 4; g++ {
		evalWg.Add(1)
		go func(id int) {
			defer evalWg.Done()
			user := fmt.Sprintf("user-%d", id)
			for {
				select {
				case <-stop:
					return
				default:
				}
				top := s.Evaluate("top", user)
				base := s.Evaluate("base", user)
				if top.Allowed && !base.Allowed && top.Seq == base.Seq {
					t.Errorf("inconsistent snapshot: top allowed but base denied (seq %d)", top.Seq)
					return
				}
			}
		}(g)
	}

	// 发布协程：交替开关 base。
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for v := 2; v <= 60; v++ {
			pct := (v % 2) * 100
			if _, err := s.Publish(fmt.Sprintf("chg-mix-%d", v), []RuleInput{
				{Feature: "base", Version: v, Percentage: pct},
			}); err != nil {
				t.Errorf("publish failed: %v", err)
				return
			}
		}
	}()

	// 回滚协程：不断回滚到 v1 快照。
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 0; i < 30; i++ {
			// 目标快照可能已被当前指针占用，忽略"已是当前"错误。
			_, _ = s.Rollback(fmt.Sprintf("chg-mix-rb-%d", i), seqV1)
		}
	}()

	writerWg.Wait()
	close(stop)
	evalWg.Wait()

	// 序列号仍然单调。
	h := s.History()
	for i := 1; i < len(h); i++ {
		if h[i].Seq <= h[i-1].Seq {
			t.Fatalf("history seq not monotonic: %+v", h)
		}
	}
}
