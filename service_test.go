package featurerollout

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// publishOne 便捷函数：单功能草稿直接发布。
func publishOne(t *testing.T, s *Service, changeID ChangeID, spec VersionSpec) *ChangeRecord {
	t.Helper()
	d := s.NewDraft([]VersionSpec{spec})
	if err := s.ValidateDraft(d.ID); err != nil {
		t.Fatalf("ValidateDraft(%s): %v", changeID, err)
	}
	rec, err := s.Publish(changeID, d.ID)
	if err != nil {
		t.Fatalf("Publish(%s): %v", changeID, err)
	}
	return rec
}

func TestPublishAndEvaluateBasic(t *testing.T) {
	s := NewService()
	publishOne(t, s, "c1", VersionSpec{Feature: "search", Version: "v1", Percentage: 100})

	ok, err := s.Evaluate("search", "user-1")
	if err != nil || !ok {
		t.Fatalf("Evaluate = %v, %v; want true, nil", ok, err)
	}
	if _, err := s.Evaluate("missing", "user-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Evaluate(missing) err = %v; want ErrNotFound", err)
	}
	if _, err := s.Evaluate("", "user-1"); !errors.Is(err, ErrInvalidParam) {
		t.Fatalf("Evaluate(\"\") err = %v; want ErrInvalidParam", err)
	}
}

func TestVersionImmutableAndSingleCurrent(t *testing.T) {
	s := NewService()
	publishOne(t, s, "c1", VersionSpec{Feature: "pay", Version: "v1", Percentage: 50})

	// 同一版本号不可再发布（不可修改）。
	d := s.NewDraft([]VersionSpec{{Feature: "pay", Version: "v1", Percentage: 80}})
	if _, err := s.Publish("c2", d.ID); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("republish v1 err = %v; want ErrVersionConflict", err)
	}

	// 新版本发布后成为唯一当前版本。
	publishOne(t, s, "c3", VersionSpec{Feature: "pay", Version: "v2", Percentage: 80})
	cur := s.CurrentVersions()
	if cur["pay"] != "v2" || len(cur) != 1 {
		t.Fatalf("CurrentVersions = %v; want pay->v2 only", cur)
	}
	// 历史版本仍可读取且内容未被修改。
	rv, err := s.GetVersion("pay", "v1")
	if err != nil || rv.Percentage != 50 {
		t.Fatalf("GetVersion(v1) = %+v, %v; want Percentage 50", rv, err)
	}
}

func TestValidationErrors(t *testing.T) {
	s := NewService()

	// 参数错误。
	for _, spec := range []VersionSpec{
		{Feature: "", Version: "v1"},
		{Feature: "f", Version: ""},
		{Feature: "f", Version: "v1", Percentage: 101},
		{Feature: "f", Version: "v1", Percentage: -1},
		{Feature: "f", Version: "v1", DependsOn: []FeatureKey{""}},
	} {
		d := s.NewDraft([]VersionSpec{spec})
		if err := s.ValidateDraft(d.ID); !errors.Is(err, ErrInvalidParam) {
			t.Fatalf("ValidateDraft(%+v) = %v; want ErrInvalidParam", spec, err)
		}
	}

	// 依赖不存在。
	d := s.NewDraft([]VersionSpec{{Feature: "a", Version: "v1", DependsOn: []FeatureKey{"ghost"}}})
	if err := s.ValidateDraft(d.ID); !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("dep on ghost = %v; want ErrDependencyNotFound", err)
	}

	// 同组内依赖可满足：b 与依赖 b 的 a 同组发布成功。
	d = s.NewDraft([]VersionSpec{
		{Feature: "a", Version: "v1", DependsOn: []FeatureKey{"b"}},
		{Feature: "b", Version: "v1"},
	})
	if err := s.ValidateDraft(d.ID); err != nil {
		t.Fatalf("in-group dep validate = %v; want nil", err)
	}
	if _, err := s.Publish("c-deps", d.ID); err != nil {
		t.Fatalf("in-group dep publish = %v; want nil", err)
	}

	// 依赖已发布功能的当前版本：OK。
	d = s.NewDraft([]VersionSpec{{Feature: "c", Version: "v1", DependsOn: []FeatureKey{"a"}}})
	if err := s.ValidateDraft(d.ID); err != nil {
		t.Fatalf("dep on published = %v; want nil", err)
	}

	// 同组重复功能。
	d = s.NewDraft([]VersionSpec{
		{Feature: "dup", Version: "v1"},
		{Feature: "dup", Version: "v2"},
	})
	if err := s.ValidateDraft(d.ID); !errors.Is(err, ErrInvalidParam) {
		t.Fatalf("duplicate feature = %v; want ErrInvalidParam", err)
	}
}

func TestDependencyCycleRejected(t *testing.T) {
	s := NewService()

	// 同组成环：a->b->a。
	d := s.NewDraft([]VersionSpec{
		{Feature: "a", Version: "v1", DependsOn: []FeatureKey{"b"}},
		{Feature: "b", Version: "v1", DependsOn: []FeatureKey{"a"}},
	})
	if err := s.ValidateDraft(d.ID); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("a<->b = %v; want ErrDependencyCycle", err)
	}

	// 自环。
	d = s.NewDraft([]VersionSpec{{Feature: "s", Version: "v1", DependsOn: []FeatureKey{"s"}}})
	if err := s.ValidateDraft(d.ID); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("self loop = %v; want ErrDependencyCycle", err)
	}

	// 与已发布版本合图成环：已发布 x（无依赖），新 y 依赖 x，同时升级 x 依赖 y。
	s2 := NewService()
	publishOne(t, s2, "x1", VersionSpec{Feature: "x", Version: "v1"})
	d = s2.NewDraft([]VersionSpec{
		{Feature: "x", Version: "v2", DependsOn: []FeatureKey{"y"}},
		{Feature: "y", Version: "v1", DependsOn: []FeatureKey{"x"}},
	})
	if err := s2.ValidateDraft(d.ID); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("cross-version cycle = %v; want ErrDependencyCycle", err)
	}
}

func TestAtomicGroupPublishAllOrNothing(t *testing.T) {
	s := NewService()
	publishOne(t, s, "base", VersionSpec{Feature: "keep", Version: "v1", Percentage: 100})

	// 组内一个版本号冲突 -> 整组失败，其它功能不受影响。
	d := s.NewDraft([]VersionSpec{
		{Feature: "keep", Version: "v1"}, // 已存在，冲突
		{Feature: "newf", Version: "v1"},
	})
	if _, err := s.Publish("bad-group", d.ID); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("group publish = %v; want ErrVersionConflict", err)
	}
	if _, err := s.Evaluate("newf", "u"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("newf visible after failed group: %v; want ErrNotFound", err)
	}
	cur := s.CurrentVersions()
	if cur["keep"] != "v1" || len(cur) != 1 {
		t.Fatalf("current after failed group = %v; want only keep->v1", cur)
	}
	if n := len(s.History()); n != 1 {
		t.Fatalf("history len = %d; want 1 (failed publish leaves no record)", n)
	}
}

func TestDeterministicBucketAndMonotonePercentage(t *testing.T) {
	s := NewService()
	publishOne(t, s, "c1", VersionSpec{Feature: "rec", Version: "v1", Percentage: 30})

	users := make([]UserID, 500)
	for i := range users {
		users[i] = UserID(fmt.Sprintf("user-%d", i))
	}
	inV1 := map[UserID]bool{}
	for _, u := range users {
		ok, err := s.Evaluate("rec", u)
		if err != nil {
			t.Fatalf("Evaluate: %v", err)
		}
		inV1[u] = ok
		// 同一用户同一版本重复判定结果稳定。
		for i := 0; i < 3; i++ {
			again, _ := s.Evaluate("rec", u)
			if again != ok {
				t.Fatalf("Evaluate(%s) unstable: %v then %v", u, ok, again)
			}
		}
	}

	// 发布同功能新版本、百分比 30 -> 70：受众只能扩大，原受众不得掉出。
	publishOne(t, s, "c2", VersionSpec{Feature: "rec", Version: "v2", Percentage: 70})
	var n1, n2 int
	for _, u := range users {
		ok, _ := s.Evaluate("rec", u)
		if ok {
			n2++
		}
		if inV1[u] {
			n1++
			if !ok {
				t.Fatalf("user %s was in 30%% bucket of v1 but dropped at 70%% of v2", u)
			}
		}
	}
	if n1 == 0 || n1 >= 200 {
		t.Fatalf("30%% bucket count = %d/500; want roughly 150", n1)
	}
	if n2 <= n1 {
		t.Fatalf("70%% bucket count = %d; want more than 30%%'s %d", n2, n1)
	}

	// 收缩：v3 百分比 10，其受众必须是 v1(30%) 受众的子集。
	publishOne(t, s, "c3", VersionSpec{Feature: "rec", Version: "v3", Percentage: 10})
	for _, u := range users {
		ok, _ := s.Evaluate("rec", u)
		if ok && !inV1[u] {
			t.Fatalf("user %s in 10%% bucket but was not in 30%% bucket", u)
		}
	}
}

func TestBucketMonotoneWithinSameVersioningScheme(t *testing.T) {
	// 直接验证分桶函数的嵌套性：百分比单调 -> 受众只增不减。
	for _, u := range []UserID{"a", "b", "c", "u-100", "u-xyz"} {
		lo := inBucket("f", u, 10)
		hi := inBucket("f", u, 90)
		if lo && !hi {
			t.Fatalf("user %s in 10%% bucket but not 90%%", u)
		}
	}
}

func TestAudienceIncludeExclude(t *testing.T) {
	s := NewService()
	publishOne(t, s, "c1", VersionSpec{
		Feature:    "beta",
		Version:    "v1",
		Percentage: 0, // 桶内无人
		Audience: Audience{
			Include: []UserID{"vip"},
			Exclude: []UserID{"banned"},
		},
	})

	if ok, _ := s.Evaluate("beta", "vip"); !ok {
		t.Fatal("include user should pass even at 0%")
	}
	if ok, _ := s.Evaluate("beta", "random"); ok {
		t.Fatal("0% bucket user should not pass")
	}
	if ok, _ := s.Evaluate("beta", "banned"); ok {
		t.Fatal("exclude user should never pass")
	}

	// 排除优先于包含。
	s2 := NewService()
	publishOne(t, s2, "c1", VersionSpec{
		Feature:    "f",
		Version:    "v1",
		Percentage: 100,
		Audience:   Audience{Include: []UserID{"x"}, Exclude: []UserID{"x"}},
	})
	if ok, _ := s2.Evaluate("f", "x"); ok {
		t.Fatal("exclude must win over include")
	}
}

func TestDependencyEvaluationUsesOneSnapshot(t *testing.T) {
	s := NewService()
	// gate 100% 放行；member 依赖 gate。
	d := s.NewDraft([]VersionSpec{
		{Feature: "gate", Version: "v1", Percentage: 100},
		{Feature: "member", Version: "v1", Percentage: 100, DependsOn: []FeatureKey{"gate"}},
	})
	if _, err := s.Publish("c1", d.ID); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ok, _ := s.Evaluate("member", "u"); !ok {
		t.Fatal("member should pass when gate passes")
	}

	// gate 新版本 0%：member 的依赖判定必须读到同一快照里的 gate v2。
	publishOne(t, s, "c2", VersionSpec{Feature: "gate", Version: "v2", Percentage: 0})
	if ok, _ := s.Evaluate("member", "u"); ok {
		t.Fatal("member must not pass when its dependency gate fails in the same snapshot")
	}
	// member 自身仍是 v1，未受影响。
	if cur := s.CurrentVersions(); cur["member"] != "v1" || cur["gate"] != "v2" {
		t.Fatalf("current = %v; want member v1, gate v2", cur)
	}
}

func TestIdempotentPublishAndConflict(t *testing.T) {
	s := NewService()
	d := s.NewDraft([]VersionSpec{{Feature: "f", Version: "v1", Percentage: 10}})

	r1, err := s.Publish("chg-1", d.ID)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	// 同号同内容：幂等重放，返回同一记录，序号不变。
	r2, err := s.Publish("chg-1", d.ID)
	if err != nil || r2 != r1 {
		t.Fatalf("idempotent replay = %v, %v; want same record, nil", r2, err)
	}
	if n := len(s.History()); n != 1 {
		t.Fatalf("history len = %d; want 1", n)
	}

	// 同号异内容：冲突。
	d2 := s.NewDraft([]VersionSpec{{Feature: "f", Version: "v2", Percentage: 20}})
	if _, err := s.Publish("chg-1", d2.ID); !errors.Is(err, ErrChangeConflict) {
		t.Fatalf("same id different content = %v; want ErrChangeConflict", err)
	}

	// 空变更号。
	if _, err := s.Publish("", d2.ID); !errors.Is(err, ErrInvalidParam) {
		t.Fatalf("empty change id = %v; want ErrInvalidParam", err)
	}
}

func TestRollbackReactivatesHistoricalSnapshot(t *testing.T) {
	s := NewService()
	publishOne(t, s, "c1", VersionSpec{Feature: "f", Version: "v1", Percentage: 100})
	publishOne(t, s, "c2", VersionSpec{Feature: "g", Version: "v1", Percentage: 100})
	publishOne(t, s, "c3", VersionSpec{Feature: "f", Version: "v2", Percentage: 0})

	// 回滚到 c2 之后的快照：f=v1, g=v1。
	rec, err := s.Rollback("rb-1", "c2")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rec.Kind != ChangeRollback || rec.RollbackOf != "c2" {
		t.Fatalf("record = %+v; want rollback of c2", rec)
	}
	cur := s.CurrentVersions()
	if cur["f"] != "v1" || cur["g"] != "v1" || len(cur) != 2 {
		t.Fatalf("current after rollback = %v; want f v1 + g v1", cur)
	}
	if ok, _ := s.Evaluate("f", "any-user"); !ok {
		t.Fatal("f v1 (100%) should pass after rollback")
	}

	// 历史未被修改：c3 记录还在，f v2 版本内容还在。
	if n := len(s.History()); n != 4 {
		t.Fatalf("history len = %d; want 4", n)
	}
	if rv, err := s.GetVersion("f", "v2"); err != nil || rv.Percentage != 0 {
		t.Fatalf("f v2 = %+v, %v; history must be untouched", rv, err)
	}

	// 回滚幂等：同号同目标重放。
	rec2, err := s.Rollback("rb-1", "c2")
	if err != nil || rec2 != rec {
		t.Fatalf("rollback replay = %v, %v; want same record", rec2, err)
	}
	// 同号异目标：冲突。
	if _, err := s.Rollback("rb-1", "c1"); !errors.Is(err, ErrChangeConflict) {
		t.Fatalf("rollback conflict = %v; want ErrChangeConflict", err)
	}
	// 回滚到不存在的记录。
	if _, err := s.Rollback("rb-2", "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rollback missing target = %v; want ErrNotFound", err)
	}
}

func TestConcurrentPublishEvaluateMonotoneSeq(t *testing.T) {
	s := NewService()
	publishOne(t, s, "c0", VersionSpec{Feature: "f", Version: "v0", Percentage: 100})

	const publishers = 8
	const evaluators = 8
	var wg sync.WaitGroup
	errs := make(chan error, publishers*2)

	// 并发发布：每个发布者用各自的变更号与版本号。
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			d := s.NewDraft([]VersionSpec{{
				Feature: "f", Version: VersionID(fmt.Sprintf("v%d", p+1)), Percentage: 100,
			}})
			if _, err := s.Publish(ChangeID(fmt.Sprintf("chg-%d", p)), d.ID); err != nil {
				errs <- fmt.Errorf("publish %d: %w", p, err)
			}
		}(p)
	}
	// 并发判定：不允许报错、不允许看到中间态（f 要么不存在——此处必然存在——要么完整可读）。
	stop := make(chan struct{})
	for e := 0; e < evaluators; e++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ok, err := s.Evaluate("f", "user-x")
				if err != nil {
					errs <- fmt.Errorf("evaluate: %w", err)
					return
				}
				if !ok {
					errs <- errors.New("evaluate: f at 100% must pass in every snapshot")
					return
				}
			}
		}()
	}

	// 等发布全部完成后停止判定。
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if len(s.History()) == publishers+1 {
				close(stop)
				return
			}
		}
	}()
	wg.Wait()
	close(done)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// 发布序号严格单调且连续。
	hist := s.History()
	for i, rec := range hist {
		if rec.Seq != int64(i+1) {
			t.Fatalf("history[%d].Seq = %d; want %d", i, rec.Seq, i+1)
		}
	}
	if len(hist) != publishers+1 {
		t.Fatalf("history len = %d; want %d", len(hist), publishers+1)
	}
}

func TestDraftUpdateAndValidateBeforePublish(t *testing.T) {
	s := NewService()
	d := s.NewDraft([]VersionSpec{{Feature: "f", Version: "v1", DependsOn: []FeatureKey{"nope"}}})
	if err := s.ValidateDraft(d.ID); !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("validate = %v; want ErrDependencyNotFound", err)
	}
	// 修改草稿后重新校验通过并发布。
	if err := s.UpdateDraft(d.ID, []VersionSpec{{Feature: "f", Version: "v1", Percentage: 100}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := s.ValidateDraft(d.ID); err != nil {
		t.Fatalf("validate after update = %v; want nil", err)
	}
	if _, err := s.Publish("c1", d.ID); err != nil {
		t.Fatalf("publish after update: %v", err)
	}
	if err := s.UpdateDraft("ghost", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update ghost = %v; want ErrNotFound", err)
	}
}
