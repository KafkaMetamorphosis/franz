package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// cleanupGovernance removes every governance row so a re-run starts clean.
func cleanupGovernance(t *testing.T, db *postgres.DB) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM policy_action`,
		`DELETE FROM policy`,
		`DELETE FROM indicator_sample`,
		`DELETE FROM indicator`,
	} {
		if _, err := db.Pool().Exec(context.Background(), stmt); err != nil {
			t.Fatalf("cleanup (%s): %v", stmt, err)
		}
	}
}

func TestIndicatorRepoLifecycle(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	repo := postgres.NewIndicatorRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	i, err := indicator.NewIndicator(r, "disk-used", indicator.UnitBytes,
		indicator.EntityKafkaCluster, "90d", []string{"gregor-samsa"})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, i); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if i.ID == uuid.Nil || i.CreatedAt.IsZero() {
		t.Fatalf("Create did not populate the surrogate key / timestamps: %+v", i)
	}

	got, err := repo.Get(ctx, r.ID, "disk-used")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Unit != indicator.UnitBytes || got.AppliesTo != indicator.EntityKafkaCluster {
		t.Errorf("round-trip = %+v", got)
	}
	// The operator's verbatim text survives, and the parsed duration is derived
	// from it on read rather than stored twice.
	if got.StalenessSpec != "90d" || got.StalenessThreshold != 90*24*time.Hour {
		t.Errorf("staleness = (%q, %v)", got.StalenessSpec, got.StalenessThreshold)
	}
	if len(got.SourceAgents) != 1 || got.SourceAgents[0] != "gregor-samsa" {
		t.Errorf("SourceAgents = %v", got.SourceAgents)
	}
	if got.LastSampleAt != nil {
		t.Error("a fresh indicator has no last_sample_at")
	}

	// A duplicate name is AlreadyExists, not a raw constraint error.
	dup, _ := indicator.NewIndicator(r, "disk-used", indicator.UnitCount,
		indicator.EntityKafkaCluster, "1h", nil)
	if err := repo.Create(ctx, dup); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("duplicate kind = %v (err %v)", errs.KindOf(err), err)
	}

	if _, err := repo.Get(ctx, r.ID, "ghost"); errs.KindOf(err) != errs.NotFound {
		t.Fatalf("Get(missing) kind = %v", errs.KindOf(err))
	}
}

// TestIndicatorRepoAppliesToIsImmutable is the persistence half of 003.14: the
// UPDATE statement omits the column, so a mutate that changes applies_to has no
// effect on disk.
func TestIndicatorRepoAppliesToIsImmutable(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	repo := postgres.NewIndicatorRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	i, err := indicator.NewIndicator(r, "lag", indicator.UnitCount,
		indicator.EntityAsyncChannel, "1h", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, i); err != nil {
		t.Fatal(err)
	}

	updated, err := repo.Mutate(ctx, r.ID, "lag", func(i *indicator.Indicator) error {
		// A caller that reaches past the setters still cannot move applies_to.
		i.AppliesTo = indicator.EntityKafkaCluster
		return i.SetStalenessThreshold("6h")
	})
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	if updated.AppliesTo != indicator.EntityAsyncChannel {
		t.Fatalf("AppliesTo = %v, want it unchanged (003.14)", updated.AppliesTo)
	}
	if updated.StalenessSpec != "6h" {
		t.Fatalf("StalenessSpec = %q, want the mutable field to have moved", updated.StalenessSpec)
	}

	reread, err := repo.Get(ctx, r.ID, "lag")
	if err != nil {
		t.Fatal(err)
	}
	if reread.AppliesTo != indicator.EntityAsyncChannel {
		t.Fatalf("on disk, AppliesTo = %v", reread.AppliesTo)
	}
}

// TestIndicatorRepoRecordSampleOnlyAdvances: an out-of-order sample is stored as
// history but never becomes "current" (003.14).
func TestIndicatorRepoRecordSampleOnlyAdvances(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	repo := postgres.NewIndicatorRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	i, err := indicator.NewIndicator(r, "lag", indicator.UnitCount,
		indicator.EntityAsyncChannel, "1h", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, i); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	frn := "default:async-channel:orders"

	advanced, err := repo.RecordSample(ctx, r.ID, "lag", frn, "100", now)
	if err != nil {
		t.Fatalf("RecordSample: %v", err)
	}
	if !advanced {
		t.Fatal("the first sample must advance the projection")
	}

	// An older sample must not overwrite the current value.
	advanced, err = repo.RecordSample(ctx, r.ID, "lag", frn, "1", now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("RecordSample(older): %v", err)
	}
	if advanced {
		t.Fatal("an out-of-order sample must not advance the projection")
	}

	// Nor must an identical timestamp — the comparison is strictly newer.
	if advanced, _ = repo.RecordSample(ctx, r.ID, "lag", frn, "2", now); advanced {
		t.Fatal("a same-instant sample must not advance the projection")
	}

	got, err := repo.Get(ctx, r.ID, "lag")
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentValue != "100" || got.CurrentResourceFRN != frn {
		t.Fatalf("current = (%q, %q), want the newest sample", got.CurrentValue, got.CurrentResourceFRN)
	}
	if got.LastSampleAt == nil || !got.LastSampleAt.Equal(now) {
		t.Fatalf("LastSampleAt = %v, want %v", got.LastSampleAt, now)
	}
	// health is derived, never stored, so it reads off the projection.
	if got.Health(now.Add(30*time.Minute)) != indicator.HealthHealthy {
		t.Error("inside the threshold the indicator is healthy")
	}
	if got.Health(now.Add(2*time.Hour)) != indicator.HealthStale {
		t.Error("past the threshold the indicator is stale")
	}
}

func TestIndicatorRepoListPaginates(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	repo := postgres.NewIndicatorRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	for _, name := range []string{"alpha", "bravo", "charlie"} {
		i, err := indicator.NewIndicator(r, name, indicator.UnitCount,
			indicator.EntityAsyncChannel, "1h", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, i); err != nil {
			t.Fatal(err)
		}
	}

	first, err := repo.List(ctx, out.IndicatorQuery{RealmID: r.ID, Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(first.Indicators) != 2 || first.Indicators[0].Name != "alpha" || first.LastName != "bravo" {
		t.Fatalf("first page = %+v (last %q)", first.Indicators, first.LastName)
	}

	second, err := repo.List(ctx, out.IndicatorQuery{RealmID: r.ID, Limit: 2, AfterName: first.LastName})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Indicators) != 1 || second.Indicators[0].Name != "charlie" {
		t.Fatalf("second page = %+v", second.Indicators)
	}
	if second.LastName != "" {
		t.Fatal("the last page must not carry a cursor")
	}
}

// --- Policy ---------------------------------------------------------------

func testPolicy(t *testing.T, r realm.Realm, name string, weight int32, enabled bool) *gov.Policy {
	t.Helper()
	def := gov.Definition{
		Indicator: "lag",
		Matcher:   gov.Matcher{Entity: indicator.EntityAsyncChannel, Selector: "tier=gold"},
		Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "100"},
		Actions: []gov.Action{
			{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}},
			{Kind: gov.ActionAddLabel, Args: []string{"paused-by", "governance"}},
		},
	}
	p, err := gov.New(r, name, def, weight, enabled)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPolicyRepoLifecycle(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	repo := postgres.NewPolicyRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	p := testPolicy(t, r, "pause-hot", 10, true)
	if err := repo.Create(ctx, p); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if p.ID == uuid.Nil || p.CreatedAt.IsZero() {
		t.Fatalf("Create did not populate the row: %+v", p)
	}

	got, err := repo.Get(ctx, r.ID, "pause-hot")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Indicator != "lag" || got.Weight != 10 || !got.Enabled {
		t.Errorf("round-trip = %+v", got)
	}
	if got.Matcher.Entity != indicator.EntityAsyncChannel || got.Matcher.Selector != "tier=gold" {
		t.Errorf("matcher = %+v", got.Matcher)
	}
	if got.Limit.Operator != gov.OpGreaterThan || got.Limit.Value != "100" {
		t.Errorf("limit = %+v", got.Limit)
	}
	// Actions round-trip in order, args intact — order is what 003.8 applies.
	if len(got.Actions) != 2 {
		t.Fatalf("actions = %+v", got.Actions)
	}
	if got.Actions[0].Kind != gov.ActionSetStatus || got.Actions[0].Args[0] != "PAUSED" {
		t.Errorf("actions[0] = %+v", got.Actions[0])
	}
	if got.Actions[1].Kind != gov.ActionAddLabel || len(got.Actions[1].Args) != 2 {
		t.Errorf("actions[1] = %+v", got.Actions[1])
	}
	if got.LastFiredAt != nil {
		t.Error("a fresh policy has not fired")
	}

	dup := testPolicy(t, r, "pause-hot", 0, true)
	if err := repo.Create(ctx, dup); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("duplicate kind = %v (err %v)", errs.KindOf(err), err)
	}

	// Mutate persists a changed definition under the row lock.
	updated, err := repo.Mutate(ctx, r.ID, "pause-hot", func(p *gov.Policy) error {
		p.Weight = 3
		p.Enabled = false
		p.Limit.Value = "500"
		p.Actions = p.Actions[:1]
		return nil
	})
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	if updated.Weight != 3 || updated.Enabled || updated.Limit.Value != "500" || len(updated.Actions) != 1 {
		t.Fatalf("mutated = %+v", updated)
	}

	// MarkFired stamps last_fired_at without disturbing updated_at, which tracks
	// operator edits rather than firings.
	before := updated.UpdatedAt
	firedAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := repo.MarkFired(ctx, r.ID, "pause-hot", firedAt); err != nil {
		t.Fatalf("MarkFired: %v", err)
	}
	reread, err := repo.Get(ctx, r.ID, "pause-hot")
	if err != nil {
		t.Fatal(err)
	}
	if reread.LastFiredAt == nil || !reread.LastFiredAt.Equal(firedAt) {
		t.Fatalf("LastFiredAt = %v, want %v", reread.LastFiredAt, firedAt)
	}
	if !reread.UpdatedAt.Equal(before) {
		t.Errorf("MarkFired moved updated_at from %v to %v", before, reread.UpdatedAt)
	}

	if err := repo.Delete(ctx, r.ID, "pause-hot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if errs.KindOf(repo.Delete(ctx, r.ID, "pause-hot")) != errs.NotFound {
		t.Fatal("deleting twice must be NotFound")
	}
}

// TestPolicyRepoIndicatorQueries covers the two reads governance depends on:
// the evaluation work list and the DeleteIndicator guard.
func TestPolicyRepoIndicatorQueries(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	repo := postgres.NewPolicyRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	enabled := testPolicy(t, r, "zulu", 1, true)
	alsoEnabled := testPolicy(t, r, "alpha", 1, true)
	disabled := testPolicy(t, r, "dormant", 1, false)
	other := testPolicy(t, r, "elsewhere", 1, true)
	other.Indicator = "disk-used"

	for _, p := range []*gov.Policy{enabled, alsoEnabled, disabled, other} {
		if err := repo.Create(ctx, p); err != nil {
			t.Fatal(err)
		}
	}

	work, err := repo.ListEnabledByIndicator(ctx, r.ID, "lag")
	if err != nil {
		t.Fatalf("ListEnabledByIndicator: %v", err)
	}
	if len(work) != 2 {
		t.Fatalf("work list = %d rows, want 2 (disabled and other-indicator excluded)", len(work))
	}
	if work[0].Name != "alpha" || work[1].Name != "zulu" {
		t.Fatalf("work list order = %q, %q — want name ascending", work[0].Name, work[1].Name)
	}

	// The DeleteIndicator guard counts disabled policies too: a disabled rule is
	// still a rule someone may re-enable.
	n, err := repo.CountByIndicator(ctx, r.ID, "lag")
	if err != nil {
		t.Fatalf("CountByIndicator: %v", err)
	}
	if n != 3 {
		t.Fatalf("CountByIndicator = %d, want 3", n)
	}
	if n, _ = repo.CountByIndicator(ctx, r.ID, "never-used"); n != 0 {
		t.Fatalf("CountByIndicator(unused) = %d", n)
	}
}

// --- PolicyAction ---------------------------------------------------------

func TestPolicyActionRepoAppendListPrune(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	policies := postgres.NewPolicyRepo(db)
	repo := postgres.NewPolicyActionRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	p := testPolicy(t, r, "pause-hot", 0, true)
	if err := policies.Create(ctx, p); err != nil {
		t.Fatal(err)
	}

	base := time.Now().UTC().Truncate(time.Microsecond)
	records := make([]*gov.ActionRecord, 0, 5)
	for i := range 5 {
		records = append(records, gov.NewActionRecord(r.ID, p.ID, p.Name,
			"default:async-channel:orders", "150",
			gov.Action{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}},
			"state=PAUSED", base.Add(time.Duration(i)*time.Minute)))
	}
	if err := repo.Append(ctx, records); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := repo.Append(ctx, nil); err != nil {
		t.Fatalf("Append(empty) must be a no-op: %v", err)
	}

	// Newest first, paged by the (occurred_at, id) cursor.
	first, err := repo.List(ctx, out.PolicyActionQuery{RealmID: r.ID, PolicyName: p.Name, Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(first.Actions) != 2 {
		t.Fatalf("first page = %d rows, want 2", len(first.Actions))
	}
	if !first.Actions[0].OccurredAt.Equal(base.Add(4 * time.Minute)) {
		t.Fatalf("first row occurred_at = %v, want the newest", first.Actions[0].OccurredAt)
	}
	if first.LastCursor == "" {
		t.Fatal("a full page must carry a cursor")
	}
	if got := first.Actions[0]; got.Action.Kind != gov.ActionSetStatus ||
		got.Result != "state=PAUSED" || got.IndicatorValue != "150" {
		t.Fatalf("row round-trip = %+v", got)
	}

	seen := 2
	cursor := first.LastCursor
	for cursor != "" {
		page, err := repo.List(ctx, out.PolicyActionQuery{
			RealmID: r.ID, PolicyName: p.Name, Limit: 2, AfterCursor: cursor,
		})
		if err != nil {
			t.Fatalf("List(page): %v", err)
		}
		seen += len(page.Actions)
		cursor = page.LastCursor
	}
	if seen != 5 {
		t.Fatalf("paged through %d rows, want 5", seen)
	}

	// The audit rows outlive the policy: they carry no foreign key, so deleting
	// the policy leaves the history readable by name (003.8).
	if err := policies.Delete(ctx, r.ID, p.Name); err != nil {
		t.Fatal(err)
	}
	after, err := repo.List(ctx, out.PolicyActionQuery{RealmID: r.ID, PolicyName: p.Name, Limit: 10})
	if err != nil {
		t.Fatalf("List after policy delete: %v", err)
	}
	if len(after.Actions) != 5 {
		t.Fatalf("after deleting the policy, %d rows survive; want all 5", len(after.Actions))
	}

	// The nightly prune drops rows older than the cutoff and keeps the rest.
	removed, err := repo.PruneOlderThan(ctx, base.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("PruneOlderThan: %v", err)
	}
	if removed != 3 {
		t.Fatalf("pruned %d rows, want 3", removed)
	}
	remaining, err := repo.List(ctx, out.PolicyActionQuery{RealmID: r.ID, PolicyName: p.Name, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining.Actions) != 2 {
		t.Fatalf("after the prune, %d rows remain; want 2", len(remaining.Actions))
	}
}

// --- IndicatorSampleRepo reads (added by this deliverable) ----------------

func TestIndicatorSampleRepoListAndLatestPerResource(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	repo := postgres.NewIndicatorSampleRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	orders := "default:async-channel:orders"
	invoices := "default:async-channel:invoices"

	var samples []*indicator.Sample
	for i, spec := range []struct {
		frn   string
		value string
		at    time.Time
	}{
		{orders, "10", base},
		{orders, "20", base.Add(10 * time.Minute)},
		{orders, "30", base.Add(20 * time.Minute)},
		{invoices, "5", base.Add(5 * time.Minute)},
		{invoices, "7", base.Add(15 * time.Minute)},
	} {
		s, err := indicator.NewSample(r.ID, "lag", spec.frn, indicator.EntityAsyncChannel,
			spec.value, "agent-a", spec.at, spec.at)
		if err != nil {
			t.Fatalf("sample %d: %v", i, err)
		}
		samples = append(samples, s)
	}
	if _, err := repo.Append(ctx, samples); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// LatestPerResource: one row per resource, the newest, ordered by FRN so a
	// dry run is deterministic.
	latest, err := repo.LatestPerResource(ctx, r.ID, "lag", 0)
	if err != nil {
		t.Fatalf("LatestPerResource: %v", err)
	}
	if len(latest) != 2 {
		t.Fatalf("latest = %d rows, want one per resource", len(latest))
	}
	if latest[0].ResourceFRN != invoices || latest[0].Value != "7" {
		t.Errorf("latest[0] = (%s, %s), want the newest invoices sample", latest[0].ResourceFRN, latest[0].Value)
	}
	if latest[1].ResourceFRN != orders || latest[1].Value != "30" {
		t.Errorf("latest[1] = (%s, %s), want the newest orders sample", latest[1].ResourceFRN, latest[1].Value)
	}

	// List: newest first, filterable by resource and time window.
	all, err := repo.List(ctx, out.SampleQuery{RealmID: r.ID, Indicator: "lag", Limit: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all.Samples) != 5 || all.Samples[0].Value != "30" {
		t.Fatalf("List = %d rows, first %+v", len(all.Samples), all.Samples[0])
	}

	byResource, err := repo.List(ctx, out.SampleQuery{
		RealmID: r.ID, Indicator: "lag", ResourceFRN: orders, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byResource.Samples) != 3 {
		t.Fatalf("resource filter = %d rows, want 3", len(byResource.Samples))
	}

	windowed, err := repo.List(ctx, out.SampleQuery{
		RealmID: r.ID, Indicator: "lag",
		From: base.Add(10 * time.Minute), To: base.Add(15 * time.Minute), Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(windowed.Samples) != 2 {
		t.Fatalf("time window = %d rows, want 2", len(windowed.Samples))
	}

	// Cursor paging walks the whole series exactly once.
	seen := 0
	cursor := ""
	for {
		page, err := repo.List(ctx, out.SampleQuery{
			RealmID: r.ID, Indicator: "lag", Limit: 2, AfterCursor: cursor,
		})
		if err != nil {
			t.Fatalf("List(page): %v", err)
		}
		seen += len(page.Samples)
		if page.LastCursor == "" {
			break
		}
		cursor = page.LastCursor
	}
	if seen != 5 {
		t.Fatalf("paged through %d rows, want 5", seen)
	}

	if _, err := repo.List(ctx, out.SampleQuery{
		RealmID: r.ID, Indicator: "lag", AfterCursor: "not-a-cursor",
	}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatal("a malformed cursor must be INVALID_ARGUMENT")
	}
}
