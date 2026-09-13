package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// cleanupObservations empties the consumer-group series so a re-run starts
// clean.
func cleanupObservations(t *testing.T, db *postgres.DB) {
	t.Helper()
	if _, err := db.Pool().Exec(context.Background(),
		`DELETE FROM observed_consumer_group`); err != nil {
		t.Fatalf("cleanup observed_consumer_group: %v", err)
	}
}

// --- indicator_sample foreign key (15.1) ----------------------------------

// 003.14's pre-registration rule is enforced in the store as well as the
// service: a sample naming an indicator nobody registered cannot be written at
// all, whatever path attempts it.
func TestIndicatorSampleRequiresRegisteredIndicator(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	samples := postgres.NewIndicatorSampleRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()
	now := time.Now().UTC()

	orphan, err := indicator.NewSample(r.ID, "never-registered",
		"default:kafka-cluster:east-1", indicator.EntityKafkaCluster, "3",
		"gregor-samsa", now, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := samples.Append(ctx, []*indicator.Sample{orphan}); err == nil {
		t.Fatal("appending a sample for an unregistered indicator must fail")
	}

	// Registering it opens the path — the "Done when" check of deliverable 15.
	registered, err := indicator.NewIndicator(r, "never-registered",
		indicator.UnitCount, indicator.EntityKafkaCluster, "1h", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewIndicatorRepo(db).Create(ctx, registered); err != nil {
		t.Fatal(err)
	}
	accepted, err := samples.Append(ctx, []*indicator.Sample{orphan})
	if err != nil {
		t.Fatalf("Append after registration: %v", err)
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1", accepted)
	}
}

// The values are encoded per the indicator's unit, so the history is
// uninterpretable once the registration is gone: deleting the indicator takes
// its series with it rather than leaving rows nothing can read.
func TestDeleteIndicatorCascadesToItsSamples(t *testing.T) {
	db := openTestDB(t)
	cleanupGovernance(t, db)
	indicators := postgres.NewIndicatorRepo(db)
	samples := postgres.NewIndicatorSampleRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()
	now := time.Now().UTC()

	registered, err := indicator.NewIndicator(r, "disk-used", indicator.UnitBytes,
		indicator.EntityKafkaCluster, "1h", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := indicators.Create(ctx, registered); err != nil {
		t.Fatal(err)
	}
	s, err := indicator.NewSample(r.ID, "disk-used", "default:kafka-cluster:east-1",
		indicator.EntityKafkaCluster, "150Gi", "gregor-samsa", now, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := samples.Append(ctx, []*indicator.Sample{s}); err != nil {
		t.Fatal(err)
	}

	if err := indicators.Delete(ctx, r.ID, "disk-used"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	page, err := samples.List(ctx, out.SampleQuery{RealmID: r.ID, Indicator: "disk-used", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Samples) != 0 {
		t.Fatalf("%d samples survived the registration", len(page.Samples))
	}
}

// --- observed_consumer_group (15.2) ---------------------------------------

// observe builds one sighting the way ReportConsumerGroups would.
func observe(t *testing.T, r realm.Realm, group, clientFRN, topic string, at time.Time) *consumergroup.Observation {
	t.Helper()
	o, err := consumergroup.NewObservation(r.ID, group, clientFRN, "", "orders",
		topic, "odradek-prod", at, at)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// The current view is one row per (group, topic) carrying the newest sighting;
// the history behind it is every raw row (003.14).
func TestObservedConsumerGroupCurrentAndHistory(t *testing.T) {
	db := openTestDB(t)
	cleanupObservations(t, db)
	repo := postgres.NewObservedConsumerGroupRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	billing := "default:client:billing"
	other := "default:client:analytics"

	if _, err := repo.Append(ctx, []*consumergroup.Observation{
		observe(t, r, "billing.orders-0", billing, "orders-0", base),
		observe(t, r, "billing.orders-0", billing, "orders-0", base.Add(10*time.Minute)),
		observe(t, r, "billing.orders-1", billing, "orders-1", base.Add(5*time.Minute)),
		observe(t, r, "legacy-batch-reader", billing, "orders-0", base.Add(6*time.Minute)),
		observe(t, r, "analytics.orders-0", other, "orders-0", base.Add(7*time.Minute)),
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	current, err := repo.ListCurrent(ctx, out.ObservedGroupQuery{RealmID: r.ID, Limit: 10})
	if err != nil {
		t.Fatalf("ListCurrent: %v", err)
	}
	if len(current.Observations) != 4 {
		t.Fatalf("current view = %d rows, want one per (group, topic)", len(current.Observations))
	}
	// Ordered by group then topic, so the page is stable under concurrent ingest.
	wantGroups := []string{"analytics.orders-0", "billing.orders-0", "billing.orders-1", "legacy-batch-reader"}
	for i, want := range wantGroups {
		if got := current.Observations[i].Group; got != want {
			t.Errorf("current[%d] = %q, want %q", i, got, want)
		}
	}
	// The (group, topic) it saw twice collapses to the newer sighting.
	if got := current.Observations[1].ObservedAt; !got.Equal(base.Add(10 * time.Minute)) {
		t.Errorf("billing.orders-0 last seen %v, want the newer sighting", got)
	}
	// `custom` is derived on write, not reported by the agent.
	if current.Observations[1].Custom {
		t.Error("billing.orders-0 follows <client>.<topic> and must not be custom")
	}
	if !current.Observations[3].Custom {
		t.Error("legacy-batch-reader must be custom")
	}

	// One client's view is only that client's groups.
	scoped, err := repo.ListCurrent(ctx, out.ObservedGroupQuery{
		RealmID: r.ID, ClientFRN: other, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped.Observations) != 1 || scoped.Observations[0].Group != "analytics.orders-0" {
		t.Fatalf("client-scoped current view = %+v", scoped.Observations)
	}

	// History is the raw series, newest first, windowable.
	history, err := repo.ListObservations(ctx, out.ObservationQuery{RealmID: r.ID, Limit: 10})
	if err != nil {
		t.Fatalf("ListObservations: %v", err)
	}
	if len(history.Observations) != 5 {
		t.Fatalf("history = %d rows, want every sighting", len(history.Observations))
	}
	if !history.Observations[0].ObservedAt.Equal(base.Add(10 * time.Minute)) {
		t.Errorf("history is not newest-first: %v", history.Observations[0].ObservedAt)
	}

	windowed, err := repo.ListObservations(ctx, out.ObservationQuery{
		RealmID: r.ID, From: base.Add(5 * time.Minute), To: base.Add(7 * time.Minute), Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(windowed.Observations) != 3 {
		t.Fatalf("time window = %d rows, want 3", len(windowed.Observations))
	}

	scopedHistory, err := repo.ListObservations(ctx, out.ObservationQuery{
		RealmID: r.ID, ClientFRN: other, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(scopedHistory.Observations) != 1 {
		t.Fatalf("client-scoped history = %d rows, want 1", len(scopedHistory.Observations))
	}
}

// Both reads page with a cursor, and each walks its result set exactly once.
func TestObservedConsumerGroupPaginates(t *testing.T) {
	db := openTestDB(t)
	cleanupObservations(t, db)
	repo := postgres.NewObservedConsumerGroupRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	var rows []*consumergroup.Observation
	for i := range 5 {
		topic := "orders-" + string(rune('0'+i))
		rows = append(rows, observe(t, r, "billing."+topic, "default:client:billing",
			topic, base.Add(time.Duration(i)*time.Minute)))
	}
	if _, err := repo.Append(ctx, rows); err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	cursor := ""
	for range 10 {
		page, err := repo.ListCurrent(ctx, out.ObservedGroupQuery{
			RealmID: r.ID, Limit: 2, AfterCursor: cursor,
		})
		if err != nil {
			t.Fatalf("ListCurrent(%q): %v", cursor, err)
		}
		for _, o := range page.Observations {
			if seen[o.Group] {
				t.Fatalf("group %q returned twice", o.Group)
			}
			seen[o.Group] = true
		}
		if page.LastCursor == "" {
			break
		}
		cursor = page.LastCursor
	}
	if len(seen) != 5 {
		t.Fatalf("current view paging saw %d of 5 groups", len(seen))
	}

	seenHistory := 0
	cursor = ""
	for range 10 {
		page, err := repo.ListObservations(ctx, out.ObservationQuery{
			RealmID: r.ID, Limit: 2, AfterCursor: cursor,
		})
		if err != nil {
			t.Fatalf("ListObservations(%q): %v", cursor, err)
		}
		seenHistory += len(page.Observations)
		if page.LastCursor == "" {
			break
		}
		cursor = page.LastCursor
	}
	if seenHistory != 5 {
		t.Fatalf("history paging saw %d of 5 sightings", seenHistory)
	}

	if _, err := repo.ListCurrent(ctx, out.ObservedGroupQuery{
		RealmID: r.ID, AfterCursor: "not-a-cursor!!",
	}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("malformed cursor kind = %v", errs.KindOf(err))
	}
}

// The nightly prune keeps the last 30 days and nothing older (003.14).
func TestObservedConsumerGroupPrune(t *testing.T) {
	db := openTestDB(t)
	cleanupObservations(t, db)
	repo := postgres.NewObservedConsumerGroupRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	now := time.Now().UTC()
	if _, err := repo.Append(ctx, []*consumergroup.Observation{
		observe(t, r, "billing.orders-0", "default:client:billing", "orders-0",
			now.Add(-40*24*time.Hour)),
		observe(t, r, "billing.orders-1", "default:client:billing", "orders-1",
			now.Add(-time.Hour)),
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := repo.PruneOlderThan(ctx, now.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatalf("PruneOlderThan: %v", err)
	}
	if removed != 1 {
		t.Fatalf("pruned %d rows, want 1", removed)
	}
	remaining, err := repo.ListObservations(ctx, out.ObservationQuery{RealmID: r.ID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining.Observations) != 1 || remaining.Observations[0].Group != "billing.orders-1" {
		t.Fatalf("after the prune: %+v", remaining.Observations)
	}
}
