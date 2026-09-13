package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

func cleanupClients(t *testing.T, db *postgres.DB) {
	t.Helper()
	if _, err := db.Pool().Exec(context.Background(), `DELETE FROM client`); err != nil {
		t.Fatalf("cleanup clients: %v", err)
	}
	if _, err := db.Pool().Exec(context.Background(), `DELETE FROM deleted_client_frn`); err != nil {
		t.Fatalf("cleanup deleted_client_frn: %v", err)
	}
}

func TestClientRepoLifecycle(t *testing.T) {
	db := openTestDB(t)
	cleanupClients(t, db)
	repo := postgres.NewClientRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	c, err := client.New(r, "billing", map[string]string{"org.com/owner": "payments-team"})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if c.CreatedAt.IsZero() {
		t.Error("timestamps not populated")
	}

	got, err := repo.Get(ctx, r.ID, "billing")
	if err != nil || got.FRN.String() != "frn:default:client:billing" ||
		got.Labels["org.com/owner"] != "payments-team" {
		t.Fatalf("round-trip: %+v (%v)", got, err)
	}

	updated, err := repo.Update(ctx, r.ID, "billing", func(c *client.Client) error {
		c.SetLabels(map[string]string{"team": "infra"})
		return nil
	})
	if err != nil || updated.Labels["team"] != "infra" || updated.Labels["org.com/owner"] != "" {
		t.Fatalf("Update: %+v (%v), want a wholesale label replacement", updated, err)
	}
}

// TestClientNameIsRealmWideUniqueAndNeverReused pins 003.10's two hardest
// invariants together: a live duplicate is rejected the same way as trying to
// resurrect a deleted name.
func TestClientNameIsRealmWideUniqueAndNeverReused(t *testing.T) {
	db := openTestDB(t)
	cleanupClients(t, db)
	repo := postgres.NewClientRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	c, err := client.New(r, "billing", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, c); err != nil {
		t.Fatal(err)
	}

	dup, _ := client.New(r, "billing", map[string]string{"team": "other"})
	if err := repo.Create(ctx, dup); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("duplicate create kind = %v", errs.KindOf(err))
	}

	if err := repo.Delete(ctx, r.ID, "billing"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := repo.Get(ctx, r.ID, "billing"); errs.KindOf(err) != errs.NotFound {
		t.Fatalf("Get after delete kind = %v, want NotFound (hard delete)", errs.KindOf(err))
	}

	reborn, _ := client.New(r, "billing", nil)
	if err := repo.Create(ctx, reborn); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("recreate-after-delete kind = %v, want AlreadyExists (003.10 name never freed)",
			errs.KindOf(err))
	}
}

func TestClientDeleteOfAbsentClientIsNotFound(t *testing.T) {
	db := openTestDB(t)
	cleanupClients(t, db)
	repo := postgres.NewClientRepo(db)
	r := seededRealm(t, db)

	if err := repo.Delete(context.Background(), r.ID, "ghost"); errs.KindOf(err) != errs.NotFound {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestClientRepoListAppliesSelectorAndPages(t *testing.T) {
	db := openTestDB(t)
	cleanupClients(t, db)
	repo := postgres.NewClientRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta", "gamma"} {
		c, err := client.New(r, name, map[string]string{"team": "infra"})
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	other, _ := client.New(r, "delta", map[string]string{"team": "payments"})
	if err := repo.Create(ctx, other); err != nil {
		t.Fatal(err)
	}

	sel, err := selector.Parse("team=infra")
	if err != nil {
		t.Fatal(err)
	}
	page, err := repo.List(ctx, out.ClientQuery{RealmID: r.ID, Selector: sel, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Clients) != 2 || page.Clients[0].Name != "alpha" || page.LastName != "beta" {
		t.Fatalf("page 1 = %+v", page.Clients)
	}

	page2, err := repo.List(ctx, out.ClientQuery{RealmID: r.ID, Selector: sel, Limit: 2, AfterName: page.LastName})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Clients) != 1 || page2.Clients[0].Name != "gamma" {
		t.Fatalf("page 2 = %+v", page2.Clients)
	}
}

// TestObservedConsumerGroupViewsScopeByClientFRN exercises 16.5/16.6 end to
// end: a real observed_consumer_group row (deliverable 15's table) is only
// visible through the views scoped to the client it names.
func TestObservedConsumerGroupViewsScopeByClientFRN(t *testing.T) {
	db := openTestDB(t)
	cleanupClients(t, db)
	if _, err := db.Pool().Exec(context.Background(), `DELETE FROM observed_consumer_group`); err != nil {
		t.Fatalf("cleanup observed_consumer_group: %v", err)
	}
	clientRepo := postgres.NewClientRepo(db)
	groupRepo := postgres.NewObservedConsumerGroupRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	c, err := client.New(r, "billing", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := clientRepo.Create(ctx, c); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	obs, err := consumergroup.NewObservation(r.ID, "billing.orders-0", c.FRN.Path(), "",
		"orders", "orders-0", "odradek-prod", now, now)
	if err != nil {
		t.Fatal(err)
	}
	other, err := consumergroup.NewObservation(r.ID, "other-group", "default:client:someone-else",
		"", "orders", "orders-0", "odradek-prod", now, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := groupRepo.Append(ctx, []*consumergroup.Observation{obs, other}); err != nil {
		t.Fatal(err)
	}

	current, err := groupRepo.ListCurrent(ctx, out.ObservedGroupQuery{RealmID: r.ID, ClientFRN: c.FRN.Path()})
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Observations) != 1 || current.Observations[0].Group != "billing.orders-0" {
		t.Fatalf("current view = %+v, want only billing's sighting", current.Observations)
	}

	history, err := groupRepo.ListObservations(ctx, out.ObservationQuery{RealmID: r.ID, ClientFRN: c.FRN.Path()})
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Observations) != 1 || history.Observations[0].Group != "billing.orders-0" {
		t.Fatalf("history view = %+v, want only billing's sighting", history.Observations)
	}
}
