package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
)

func cleanupProviderEvents(t *testing.T, db *postgres.DB) {
	t.Helper()
	if _, err := db.Pool().Exec(context.Background(), `DELETE FROM cluster_provider_event`); err != nil {
		t.Fatalf("cleanup provider events: %v", err)
	}
}

func TestProviderEventRepo(t *testing.T) {
	db := openTestDB(t)
	cleanupProviderEvents(t, db)
	cleanupClusters(t, db)
	clusterRepo := postgres.NewClusterRepo(db)
	repo := postgres.NewProviderEventRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	c, _ := cluster.New(r, "east-1", plain("b:9092"), nil, nil, "prov-1")
	if err := clusterRepo.Create(ctx, c); err != nil {
		t.Fatal(err)
	}

	// no events yet
	if st, err := repo.LatestStatus(ctx, c.ID); err != nil || st != nil {
		t.Fatalf("LatestStatus(empty) = %+v %v, want nil nil", st, err)
	}

	base := time.Now().UTC().Truncate(time.Millisecond)
	for i, ph := range []provider.Phase{provider.PhaseProvisioning, provider.PhaseReady, provider.PhaseDegraded} {
		ev, err := provider.NewEvent(c.ID, r.ID, c.FRN, ph, ph == provider.PhaseReady,
			"msg", "prov-1", "local-docker@abcd", base.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Append(ctx, ev); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// current = newest
	st, err := repo.LatestStatus(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != provider.PhaseDegraded || st.ReportingAgent != "prov-1" || st.RecipeRef != "local-docker@abcd" {
		t.Errorf("LatestStatus = %+v", st)
	}

	// history newest-first, paginated
	p1, err := repo.ListByCluster(ctx, c.ID, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Events) != 2 || p1.Events[0].Phase != provider.PhaseDegraded || p1.After == "" {
		t.Fatalf("page 1 = %d events, first %v, after %q", len(p1.Events), p1.Events[0].Phase, p1.After)
	}
	p2, err := repo.ListByCluster(ctx, c.ID, 2, p1.After)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Events) != 1 || p2.Events[0].Phase != provider.PhaseProvisioning || p2.After != "" {
		t.Fatalf("page 2 = %d events, first %v, after %q", len(p2.Events), p2.Events[0].Phase, p2.After)
	}

	// prune drops everything older than "now + 1h" (all of them)
	n, err := repo.PruneOlderThan(ctx, base.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("pruned %d, want 3", n)
	}
	if st, _ := repo.LatestStatus(ctx, c.ID); st != nil {
		t.Error("status should be gone after prune")
	}
}
