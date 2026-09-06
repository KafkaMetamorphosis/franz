package postgres_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

func plain(url string) []cluster.ConnectionString {
	return []cluster.ConnectionString{{BootstrapURLs: []string{url}, Type: cluster.ConnectionPlaintext}}
}

func seededRealm(t *testing.T, db *postgres.DB) realm.Realm {
	t.Helper()
	r, err := postgres.NewRealmRepo(db).GetBySlug(context.Background(), realm.DefaultSlug)
	if err != nil {
		t.Fatalf("seed realm: %v", err)
	}
	return r
}

// cleanupClusters removes every kafka_cluster row so a re-run starts clean.
func cleanupClusters(t *testing.T, db *postgres.DB) {
	t.Helper()
	if _, err := db.Pool().Exec(context.Background(), `DELETE FROM kafka_cluster`); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

func TestClusterRepoLifecycle(t *testing.T) {
	db := openTestDB(t)
	cleanupClusters(t, db)
	repo := postgres.NewClusterRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	c, err := cluster.New(r, "east-1", plain("b:9092"), map[string]string{"env": "prod"}, map[string]string{"k": "v"}, "agent-x")
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if c.ID.String() == "00000000-0000-0000-0000-000000000000" || c.CreatedAt.IsZero() {
		t.Errorf("Create did not populate id/timestamps: %+v", c)
	}

	// duplicate name → AlreadyExists
	dup, _ := cluster.New(r, "east-1", plain("b:9092"), nil, nil, "")
	if err := repo.Create(ctx, dup); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("duplicate create = %v", err)
	}

	got, err := repo.Get(ctx, r.ID, "east-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.FRN.String() != "frn:default:kafka-cluster:east-1" || got.Labels["env"] != "prod" || got.Configuration["k"] != "v" {
		t.Errorf("round-trip mismatch: %+v", got)
	}

	// soft delete
	if _, err := repo.Mutate(ctx, r.ID, "east-1", func(c *cluster.Cluster) error { return c.Delete() }); err != nil {
		t.Fatalf("delete mutate: %v", err)
	}
	// hidden from List
	page, err := repo.List(ctx, out.ClusterQuery{RealmID: r.ID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Clusters) != 0 {
		t.Errorf("deleted cluster still listed: %d", len(page.Clusters))
	}
	// still returned by Get
	del, err := repo.Get(ctx, r.ID, "east-1")
	if err != nil || del.State != cluster.StateDeleted {
		t.Errorf("Get(deleted) = %+v %v", del, err)
	}
	// name not reusable
	reuse, _ := cluster.New(r, "east-1", plain("b:9092"), nil, nil, "")
	if err := repo.Create(ctx, reuse); errs.KindOf(err) != errs.AlreadyExists {
		t.Errorf("recreate after delete = %v, want AlreadyExists", err)
	}
}

func TestClusterRepoListSelectorAndPagination(t *testing.T) {
	db := openTestDB(t)
	cleanupClusters(t, db)
	repo := postgres.NewClusterRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	for _, tc := range []struct{ name, env string }{
		{"a", "prod"}, {"b", "prod"}, {"c", "staging"}, {"d", "prod"},
	} {
		c, _ := cluster.New(r, tc.name, plain("b:9092"), map[string]string{"env": tc.env}, nil, "")
		if err := repo.Create(ctx, c); err != nil {
			t.Fatal(err)
		}
	}

	sel, _ := selector.Parse("env=prod")
	p1, err := repo.List(ctx, out.ClusterQuery{RealmID: r.ID, Selector: sel, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Clusters) != 2 || p1.LastName != "b" {
		t.Fatalf("page 1 = %v / lastName %q", names(p1.Clusters), p1.LastName)
	}
	p2, _ := repo.List(ctx, out.ClusterQuery{RealmID: r.ID, Selector: sel, Limit: 2, AfterName: p1.LastName})
	if len(p2.Clusters) != 1 || p2.Clusters[0].Name != "d" || p2.LastName != "" {
		t.Fatalf("page 2 = %v / lastName %q", names(p2.Clusters), p2.LastName)
	}
}

func names(cs []*cluster.Cluster) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	return out
}

// TestClusterRepoMutateSerialises checks that two concurrent Mutate calls do not
// lose an update — the second SELECT … FOR UPDATE blocks on the first txn.
func TestClusterRepoMutateSerialises(t *testing.T) {
	db := openTestDB(t)
	cleanupClusters(t, db)
	repo := postgres.NewClusterRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	c, _ := cluster.New(r, "counter", plain("b:9092"), map[string]string{"n": "0"}, nil, "")
	if err := repo.Create(ctx, c); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, err := repo.Mutate(ctx, r.ID, "counter", func(c *cluster.Cluster) error {
				n, _ := strconv.Atoi(c.Labels["n"])
				c.Labels["n"] = strconv.Itoa(n + 1)
				return nil
			})
			if err != nil {
				t.Errorf("mutate: %v", err)
			}
		}()
	}
	wg.Wait()

	final, _ := repo.Get(ctx, r.ID, "counter")
	if final.Labels["n"] != strconv.Itoa(workers) {
		t.Fatalf("counter = %s, want %d (lost updates ⇒ FOR UPDATE not serialising)", final.Labels["n"], workers)
	}
}
