package postgres_test

import (
	"context"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

func cleanupAgents(t *testing.T, db *postgres.DB) {
	t.Helper()
	if _, err := db.Pool().Exec(context.Background(), `DELETE FROM agent`); err != nil {
		t.Fatalf("cleanup agents: %v", err)
	}
}

func TestAgentRepoLifecycle(t *testing.T) {
	db := openTestDB(t)
	cleanupAgents(t, db)
	repo := postgres.NewAgentRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	a, err := agent.New(r, "prov-1", agent.TypeClusterProvider, map[string]string{"team": "infra"}, nil, "hash-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, a); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if a.CreatedAt.IsZero() {
		t.Error("timestamps not populated")
	}

	dup, _ := agent.New(r, "prov-1", agent.TypeCustom, nil, nil, "h")
	if err := repo.Create(ctx, dup); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("dup = %v", err)
	}

	got, err := repo.Get(ctx, r.ID, "prov-1")
	if err != nil || got.FRN.String() != "frn:default:agent:prov-1" || got.Type != agent.TypeClusterProvider || got.TokenHash != "hash-1" {
		t.Fatalf("round-trip: %+v (%v)", got, err)
	}

	// provisioning-label schema round-trips + is replaced wholesale via Mutate
	if _, err := repo.Mutate(ctx, r.ID, "prov-1", func(a *agent.Agent) error {
		return a.SetProvisioningLabels([]agent.ProvisioningLabelSpec{
			{Key: "franz.provisioning/deployment-type", AllowedValues: []string{"local-docker"}, DefaultValue: "local-docker", Required: true},
			{Key: "franz.provisioning/kafka-image", Description: "full image ref"},
		})
	}); err != nil {
		t.Fatalf("set provisioning labels: %v", err)
	}
	got, _ = repo.Get(ctx, r.ID, "prov-1")
	if len(got.ProvisioningLabels) != 2 || got.ProvisioningLabels[0].DefaultValue != "local-docker" ||
		!got.ProvisioningLabels[0].Required || got.ProvisioningLabels[1].Key != "franz.provisioning/kafka-image" {
		t.Fatalf("provisioning labels round-trip: %+v", got.ProvisioningLabels)
	}
	if _, err := repo.Mutate(ctx, r.ID, "prov-1", func(a *agent.Agent) error {
		return a.SetProvisioningLabels(nil)
	}); err != nil {
		t.Fatalf("clear provisioning labels: %v", err)
	}
	if got, _ = repo.Get(ctx, r.ID, "prov-1"); len(got.ProvisioningLabels) != 0 {
		t.Errorf("provisioning labels not cleared: %+v", got.ProvisioningLabels)
	}

	// rotate token via Mutate
	if _, err := repo.Mutate(ctx, r.ID, "prov-1", func(a *agent.Agent) error {
		a.RotateToken("hash-2")
		return nil
	}); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	got, _ = repo.Get(ctx, r.ID, "prov-1")
	if got.TokenHash != "hash-2" {
		t.Errorf("token hash after rotate = %q", got.TokenHash)
	}

	// soft delete → hidden from List, still Get-able, name not reusable
	if _, err := repo.Mutate(ctx, r.ID, "prov-1", func(a *agent.Agent) error { return a.Delete() }); err != nil {
		t.Fatal(err)
	}
	page, _ := repo.List(ctx, out.AgentQuery{RealmID: r.ID, Limit: 10})
	if len(page.Agents) != 0 {
		t.Errorf("deleted agent listed: %d", len(page.Agents))
	}
	if del, err := repo.Get(ctx, r.ID, "prov-1"); err != nil || del.Status != agent.StatusDeleted {
		t.Errorf("Get(deleted) = %+v %v", del, err)
	}
	reuse, _ := agent.New(r, "prov-1", agent.TypeCustom, nil, nil, "h")
	if err := repo.Create(ctx, reuse); errs.KindOf(err) != errs.AlreadyExists {
		t.Errorf("recreate deleted name = %v", err)
	}
}

func TestAgentRepoListTypeFilter(t *testing.T) {
	db := openTestDB(t)
	cleanupAgents(t, db)
	repo := postgres.NewAgentRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		typ  agent.Type
	}{
		{"a", agent.TypeClusterProvider},
		{"b", agent.TypeTelemetryAgent},
		{"c", agent.TypeClusterProvider},
	} {
		x, _ := agent.New(r, tc.name, tc.typ, nil, nil, "h")
		if err := repo.Create(ctx, x); err != nil {
			t.Fatal(err)
		}
	}

	cp, err := repo.List(ctx, out.AgentQuery{RealmID: r.ID, TypeFilter: agent.TypeClusterProvider, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.Agents) != 2 {
		t.Fatalf("type filter = %d, want 2", len(cp.Agents))
	}

	// pagination with the filter applied
	p1, _ := repo.List(ctx, out.AgentQuery{RealmID: r.ID, TypeFilter: agent.TypeClusterProvider, Limit: 1})
	if len(p1.Agents) != 1 || p1.LastName != "a" {
		t.Fatalf("page 1 = %v / %q", p1.Agents, p1.LastName)
	}
	p2, _ := repo.List(ctx, out.AgentQuery{RealmID: r.ID, TypeFilter: agent.TypeClusterProvider, Limit: 1, AfterName: p1.LastName})
	if len(p2.Agents) != 1 || p2.Agents[0].Name != "c" || p2.LastName != "" {
		t.Fatalf("page 2 = %v / %q", p2.Agents, p2.LastName)
	}
}

// TestAgentDeleteLeavesClusterProviderStringDangling — 04.5 / 003.3: the
// cluster_provider_agent link is an unvalidated string; deleting the agent does
// not touch the cluster.
func TestAgentDeleteLeavesClusterProviderStringDangling(t *testing.T) {
	db := openTestDB(t)
	cleanupClusters(t, db)
	cleanupAgents(t, db)
	agentRepo := postgres.NewAgentRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)
	r := seededRealm(t, db)
	ctx := context.Background()

	a, _ := agent.New(r, "prov-1", agent.TypeClusterProvider, nil, nil, "h")
	if err := agentRepo.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	c, _ := cluster.New(r, "east-1", plain("b:9092"), nil, nil, "prov-1")
	if err := clusterRepo.Create(ctx, c); err != nil {
		t.Fatal(err)
	}

	if _, err := agentRepo.Mutate(ctx, r.ID, "prov-1", func(a *agent.Agent) error { return a.Delete() }); err != nil {
		t.Fatal(err)
	}

	got, err := clusterRepo.Get(ctx, r.ID, "east-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ProviderAgent != "prov-1" {
		t.Errorf("cluster_provider_agent = %q, want dangling 'prov-1'", got.ProviderAgent)
	}
}
