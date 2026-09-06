package provider

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	prov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// --- fakes -------------------------------------------------------------

type fakeClusters struct {
	byName map[string]*cluster.Cluster
}

func (f *fakeClusters) Create(context.Context, *cluster.Cluster) error { panic("unused") }
func (f *fakeClusters) List(context.Context, out.ClusterQuery) (out.ClusterPage, error) {
	panic("unused")
}
func (f *fakeClusters) Mutate(context.Context, uuid.UUID, string, func(*cluster.Cluster) error) (*cluster.Cluster, error) {
	panic("unused")
}
func (f *fakeClusters) Get(_ context.Context, _ uuid.UUID, name string) (*cluster.Cluster, error) {
	c, ok := f.byName[name]
	if !ok {
		return nil, errs.NotFoundf("kafka cluster %q not found", name)
	}
	return c, nil
}
func (f *fakeClusters) ListByProviderAgent(_ context.Context, _ uuid.UUID, agentName string) ([]*cluster.Cluster, error) {
	var out []*cluster.Cluster
	for _, c := range f.byName {
		if c.ProviderAgent == agentName {
			out = append(out, c)
		}
	}
	return out, nil
}

type fakeEvents struct{ appended []*prov.Event }

func (f *fakeEvents) Append(_ context.Context, e *prov.Event) error {
	f.appended = append(f.appended, e)
	return nil
}
func (f *fakeEvents) ListByCluster(context.Context, uuid.UUID, int, string) (out.ProviderEventPage, error) {
	return out.ProviderEventPage{Events: f.appended}, nil
}
func (f *fakeEvents) PruneOlderThan(context.Context, time.Time) (int64, error) { return 0, nil }

// --- helpers ---------------------------------------------------------

func mkCluster(t *testing.T, name, providerAgent string) *cluster.Cluster {
	t.Helper()
	c, err := cluster.New(
		realm.Realm{ID: uuid.New(), Slug: "default"},
		name,
		[]cluster.ConnectionString{{BootstrapURLs: []string{"b:9092"}, Type: cluster.ConnectionPlaintext}},
		map[string]string{"franz.provisioning/deployment-type": "local-docker", "env": "prod"},
		map[string]string{"num.partitions": "3"},
		providerAgent,
	)
	if err != nil {
		t.Fatal(err)
	}
	c.ID = uuid.New()
	return c
}

func agentCtx(name string, realmID uuid.UUID) context.Context {
	return agent.NewContext(context.Background(), &agent.Agent{
		Name: name, RealmID: realmID, Type: agent.TypeClusterProvider, Status: agent.StatusActive,
	})
}

// --- tests ----------------------------------------------------------

func TestInitialAssignments(t *testing.T) {
	c1 := mkCluster(t, "east-1", "prov-1")
	c2 := mkCluster(t, "west-1", "prov-1")
	c3 := mkCluster(t, "other", "prov-2")
	c2.State = cluster.StatePaused
	svc := NewService(&fakeClusters{byName: map[string]*cluster.Cluster{"east-1": c1, "west-1": c2, "other": c3}}, &fakeEvents{})

	got, err := svc.InitialAssignments(agentCtx("prov-1", c1.RealmID))
	if err != nil {
		t.Fatalf("InitialAssignments: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d assignments, want 2 (only prov-1's clusters)", len(got))
	}
	byName := map[string]prov.Assignment{}
	for _, a := range got {
		byName[a.ClusterName] = a
	}
	if byName["east-1"].Change != prov.ChangeSet {
		t.Errorf("east-1 change = %v, want SET", byName["east-1"].Change)
	}
	if byName["west-1"].Change != prov.ChangePaused {
		t.Errorf("west-1 change = %v, want PAUSED (cluster is paused)", byName["west-1"].Change)
	}
	if byName["east-1"].Provisioning["franz.provisioning/deployment-type"] != "local-docker" {
		t.Errorf("provisioning labels missing / includes non-provisioning keys: %v", byName["east-1"].Provisioning)
	}
	if _, leaked := byName["east-1"].Provisioning["env"]; leaked {
		t.Error("non-provisioning label leaked into assignment")
	}
}

func TestReportStatusOwnershipAndValidation(t *testing.T) {
	c := mkCluster(t, "east-1", "prov-1")
	svc := NewService(&fakeClusters{byName: map[string]*cluster.Cluster{"east-1": c}}, &fakeEvents{})

	// wrong agent → PERMISSION_DENIED
	err := svc.ReportStatus(agentCtx("prov-2", c.RealmID), in.ReportStatusInput{
		ClusterName: "east-1", Phase: prov.PhaseReady, Reachable: true,
	})
	if errs.KindOf(err) != errs.PermissionDenied {
		t.Fatalf("non-owner report = %v, want PERMISSION_DENIED", err)
	}

	// unknown cluster → NOT_FOUND
	if err := svc.ReportStatus(agentCtx("prov-1", c.RealmID), in.ReportStatusInput{ClusterName: "nope", Phase: prov.PhaseReady}); errs.KindOf(err) != errs.NotFound {
		t.Fatalf("unknown cluster = %v", err)
	}

	// bad phase → INVALID_ARGUMENT
	if err := svc.ReportStatus(agentCtx("prov-1", c.RealmID), in.ReportStatusInput{ClusterName: "east-1", Phase: "WAT"}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("bad phase = %v", err)
	}

	// owner, valid → appended
	events := &fakeEvents{}
	svc2 := NewService(&fakeClusters{byName: map[string]*cluster.Cluster{"east-1": c}}, events)
	if err := svc2.ReportStatus(agentCtx("prov-1", c.RealmID), in.ReportStatusInput{
		ClusterName: "east-1", Phase: prov.PhaseReady, Reachable: true, RecipeRef: "local-docker@abcd",
	}); err != nil {
		t.Fatalf("owner report: %v", err)
	}
	if len(events.appended) != 1 {
		t.Fatalf("appended %d events, want 1", len(events.appended))
	}
	e := events.appended[0]
	if e.ReportingAgent != "prov-1" || e.ClusterID != c.ID || e.Phase != prov.PhaseReady || e.RecipeRef != "local-docker@abcd" {
		t.Errorf("event = %+v", e)
	}
}

func TestListEventsUsesRealmFromContext(t *testing.T) {
	c := mkCluster(t, "east-1", "prov-1")
	events := &fakeEvents{appended: []*prov.Event{{Phase: prov.PhaseReady}}}
	svc := NewService(&fakeClusters{byName: map[string]*cluster.Cluster{"east-1": c}}, events)

	ctx := realm.NewContext(context.Background(), realm.Realm{ID: c.RealmID, Slug: "default"})
	page, err := svc.ListEvents(ctx, "east-1", 10, "")
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(page.Events) != 1 {
		t.Fatalf("got %d events", len(page.Events))
	}
}
