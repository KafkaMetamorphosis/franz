package clusters

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// --- in-memory fakes -----------------------------------------------------

type memRepo struct {
	rows map[string]*cluster.Cluster // key: name
}

func newMemRepo() *memRepo { return &memRepo{rows: map[string]*cluster.Cluster{}} }

func (m *memRepo) Create(_ context.Context, c *cluster.Cluster) error {
	if _, ok := m.rows[c.Name]; ok {
		return errs.Existsf("kafka cluster %q already exists", c.Name)
	}
	c.ID = uuid.New()
	cp := *c
	m.rows[c.Name] = &cp
	return nil
}

func (m *memRepo) Get(_ context.Context, _ uuid.UUID, name string) (*cluster.Cluster, error) {
	c, ok := m.rows[name]
	if !ok {
		return nil, errs.NotFoundf("kafka cluster %q not found", name)
	}
	cp := *c
	return &cp, nil
}

func (m *memRepo) List(_ context.Context, q out.ClusterQuery) (out.ClusterPage, error) {
	var names []string
	for n := range m.rows {
		names = append(names, n)
	}
	sort.Strings(names)

	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	var page out.ClusterPage
	for _, n := range names {
		if n <= q.AfterName {
			continue
		}
		c := m.rows[n]
		if !q.IncludeDeleted && c.State == cluster.StateDeleted {
			continue
		}
		if !q.Selector.Match(c.Labels) {
			continue
		}
		cp := *c
		page.Clusters = append(page.Clusters, &cp)
		if len(page.Clusters) > limit {
			page.Clusters = page.Clusters[:limit]
			page.LastName = page.Clusters[limit-1].Name
			break
		}
	}
	return page, nil
}

func (m *memRepo) Mutate(_ context.Context, _ uuid.UUID, name string,
	mutate func(*cluster.Cluster) error) (*cluster.Cluster, error) {
	c, ok := m.rows[name]
	if !ok {
		return nil, errs.NotFoundf("kafka cluster %q not found", name)
	}
	work := *c
	if err := mutate(&work); err != nil {
		return nil, err
	}
	m.rows[name] = &work
	cp := work
	return &cp, nil
}

func (m *memRepo) ListByProviderAgent(_ context.Context, _ uuid.UUID, agentName string) ([]*cluster.Cluster, error) {
	var out []*cluster.Cluster
	for _, c := range m.rows {
		if c.ProviderAgent == agentName {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out, nil
}

type guard struct{ n int }

func (g guard) CountLiveTopics(context.Context, uuid.UUID) (int, error) { return g.n, nil }

// capturePublisher records every assignment published, keyed by agent name.
type capturePublisher struct {
	mu   sync.Mutex
	sent map[string][]provider.Assignment
}

func newCapturePublisher() *capturePublisher {
	return &capturePublisher{sent: map[string][]provider.Assignment{}}
}

func (p *capturePublisher) PublishAssignment(agentName string, a provider.Assignment) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent[agentName] = append(p.sent[agentName], a)
}

func (p *capturePublisher) last(agentName string) (provider.Assignment, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	xs := p.sent[agentName]
	if len(xs) == 0 {
		return provider.Assignment{}, false
	}
	return xs[len(xs)-1], true
}

// noStatus is a ProviderStatusReader that always reports "no status yet".
type noStatus struct{}

func (noStatus) LatestStatus(context.Context, uuid.UUID) (*provider.Status, error) { return nil, nil }

// --- helpers -----------------------------------------------------------

func ctxWithRealm() context.Context {
	return realm.NewContext(context.Background(),
		realm.Realm{ID: uuid.New(), Slug: "default", Name: "Default"})
}

func mkService(topics int) (*Service, *memRepo) {
	svc, repo, _ := mkServiceP(topics)
	return svc, repo
}

func mkServiceP(topics int) (*Service, *memRepo, *capturePublisher) {
	repo := newMemRepo()
	pub := newCapturePublisher()
	return NewService(repo, guard{n: topics}, noStatus{}, pub), repo, pub
}

func plainConns() []cluster.ConnectionString {
	return []cluster.ConnectionString{{BootstrapURLs: []string{"b:9092"}, Type: cluster.ConnectionPlaintext}}
}

// --- tests -----------------------------------------------------------

func TestCreateGetLifecycle(t *testing.T) {
	svc, _ := mkService(0)
	ctx := ctxWithRealm()

	c, err := svc.Create(ctx, in.CreateClusterInput{Name: "east-1", ConnectionStrings: plainConns()})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if c.State != cluster.StateActive {
		t.Fatalf("state = %v", c.State)
	}

	// duplicate name
	if _, err := svc.Create(ctx, in.CreateClusterInput{Name: "east-1", ConnectionStrings: plainConns()}); err == nil ||
		errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("duplicate create = %v", err)
	}

	got, err := svc.Get(ctx, "east-1")
	if err != nil || got.Name != "east-1" {
		t.Fatalf("Get: %v %+v", err, got)
	}

	if _, err := svc.Pause(ctx, "east-1"); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	paused, _ := svc.Get(ctx, "east-1")
	if paused.State != cluster.StatePaused {
		t.Fatalf("state after pause = %v", paused.State)
	}
	if _, err := svc.Resume(ctx, "east-1"); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if err := svc.Delete(ctx, "east-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// operations on a deleted cluster fail
	if _, err := svc.Pause(ctx, "east-1"); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("pause deleted = %v", err)
	}
	if err := svc.Delete(ctx, "east-1"); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("re-delete = %v", err)
	}
}

func TestDeleteBlockedByLiveTopics(t *testing.T) {
	svc, _ := mkService(3)
	ctx := ctxWithRealm()
	if _, err := svc.Create(ctx, in.CreateClusterInput{Name: "east-1", ConnectionStrings: plainConns()}); err != nil {
		t.Fatal(err)
	}
	err := svc.Delete(ctx, "east-1")
	if errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("delete with live topics = %v, want FAILED_PRECONDITION", err)
	}
}

func TestUpdateMaskedFields(t *testing.T) {
	svc, _ := mkService(0)
	ctx := ctxWithRealm()
	_, _ = svc.Create(ctx, in.CreateClusterInput{
		Name:              "east-1",
		ConnectionStrings: plainConns(),
		Labels:            map[string]string{"env": "prod", "team": "billing"},
		Configuration:     map[string]string{"retention.ms": "1000"},
	})

	newLabels := map[string]string{"env": "staging"}
	c, err := svc.Update(ctx, in.UpdateClusterInput{Name: "east-1", Labels: &newLabels})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if len(c.Labels) != 1 || c.Labels["env"] != "staging" {
		t.Errorf("labels = %v (want wholesale replace)", c.Labels)
	}
	if c.Configuration["retention.ms"] != "1000" {
		t.Errorf("unmasked cluster_configuration was disturbed: %v", c.Configuration)
	}

	// update rejects a bad connection string
	bad := []cluster.ConnectionString{{}}
	if _, err := svc.Update(ctx, in.UpdateClusterInput{Name: "east-1", ConnectionStrings: &bad}); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("update with bad conn = %v", err)
	}
}

func TestListSelectorAndPagination(t *testing.T) {
	svc, _ := mkService(0)
	ctx := ctxWithRealm()
	for _, tc := range []struct {
		name string
		env  string
	}{{"a", "prod"}, {"b", "prod"}, {"c", "staging"}, {"d", "prod"}} {
		_, _ = svc.Create(ctx, in.CreateClusterInput{
			Name: tc.name, ConnectionStrings: plainConns(),
			Labels: map[string]string{"env": tc.env},
		})
	}

	// selector filter
	page, err := svc.List(ctx, in.ListClustersInput{Selector: "env=prod"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Clusters) != 3 {
		t.Fatalf("selector list = %d clusters, want 3", len(page.Clusters))
	}

	// pagination: 2 per page
	p1, _ := svc.List(ctx, in.ListClustersInput{Selector: "env=prod", PageSize: 2})
	if len(p1.Clusters) != 2 || p1.NextPageToken == "" {
		t.Fatalf("page 1 = %d, token %q", len(p1.Clusters), p1.NextPageToken)
	}
	p2, _ := svc.List(ctx, in.ListClustersInput{Selector: "env=prod", PageSize: 2, PageToken: p1.NextPageToken})
	if len(p2.Clusters) != 1 || p2.NextPageToken != "" {
		t.Fatalf("page 2 = %d, token %q", len(p2.Clusters), p2.NextPageToken)
	}
	if p1.Clusters[0].Name == p2.Clusters[0].Name {
		t.Error("page 2 repeated a row from page 1")
	}

	// a token from a different query is rejected
	if _, err := svc.List(ctx, in.ListClustersInput{Selector: "env=staging", PageToken: p1.NextPageToken}); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("cross-query token = %v", err)
	}

	// deleted clusters drop out of List but Get still returns them
	_ = svc.Delete(ctx, "a")
	after, _ := svc.List(ctx, in.ListClustersInput{Selector: "env=prod"})
	if len(after.Clusters) != 2 {
		t.Errorf("after delete list = %d, want 2", len(after.Clusters))
	}
	if g, err := svc.Get(ctx, "a"); err != nil || g.State != cluster.StateDeleted {
		t.Errorf("Get(deleted) = %+v %v", g, err)
	}
}

func TestAssignmentPublishedOnLifecycle(t *testing.T) {
	svc, _, pub := mkServiceP(0)
	ctx := ctxWithRealm()

	_, err := svc.Create(ctx, in.CreateClusterInput{
		Name:              "east-1",
		ConnectionStrings: plainConns(),
		ProviderAgent:     "prov-1",
		Labels:            map[string]string{"franz.provisioning/deployment-type": "local-docker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	a, ok := pub.last("prov-1")
	if !ok || a.Change != provider.ChangeSet {
		t.Fatalf("create → %+v ok=%v, want SET", a, ok)
	}
	if a.Provisioning["franz.provisioning/deployment-type"] != "local-docker" {
		t.Errorf("provisioning labels not carried: %v", a.Provisioning)
	}

	// editing a provisioning label pushes a SET delta
	newLabels := map[string]string{"franz.provisioning/deployment-type": "local-docker", "franz.provisioning/kafka-version": "3.7.0"}
	if _, err := svc.Update(ctx, in.UpdateClusterInput{Name: "east-1", Labels: &newLabels}); err != nil {
		t.Fatal(err)
	}
	if a, _ := pub.last("prov-1"); a.Change != provider.ChangeSet || a.Provisioning["franz.provisioning/kafka-version"] != "3.7.0" {
		t.Errorf("update → %+v, want SET with new label", a)
	}

	if _, err := svc.Pause(ctx, "east-1"); err != nil {
		t.Fatal(err)
	}
	if a, _ := pub.last("prov-1"); a.Change != provider.ChangePaused {
		t.Errorf("pause → %v, want PAUSED", a.Change)
	}

	if _, err := svc.Resume(ctx, "east-1"); err != nil {
		t.Fatal(err)
	}
	if a, _ := pub.last("prov-1"); a.Change != provider.ChangeSet {
		t.Errorf("resume → %v, want SET", a.Change)
	}

	if err := svc.Delete(ctx, "east-1"); err != nil {
		t.Fatal(err)
	}
	if a, _ := pub.last("prov-1"); a.Change != provider.ChangeRemoved {
		t.Errorf("delete → %v, want REMOVED", a.Change)
	}
}

func TestAssignmentReassignPublishesRemovedToOldAgent(t *testing.T) {
	svc, _, pub := mkServiceP(0)
	ctx := ctxWithRealm()

	_, _ = svc.Create(ctx, in.CreateClusterInput{Name: "east-1", ConnectionStrings: plainConns(), ProviderAgent: "prov-1"})

	newAgent := "prov-2"
	if _, err := svc.Update(ctx, in.UpdateClusterInput{Name: "east-1", ProviderAgent: &newAgent}); err != nil {
		t.Fatal(err)
	}
	if a, ok := pub.last("prov-1"); !ok || a.Change != provider.ChangeRemoved {
		t.Errorf("old agent → %+v, want REMOVED", a)
	}
	if a, ok := pub.last("prov-2"); !ok || a.Change != provider.ChangeSet {
		t.Errorf("new agent → %+v, want SET", a)
	}
}
