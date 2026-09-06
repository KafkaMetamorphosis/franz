package agents

import (
	"context"
	"sort"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/token"
)

type memRepo struct{ rows map[string]*agent.Agent }

func newMemRepo() *memRepo { return &memRepo{rows: map[string]*agent.Agent{}} }

func (m *memRepo) Create(_ context.Context, a *agent.Agent) error {
	if _, ok := m.rows[a.Name]; ok {
		return errs.Existsf("agent %q already exists", a.Name)
	}
	a.ID = uuid.New()
	cp := *a
	m.rows[a.Name] = &cp
	return nil
}

func (m *memRepo) Get(_ context.Context, _ uuid.UUID, name string) (*agent.Agent, error) {
	a, ok := m.rows[name]
	if !ok {
		return nil, errs.NotFoundf("agent %q not found", name)
	}
	cp := *a
	return &cp, nil
}

func (m *memRepo) List(_ context.Context, q out.AgentQuery) (out.AgentPage, error) {
	var names []string
	for n := range m.rows {
		names = append(names, n)
	}
	sort.Strings(names)
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	var page out.AgentPage
	for _, n := range names {
		if n <= q.AfterName {
			continue
		}
		a := m.rows[n]
		if a.Status == agent.StatusDeleted {
			continue
		}
		if q.TypeFilter != "" && a.Type != q.TypeFilter {
			continue
		}
		cp := *a
		page.Agents = append(page.Agents, &cp)
		if len(page.Agents) > limit {
			page.Agents = page.Agents[:limit]
			page.LastName = page.Agents[limit-1].Name
			break
		}
	}
	return page, nil
}

func (m *memRepo) Mutate(_ context.Context, _ uuid.UUID, name string,
	mutate func(*agent.Agent) error) (*agent.Agent, error) {
	a, ok := m.rows[name]
	if !ok {
		return nil, errs.NotFoundf("agent %q not found", name)
	}
	work := *a
	if err := mutate(&work); err != nil {
		return nil, err
	}
	m.rows[name] = &work
	cp := work
	return &cp, nil
}

func ctxWithRealm() context.Context {
	return realm.NewContext(context.Background(),
		realm.Realm{ID: uuid.New(), Slug: "default", Name: "Default"})
}

func TestCreateReturnsTokenAndStoresHash(t *testing.T) {
	repo := newMemRepo()
	svc := NewService(repo)
	ctx := ctxWithRealm()

	got, err := svc.Create(ctx, in.CreateAgentInput{Name: "prov-1", Type: agent.TypeClusterProvider})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got.Token == "" {
		t.Fatal("no token returned")
	}
	stored := repo.rows["prov-1"]
	if stored.TokenHash != token.Hash(got.Token) {
		t.Errorf("stored hash %q != hash(token)", stored.TokenHash)
	}
	if stored.TokenHash == got.Token {
		t.Error("plaintext token was stored")
	}

	// duplicate
	if _, err := svc.Create(ctx, in.CreateAgentInput{Name: "prov-1", Type: agent.TypeCustom}); errs.KindOf(err) != errs.AlreadyExists {
		t.Errorf("dup create = %v", err)
	}
	// invalid type
	if _, err := svc.Create(ctx, in.CreateAgentInput{Name: "x", Type: ""}); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("unspecified type = %v", err)
	}
}

func TestRotateTokenInvalidatesOld(t *testing.T) {
	repo := newMemRepo()
	svc := NewService(repo)
	ctx := ctxWithRealm()
	created, _ := svc.Create(ctx, in.CreateAgentInput{Name: "a", Type: agent.TypeCustom})

	newTok, err := svc.RotateToken(ctx, "a")
	if err != nil {
		t.Fatalf("RotateToken: %v", err)
	}
	if newTok == created.Token {
		t.Fatal("rotate returned the same token")
	}
	if repo.rows["a"].TokenHash != token.Hash(newTok) {
		t.Error("stored hash not updated to new token")
	}

	// rotate on a deleted agent fails
	_ = svc.Delete(ctx, "a")
	if _, err := svc.RotateToken(ctx, "a"); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("rotate deleted = %v", err)
	}
}

func TestLifecycleAndUpdate(t *testing.T) {
	repo := newMemRepo()
	svc := NewService(repo)
	ctx := ctxWithRealm()
	_, _ = svc.Create(ctx, in.CreateAgentInput{Name: "a", Type: agent.TypeCustom, Labels: map[string]string{"team": "x"}})

	// type is mutable (04.3 assumption)
	newType := agent.TypeTelemetryAgent
	a, err := svc.Update(ctx, in.UpdateAgentInput{Name: "a", Type: &newType})
	if err != nil || a.Type != agent.TypeTelemetryAgent {
		t.Fatalf("Update type: %v %v", err, a.Type)
	}

	if _, err := svc.Pause(ctx, "a"); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if _, err := svc.Resume(ctx, "a"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if err := svc.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := svc.Update(ctx, in.UpdateAgentInput{Name: "a", Labels: &map[string]string{}}); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("update deleted = %v", err)
	}
	// Get still returns the tombstone
	if g, err := svc.Get(ctx, "a"); err != nil || g.Status != agent.StatusDeleted {
		t.Errorf("Get(deleted) = %+v %v", g, err)
	}
}

func TestListTypeFilterAndPagination(t *testing.T) {
	repo := newMemRepo()
	svc := NewService(repo)
	ctx := ctxWithRealm()
	for _, tc := range []struct {
		name string
		typ  agent.Type
	}{
		{"a", agent.TypeClusterProvider},
		{"b", agent.TypeClusterProvider},
		{"c", agent.TypeTelemetryAgent},
		{"d", agent.TypeClusterProvider},
	} {
		_, _ = svc.Create(ctx, in.CreateAgentInput{Name: tc.name, Type: tc.typ})
	}

	all, _ := svc.List(ctx, in.ListAgentsInput{})
	if len(all.Agents) != 4 {
		t.Fatalf("list all = %d", len(all.Agents))
	}
	cp, _ := svc.List(ctx, in.ListAgentsInput{TypeFilter: agent.TypeClusterProvider})
	if len(cp.Agents) != 3 {
		t.Fatalf("type filter = %d, want 3", len(cp.Agents))
	}

	p1, _ := svc.List(ctx, in.ListAgentsInput{TypeFilter: agent.TypeClusterProvider, PageSize: 2})
	if len(p1.Agents) != 2 || p1.NextPageToken == "" {
		t.Fatalf("page 1 = %d tok %q", len(p1.Agents), p1.NextPageToken)
	}
	p2, _ := svc.List(ctx, in.ListAgentsInput{TypeFilter: agent.TypeClusterProvider, PageSize: 2, PageToken: p1.NextPageToken})
	if len(p2.Agents) != 1 || p2.NextPageToken != "" {
		t.Fatalf("page 2 = %d tok %q", len(p2.Agents), p2.NextPageToken)
	}
}
