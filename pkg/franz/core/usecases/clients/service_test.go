package clients_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clients"
)

var testRealmID = uuid.New()

func ctxWithRealm() context.Context {
	return realm.NewContext(context.Background(),
		realm.Realm{ID: testRealmID, Slug: "default"})
}

// --- fakeClientRepo --------------------------------------------------------

type fakeClientRepo struct {
	byName    map[string]*client.Client
	reserved  map[string]bool
	createErr error
	deleteErr error
}

var _ out.ClientRepository = (*fakeClientRepo)(nil)

func newFakeClientRepo() *fakeClientRepo {
	return &fakeClientRepo{byName: map[string]*client.Client{}, reserved: map[string]bool{}}
}

func (f *fakeClientRepo) Create(_ context.Context, c *client.Client) error {
	if f.createErr != nil {
		return f.createErr
	}
	if _, ok := f.byName[c.Name]; ok || f.reserved[c.Name] {
		return errs.Existsf("client %q already exists", c.Name)
	}
	c.ID = uuid.New()
	c.CreatedAt, c.UpdatedAt = time.Now(), time.Now()
	f.byName[c.Name] = c
	return nil
}

func (f *fakeClientRepo) Get(_ context.Context, _ uuid.UUID, name string) (*client.Client, error) {
	c, ok := f.byName[name]
	if !ok {
		return nil, errs.NotFoundf("client %q not found", name)
	}
	return c, nil
}

func (f *fakeClientRepo) List(_ context.Context, q out.ClientQuery) (out.ClientPage, error) {
	var page out.ClientPage
	for _, c := range f.byName {
		if q.Selector.Match(c.Labels) {
			page.Clients = append(page.Clients, c)
		}
	}
	return page, nil
}

func (f *fakeClientRepo) Update(
	_ context.Context, _ uuid.UUID, name string, mutate func(*client.Client) error,
) (*client.Client, error) {
	c, ok := f.byName[name]
	if !ok {
		return nil, errs.NotFoundf("client %q not found", name)
	}
	if err := mutate(c); err != nil {
		return nil, err
	}
	c.UpdatedAt = time.Now()
	return c, nil
}

func (f *fakeClientRepo) Delete(_ context.Context, _ uuid.UUID, name string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if _, ok := f.byName[name]; !ok {
		return errs.NotFoundf("client %q not found", name)
	}
	delete(f.byName, name)
	f.reserved[name] = true
	return nil
}

// --- fakeGroupRepo -----------------------------------------------------

type fakeGroupRepo struct {
	current      out.ObservationPage
	observations out.ObservationPage
	lastQuery    out.ObservedGroupQuery
	lastObsQuery out.ObservationQuery
}

var _ out.ObservedConsumerGroupRepository = (*fakeGroupRepo)(nil)

func (f *fakeGroupRepo) Append(context.Context, []*consumergroup.Observation) (int, error) {
	return 0, nil
}
func (f *fakeGroupRepo) ListCurrent(_ context.Context, q out.ObservedGroupQuery) (out.ObservationPage, error) {
	f.lastQuery = q
	return f.current, nil
}
func (f *fakeGroupRepo) ListObservations(_ context.Context, q out.ObservationQuery) (out.ObservationPage, error) {
	f.lastObsQuery = q
	return f.observations, nil
}
func (f *fakeGroupRepo) PruneOlderThan(context.Context, time.Time) (int64, error) { return 0, nil }

// --- tests -----------------------------------------------------------------

func TestCreateThenGet(t *testing.T) {
	svc := clients.NewService(newFakeClientRepo(), &fakeGroupRepo{}, nil)
	created, err := svc.Create(ctxWithRealm(), in.CreateClientInput{
		Name: "billing", Labels: map[string]string{"org.com/owner": "payments-team"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.Get(ctxWithRealm(), "billing")
	if err != nil || got.Name != "billing" || got != created {
		t.Fatalf("Get = %+v, %v", got, err)
	}
}

func TestCreateRejectsDuplicateName(t *testing.T) {
	repo := newFakeClientRepo()
	svc := clients.NewService(repo, &fakeGroupRepo{}, nil)
	if _, err := svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "billing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "billing"}); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

// TestCreateRejectsRecreatingADeletedName pins 003.10 "DeleteClient does not
// free the name / FRN" at the service level, not just the repo's.
func TestCreateRejectsRecreatingADeletedName(t *testing.T) {
	repo := newFakeClientRepo()
	svc := clients.NewService(repo, &fakeGroupRepo{}, nil)
	if _, err := svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "billing"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(ctxWithRealm(), "billing"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "billing"}); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestUpdateReplacesLabelsWholesale(t *testing.T) {
	repo := newFakeClientRepo()
	svc := clients.NewService(repo, &fakeGroupRepo{}, nil)
	if _, err := svc.Create(ctxWithRealm(), in.CreateClientInput{
		Name: "billing", Labels: map[string]string{"team": "infra"},
	}); err != nil {
		t.Fatal(err)
	}
	newLabels := map[string]string{"org.com/owner": "payments-team"}
	updated, err := svc.Update(ctxWithRealm(), in.UpdateClientInput{Name: "billing", Labels: &newLabels})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Labels["team"] != "" || updated.Labels["org.com/owner"] != "payments-team" {
		t.Fatalf("Labels = %+v", updated.Labels)
	}
}

func TestUpdateRequiresAMaskedField(t *testing.T) {
	svc := clients.NewService(newFakeClientRepo(), &fakeGroupRepo{}, nil)
	if _, err := svc.Update(ctxWithRealm(), in.UpdateClientInput{Name: "billing"}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestListFiltersBySelector(t *testing.T) {
	repo := newFakeClientRepo()
	svc := clients.NewService(repo, &fakeGroupRepo{}, nil)
	svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "billing", Labels: map[string]string{"team": "infra"}})
	svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "payments", Labels: map[string]string{"team": "payments"}})

	page, err := svc.List(ctxWithRealm(), in.ListClientsInput{Selector: "team=infra"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Clients) != 1 || page.Clients[0].Name != "billing" {
		t.Fatalf("Clients = %+v", page.Clients)
	}
}

func TestListRejectsInvalidSelector(t *testing.T) {
	svc := clients.NewService(newFakeClientRepo(), &fakeGroupRepo{}, nil)
	if _, err := svc.List(ctxWithRealm(), in.ListClientsInput{Selector: "==="}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestListObservedConsumerGroupsRequiresAnExistingClient(t *testing.T) {
	svc := clients.NewService(newFakeClientRepo(), &fakeGroupRepo{}, nil)
	if _, err := svc.ListObservedConsumerGroups(ctxWithRealm(),
		in.ListObservedConsumerGroupsInput{Name: "ghost"}); errs.KindOf(err) != errs.NotFound {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestListObservedConsumerGroupsScopesByClientFRN(t *testing.T) {
	repo := newFakeClientRepo()
	groups := &fakeGroupRepo{}
	svc := clients.NewService(repo, groups, nil)
	svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "billing"})

	if _, err := svc.ListObservedConsumerGroups(ctxWithRealm(),
		in.ListObservedConsumerGroupsInput{Name: "billing"}); err != nil {
		t.Fatal(err)
	}
	if groups.lastQuery.ClientFRN != "default:client:billing" {
		t.Fatalf("ClientFRN = %q", groups.lastQuery.ClientFRN)
	}
}

func TestListConsumerGroupObservationsScopesByClientFRN(t *testing.T) {
	repo := newFakeClientRepo()
	groups := &fakeGroupRepo{}
	svc := clients.NewService(repo, groups, nil)
	svc.Create(ctxWithRealm(), in.CreateClientInput{Name: "billing"})

	if _, err := svc.ListConsumerGroupObservations(ctxWithRealm(),
		in.ListConsumerGroupObservationsInput{Name: "billing"}); err != nil {
		t.Fatal(err)
	}
	if groups.lastObsQuery.ClientFRN != "default:client:billing" {
		t.Fatalf("ClientFRN = %q", groups.lastObsQuery.ClientFRN)
	}
}
