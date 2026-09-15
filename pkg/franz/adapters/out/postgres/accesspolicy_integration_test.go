package postgres_test

import (
	"context"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/channels"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clients"
)

// newClient registers a Client through the repository directly (no service
// layer needed for these fixtures) and returns it.
func newClient(t *testing.T, repo *postgres.ClientRepo, r realm.Realm, name string, labels map[string]string) *client.Client {
	t.Helper()
	c, err := client.New(r, name, labels)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if err := repo.Create(context.Background(), c); err != nil {
		t.Fatalf("Create client %q: %v", name, err)
	}
	return c
}

// TestAccessPolicyForwardAndReverseViewsAgree is 17.8's core check: the
// forward view (ListChannelClients) and the reverse view
// (ListClientChannelAccess) must report the identical grant for the same
// (client, channel) pair, since both are the same accesspolicy.Evaluate call
// viewed from opposite ends.
func TestAccessPolicyForwardAndReverseViewsAgree(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	cleanupClients(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)

	chRepo := postgres.NewChannelRepo(db)
	clientRepo := postgres.NewClientRepo(db)
	channelSvc := channels.NewService(chRepo, nil, nil, clientRepo)
	clientSvc := clients.NewService(clientRepo, nil, chRepo)

	billing := newClient(t, clientRepo, r, "billing", nil)
	newClient(t, clientRepo, r, "payments-consumer", nil)

	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{{
		Effect:      accesspolicy.Allow,
		Principal:   accesspolicy.Principal{ClientFRN: billing.FRN.Path()},
		Permissions: []accesspolicy.Permission{accesspolicy.Read, accesspolicy.Write},
	}}}
	if _, err := channelSvc.Create(ctx, in.CreateChannelInput{
		Name: "orders", Type: channel.TypeKafkaTopic, ChannelPartitions: 1, AccessPolicy: policy,
	}); err != nil {
		t.Fatalf("Create channel: %v", err)
	}
	if _, err := channelSvc.Create(ctx, in.CreateChannelInput{
		Name: "invoices", Type: channel.TypeKafkaTopic, ChannelPartitions: 1,
	}); err != nil {
		t.Fatalf("Create channel: %v", err)
	}

	// Forward: orders' client list has billing (READ+WRITE), not payments-consumer.
	forward, err := channelSvc.ListChannelClients(ctx, in.ListChannelClientsInput{Name: "orders"})
	if err != nil {
		t.Fatalf("ListChannelClients: %v", err)
	}
	if len(forward.Access) != 1 || forward.Access[0].ClientFRN.Path() != billing.FRN.Path() {
		t.Fatalf("forward view = %+v, want only billing", forward.Access)
	}
	if len(forward.Access[0].Effective) != 2 {
		t.Fatalf("forward effective = %+v, want READ+WRITE", forward.Access[0].Effective)
	}

	// Reverse: billing's channel-access list has orders, not invoices.
	reverse, err := clientSvc.ListClientChannelAccess(ctx, in.ListClientChannelAccessInput{Name: "billing"})
	if err != nil {
		t.Fatalf("ListClientChannelAccess: %v", err)
	}
	if len(reverse.Access) != 1 || reverse.Access[0].AsyncChannel != "orders" {
		t.Fatalf("reverse view = %+v, want only orders", reverse.Access)
	}
	if len(reverse.Access[0].Effective) != 2 {
		t.Fatalf("reverse effective = %+v, want READ+WRITE", reverse.Access[0].Effective)
	}

	// The other client / the other channel see nothing.
	noAccess, err := clientSvc.ListClientChannelAccess(ctx, in.ListClientChannelAccessInput{Name: "payments-consumer"})
	if err != nil {
		t.Fatalf("ListClientChannelAccess: %v", err)
	}
	if len(noAccess.Access) != 0 {
		t.Fatalf("payments-consumer access = %+v, want none", noAccess.Access)
	}
	invoicesClients, err := channelSvc.ListChannelClients(ctx, in.ListChannelClientsInput{Name: "invoices"})
	if err != nil {
		t.Fatalf("ListChannelClients: %v", err)
	}
	if len(invoicesClients.Access) != 0 {
		t.Fatalf("invoices clients = %+v, want none (empty policy denies everyone)", invoicesClients.Access)
	}
}

// TestAccessPolicyChangeIsReflectedInBothViews pins 17.8's second check: a
// SetAccessPolicy change shows up in both views without anything else changing.
func TestAccessPolicyChangeIsReflectedInBothViews(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	cleanupClients(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)

	chRepo := postgres.NewChannelRepo(db)
	clientRepo := postgres.NewClientRepo(db)
	channelSvc := channels.NewService(chRepo, nil, nil, clientRepo)
	clientSvc := clients.NewService(clientRepo, nil, chRepo)

	billing := newClient(t, clientRepo, r, "billing", nil)
	if _, err := channelSvc.Create(ctx, in.CreateChannelInput{
		Name: "orders", Type: channel.TypeKafkaTopic, ChannelPartitions: 1,
	}); err != nil {
		t.Fatalf("Create channel: %v", err)
	}

	// Before SetAccessPolicy: empty policy denies everyone.
	before, _ := channelSvc.ListChannelClients(ctx, in.ListChannelClientsInput{Name: "orders"})
	if len(before.Access) != 0 {
		t.Fatalf("before = %+v, want none", before.Access)
	}

	grant := accesspolicy.Policy{Statements: []accesspolicy.Statement{{
		Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{ClientFRN: billing.FRN.Path()},
		Permissions: []accesspolicy.Permission{accesspolicy.Read},
	}}}
	if _, err := channelSvc.SetAccessPolicy(ctx, "orders", grant); err != nil {
		t.Fatalf("SetAccessPolicy: %v", err)
	}

	afterForward, _ := channelSvc.ListChannelClients(ctx, in.ListChannelClientsInput{Name: "orders"})
	if len(afterForward.Access) != 1 || afterForward.Access[0].ClientFRN.Path() != billing.FRN.Path() {
		t.Fatalf("forward after grant = %+v", afterForward.Access)
	}
	afterReverse, _ := clientSvc.ListClientChannelAccess(ctx, in.ListClientChannelAccessInput{Name: "billing"})
	if len(afterReverse.Access) != 1 || afterReverse.Access[0].AsyncChannel != "orders" {
		t.Fatalf("reverse after grant = %+v", afterReverse.Access)
	}

	// Revoke via an explicit DENY — both views must reflect it.
	revoke := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		{Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{ClientFRN: billing.FRN.Path()},
			Permissions: []accesspolicy.Permission{accesspolicy.Read}},
		{Effect: accesspolicy.Deny, Principal: accesspolicy.Principal{ClientFRN: billing.FRN.Path()},
			Permissions: []accesspolicy.Permission{accesspolicy.Read}},
	}}
	if _, err := channelSvc.SetAccessPolicy(ctx, "orders", revoke); err != nil {
		t.Fatalf("SetAccessPolicy revoke: %v", err)
	}
	revokedForward, _ := channelSvc.ListChannelClients(ctx, in.ListChannelClientsInput{Name: "orders"})
	if len(revokedForward.Access) != 0 {
		t.Fatalf("forward after revoke = %+v, want none", revokedForward.Access)
	}
	revokedReverse, _ := clientSvc.ListClientChannelAccess(ctx, in.ListClientChannelAccessInput{Name: "billing"})
	if len(revokedReverse.Access) != 0 {
		t.Fatalf("reverse after revoke = %+v, want none", revokedReverse.Access)
	}
}

// TestListChannelClientsPaginates exercises 17.8's third check over a real
// keyset page — the underlying Client scan mirrors ClusterRepo.List's pattern
// (fetch a page, filter in Go), so a small page size must still page forward
// correctly across matches.
func TestListChannelClientsPaginates(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	cleanupClients(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)

	chRepo := postgres.NewChannelRepo(db)
	clientRepo := postgres.NewClientRepo(db)
	channelSvc := channels.NewService(chRepo, nil, nil, clientRepo)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		newClient(t, clientRepo, r, name, nil)
	}
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{{
		Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{ClientFRN: "*"},
		Permissions: []accesspolicy.Permission{accesspolicy.Read},
	}}}
	if _, err := channelSvc.Create(ctx, in.CreateChannelInput{
		Name: "orders", Type: channel.TypeKafkaTopic, ChannelPartitions: 1, AccessPolicy: policy,
	}); err != nil {
		t.Fatalf("Create channel: %v", err)
	}

	page1, err := channelSvc.ListChannelClients(ctx, in.ListChannelClientsInput{Name: "orders", PageSize: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Access) != 2 || page1.NextPageToken == "" {
		t.Fatalf("page 1 = %+v", page1.Access)
	}
	page2, err := channelSvc.ListChannelClients(ctx, in.ListChannelClientsInput{
		Name: "orders", PageSize: 2, PageToken: page1.NextPageToken,
	})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2.Access) != 1 {
		t.Fatalf("page 2 = %+v, want the last client", page2.Access)
	}

	seen := map[string]bool{}
	for _, a := range append(page1.Access, page2.Access...) {
		seen[a.ClientFRN.Name()] = true
	}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if !seen[name] {
			t.Errorf("client %q missing across pages", name)
		}
	}
}
