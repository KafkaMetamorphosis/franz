package postgres_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/channels"
)

func itoa(i int) string { return strconv.Itoa(i) }

func createInput(name string, parts int32) in.CreateChannelInput {
	return in.CreateChannelInput{Name: name, Type: channel.TypeKafkaTopic, ChannelPartitions: parts}
}

func mustSelector(t *testing.T, s string) selector.Selector {
	t.Helper()
	sel, err := selector.Parse(s)
	if err != nil {
		t.Fatalf("selector %q: %v", s, err)
	}
	return sel
}

func allowRead(clientFRN string) accesspolicy.Policy {
	return accesspolicy.Policy{Statements: []accesspolicy.Statement{{
		Effect:      accesspolicy.Allow,
		Principal:   accesspolicy.Principal{ClientFRN: clientFRN},
		Permissions: []accesspolicy.Permission{accesspolicy.Read},
	}}}
}

func newChannel(t *testing.T, r realm.Realm, name string, parts int32, policy accesspolicy.Policy) *channel.AsyncChannel {
	t.Helper()
	c, err := channel.New(r, name, channel.TypeKafkaTopic, parts, map[string]string{"team": "x"}, policy)
	if err != nil {
		t.Fatalf("channel.New: %v", err)
	}
	return c
}

func countTopics(t *testing.T, db *postgres.DB, channelID uuid.UUID) int {
	t.Helper()
	var n int
	if err := db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM kafka_topic WHERE async_channel_id=$1`, channelID).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestChannelRepoCreateWritesNoShards(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	repo := postgres.NewChannelRepo(db)

	c := newChannel(t, r, "orders", 6, allowRead("frn:default:client:a"))
	if err := repo.Create(ctx, c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if countTopics(t, db, c.ID) != 0 {
		t.Fatal("CreateAsyncChannel must not create shards (ADR-API-009)")
	}
	got, err := repo.Get(ctx, r.ID, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got.ChannelPartitions != 6 || got.State != channel.StateActive ||
		got.FRN.String() != "frn:default:async-channel:orders" {
		t.Fatalf("round-trip: %+v", got)
	}
	if len(got.AccessPolicy.Statements) != 1 ||
		got.AccessPolicy.Statements[0].Principal.ClientFRN != "frn:default:client:a" {
		t.Fatalf("policy round-trip: %+v", got.AccessPolicy)
	}

	// duplicate name
	dup := newChannel(t, r, "orders", 1, accesspolicy.Policy{})
	if err := repo.Create(ctx, dup); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("dup = %v", err)
	}
}

func TestChannelServiceDeleteAndPauseCascade(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	chRepo := postgres.NewChannelRepo(db)
	topicRepo := postgres.NewTopicRepo(db)
	svc := channels.NewService(chRepo, nil)

	if _, err := svc.Create(ctx, createInput("orders", 3)); err != nil {
		t.Fatal(err)
	}
	c, _ := chRepo.Get(ctx, r.ID, "orders")

	// placement (deliverable 11) would create these; insert two directly
	for i := 0; i < 2; i++ {
		sh, _ := topic.New(r, c.ID, "orders", i, nil, nil, 1, 1)
		if err := topicRepo.Create(ctx, sh); err != nil {
			t.Fatal(err)
		}
	}

	// pause cascades
	if _, err := svc.Pause(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	c, _ = chRepo.Get(ctx, r.ID, "orders")
	if c.State != channel.StatePaused {
		t.Fatalf("channel state = %s", c.State)
	}
	for i := 0; i < 2; i++ {
		sh, _ := topicRepo.Get(ctx, r.ID, "orders-"+itoa(i))
		if sh.State != topic.StatePaused {
			t.Errorf("shard %d state = %s, want PAUSED", i, sh.State)
		}
	}

	// resume returns shards to PENDING
	if _, err := svc.Resume(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		sh, _ := topicRepo.Get(ctx, r.ID, "orders-"+itoa(i))
		if sh.State != topic.StatePending {
			t.Errorf("shard %d after resume = %s, want PENDING", i, sh.State)
		}
	}

	// delete cascades DELETED to channel + shards
	if err := svc.Delete(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	c, _ = chRepo.Get(ctx, r.ID, "orders")
	if c.State != channel.StateDeleted {
		t.Fatalf("channel not deleted: %s", c.State)
	}
	for i := 0; i < 2; i++ {
		sh, _ := topicRepo.Get(ctx, r.ID, "orders-"+itoa(i))
		if sh.State != topic.StateDeleted {
			t.Errorf("shard %d after channel delete = %s, want DELETED", i, sh.State)
		}
	}
	// any further op on the deleted channel fails
	if err := svc.Delete(ctx, "orders"); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("delete on deleted → %v", err)
	}
}

func TestChannelServiceSetAccessPolicy(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	svc := channels.NewService(postgres.NewChannelRepo(db), nil)

	if _, err := svc.Create(ctx, createInput("orders", 1)); err != nil {
		t.Fatal(err)
	}

	// a well-formed policy is stored verbatim
	good := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		{Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{ClientFRN: "frn:default:client:xpto-*"}, Permissions: []accesspolicy.Permission{accesspolicy.Read}},
		{Effect: accesspolicy.Deny, Principal: accesspolicy.Principal{ClientFRN: "frn:default:client:xpto-blah"}, Permissions: []accesspolicy.Permission{accesspolicy.Read}},
	}}
	c, err := svc.SetAccessPolicy(ctx, "orders", good)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.AccessPolicy.Statements) != 2 || c.AccessPolicy.Statements[1].Effect != accesspolicy.Deny {
		t.Fatalf("policy not stored: %+v", c.AccessPolicy)
	}

	// malformed statements are rejected
	bad := []accesspolicy.Policy{
		{Statements: []accesspolicy.Statement{{Effect: "", Principal: accesspolicy.Principal{ClientFRN: "x"}, Permissions: []accesspolicy.Permission{accesspolicy.Read}}}},
		{Statements: []accesspolicy.Statement{{Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{ClientFRN: "x"}}}},
		{Statements: []accesspolicy.Statement{{Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{}, Permissions: []accesspolicy.Permission{accesspolicy.Read}}}},
	}
	for i, p := range bad {
		if _, err := svc.SetAccessPolicy(ctx, "orders", p); errs.KindOf(err) != errs.InvalidArgument {
			t.Errorf("bad policy %d → %v", i, err)
		}
	}
}

func TestChannelRepoListSelector(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	repo := postgres.NewChannelRepo(db)

	a := newChannel(t, r, "alpha", 1, accesspolicy.Policy{})
	a.Labels = map[string]string{"team": "payments"}
	b := newChannel(t, r, "beta", 1, accesspolicy.Policy{})
	b.Labels = map[string]string{"team": "billing"}
	if err := repo.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, b); err != nil {
		t.Fatal(err)
	}

	all, err := repo.List(ctx, out.ChannelQuery{RealmID: r.ID, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Channels) != 2 {
		t.Fatalf("list all: %d", len(all.Channels))
	}

	sel := mustSelector(t, "team=payments")
	filtered, _ := repo.List(ctx, out.ChannelQuery{RealmID: r.ID, Selector: sel, Limit: 50})
	if len(filtered.Channels) != 1 || filtered.Channels[0].Name != "alpha" {
		t.Fatalf("selector filter: %+v", filtered.Channels)
	}
}
