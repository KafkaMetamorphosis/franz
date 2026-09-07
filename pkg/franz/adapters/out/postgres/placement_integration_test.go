package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"fmt"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	placementdomain "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/channels"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/topics"
)

// placementFixture is the placement wire over a real database: the channel,
// cluster and topic repositories plus the three services that trigger a pass.
type placementFixture struct {
	db        *postgres.DB
	realm     realm.Realm
	ctx       context.Context
	placer    *placement.Service
	channels  *channels.Service
	clusters  *clusters.Service
	topics    *topics.Service
	topicRepo *postgres.TopicRepo
}

func newPlacementFixture(t *testing.T) *placementFixture {
	t.Helper()
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)

	channelRepo := postgres.NewChannelRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)
	topicRepo := postgres.NewTopicRepo(db)
	eventRepo := postgres.NewProviderEventRepo(db)
	realmRepo := postgres.NewRealmRepo(db)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	placer := placement.NewService(channelRepo, clusterRepo, topicRepo, realmRepo, nil, log)
	return &placementFixture{
		db:        db,
		realm:     r,
		ctx:       realm.NewContext(context.Background(), r),
		placer:    placer,
		channels:  channels.NewService(channelRepo, nil, placer),
		clusters:  clusters.NewService(clusterRepo, topicRepo, eventRepo, streamhub.New(), nil, placer),
		topics:    topics.NewService(topicRepo, clusterRepo, nil),
		topicRepo: topicRepo,
	}
}

// shardRows returns every live async-channel shard row of a channel, ordered by
// name, as (shard name → cluster name).
func (f *placementFixture) shardRows(t *testing.T, channelName string) map[string]string {
	t.Helper()
	page, err := f.topics.List(f.ctx, in.ListTopicsInput{AsyncChannel: channelName, PageSize: 100})
	if err != nil {
		t.Fatalf("list shards of %q: %v", channelName, err)
	}
	byName := make(map[string]string, len(page.Topics))
	for _, shard := range page.Topics {
		byName[shard.Name] = shard.ClusterName
	}
	return byName
}

func (f *placementFixture) createCluster(t *testing.T, name string, labels, config map[string]string) {
	t.Helper()
	if _, err := f.clusters.Create(f.ctx, in.CreateClusterInput{
		Name:              name,
		ConnectionStrings: plain(name + ":9092"),
		Labels:            labels,
		Configuration:     config,
	}); err != nil {
		t.Fatalf("create cluster %q: %v", name, err)
	}
}

func (f *placementFixture) createChannel(t *testing.T, name string, shards int32, labels map[string]string) {
	t.Helper()
	if _, err := f.channels.Create(f.ctx, in.CreateChannelInput{
		Name:              name,
		Type:              channel.TypeKafkaTopic,
		ChannelPartitions: shards,
		Labels:            labels,
	}); err != nil {
		t.Fatalf("create channel %q: %v", name, err)
	}
}

func (f *placementFixture) relabelCluster(t *testing.T, name string, labels map[string]string) {
	t.Helper()
	if _, err := f.clusters.Update(f.ctx, in.UpdateClusterInput{
		Name:   name,
		Labels: &labels,
	}); err != nil {
		t.Fatalf("relabel cluster %q: %v", name, err)
	}
}

func containsChannel(channels []*channel.AsyncChannel, name string) bool {
	for _, c := range channels {
		if c.Name == name {
			return true
		}
	}
	return false
}

func (f *placementFixture) relabelChannel(t *testing.T, name string, labels map[string]string) {
	t.Helper()
	if _, err := f.channels.Update(f.ctx, in.UpdateChannelInput{
		Name:   name,
		Labels: &labels,
	}); err != nil {
		t.Fatalf("relabel channel %q: %v", name, err)
	}
}

// A shard row that exists but was never given a cluster — the ADR-API-009
// anomaly a stale fixture or an aborted write can leave. Placement must heal it,
// not skip it forever because "a row with that name exists".
func TestPlacementAdoptsAnUnplacedShardRow(t *testing.T) {
	f := newPlacementFixture(t)
	f.createChannel(t, "orders", 1, nil) // no selector → 0 rows created

	channelID, err := f.topicRepo.ResolveChannelID(f.ctx, f.realm.ID, "orders")
	if err != nil {
		t.Fatal(err)
	}
	unplaced, err := topic.New(f.realm, channelID, "orders", 0, nil, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	// leave KafkaClusterID nil — the anomaly under test
	if err := f.topicRepo.Create(f.ctx, unplaced); err != nil {
		t.Fatalf("seed unplaced shard: %v", err)
	}

	f.createCluster(t, "east-1", map[string]string{"env": "prod"},
		map[string]string{"partitions": "6", "replication-factor": "3", "retention.ms": "60000"})

	// The retry sweep must still see `orders` as underplaced — its one row has
	// no cluster, so it does not count toward channel_partitions.
	underplaced, err := postgres.NewChannelRepo(f.db).ListUnderplaced(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !containsChannel(underplaced, "orders") {
		t.Fatal("orders is not in the sweep work list despite its only shard being unplaced")
	}

	f.relabelChannel(t, "orders", map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})

	shard, err := f.topicRepo.Get(f.ctx, f.realm.ID, "orders-0")
	if err != nil {
		t.Fatal(err)
	}
	if shard.KafkaClusterID == nil || shard.ClusterName != "east-1" {
		t.Fatalf("orders-0 not placed on east-1: %+v", shard)
	}
	if shard.Partitions != 6 || shard.ReplicationFactor != 3 {
		t.Errorf("shape not seeded: %d/%d", shard.Partitions, shard.ReplicationFactor)
	}
	if shard.MaterializedConfiguration["retention.ms"] != "60000" ||
		shard.MaterializedConfiguration["partitions"] != "" {
		t.Errorf("config wrong: %v", shard.MaterializedConfiguration)
	}
	if shard.Misplaced {
		t.Error("adopted shard still marked misplaced")
	}

	// And it stays a single row — no duplicate created alongside the adopted one.
	if rows := f.shardRows(t, "orders"); len(rows) != 1 {
		t.Fatalf("orders has %d shard rows, want 1", len(rows))
	}
}

// A channel with no eligible cluster has zero shard rows; registering a matching
// cluster materialises exactly `channel_partitions` of them, with the Kafka
// shape seeded from that cluster's cluster_configuration (ADR-API-009, 003.6).
func TestPlacementMaterialisesOnClusterRegistration(t *testing.T) {
	f := newPlacementFixture(t)

	f.createChannel(t, "billing-events", 3, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	if got := f.shardRows(t, "billing-events"); len(got) != 0 {
		t.Fatalf("a channel with no eligible cluster has %d shard rows, want 0: %v", len(got), got)
	}

	// A cluster that does not satisfy the affinity changes nothing.
	f.createCluster(t, "west-1", map[string]string{"env": "staging"}, nil)
	if got := f.shardRows(t, "billing-events"); len(got) != 0 {
		t.Fatalf("a non-matching cluster materialised %d shard rows: %v", len(got), got)
	}

	f.createCluster(t, "east-1", map[string]string{"env": "prod"},
		map[string]string{"partitions": "6", "replication-factor": "3", "retention.ms": "604800000"})

	got := f.shardRows(t, "billing-events")
	want := map[string]string{
		"billing-events-0": "east-1",
		"billing-events-1": "east-1",
		"billing-events-2": "east-1",
	}
	if !sameMap(got, want) {
		t.Fatalf("shards = %v, want %v", got, want)
	}
	shard, err := f.topicRepo.Get(f.ctx, f.realm.ID, "billing-events-0")
	if err != nil {
		t.Fatal(err)
	}
	if shard.Partitions != 6 || shard.ReplicationFactor != 3 {
		t.Errorf("kafka shape = %d/%d, want 6/3 seeded from cluster_configuration",
			shard.Partitions, shard.ReplicationFactor)
	}
	if shard.MaterializedConfiguration["retention.ms"] != "604800000" {
		t.Errorf("materialized config = %v", shard.MaterializedConfiguration)
	}
	if _, leaked := shard.MaterializedConfiguration["partitions"]; leaked {
		t.Error("the `partitions` seed key leaked into the config merge")
	}
	if shard.Misplaced {
		t.Error("a freshly placed shard is misplaced")
	}

	// Re-running the pass is idempotent — no duplicate rows, no second cluster.
	f.placer.PlaceRealm(f.ctx, f.realm.ID)
	if got := f.shardRows(t, "billing-events"); !sameMap(got, want) {
		t.Fatalf("a repeated pass changed the assignment: %v", got)
	}
}

// Absent `franz.affinity/selector` ⇒ no candidates ⇒ no shard rows, and the
// channel create still succeeds (003.7, task 13.6).
func TestPlacementWithoutASelectorCreatesNoShards(t *testing.T) {
	f := newPlacementFixture(t)
	f.createCluster(t, "east-1", map[string]string{"env": "prod"}, nil)

	f.createChannel(t, "orders", 4, map[string]string{"my-fleet/tier": "high-volume"})

	if _, err := f.channels.Get(f.ctx, "orders"); err != nil {
		t.Fatalf("the channel must exist even though nothing was placed: %v", err)
	}
	if got := f.shardRows(t, "orders"); len(got) != 0 {
		t.Fatalf("a channel without an affinity selector has %d shard rows, want 0: %v",
			len(got), got)
	}
}

// Re-labelling a cluster so a placed shard mismatches sets `misplaced` and moves
// nothing (003.7 "Re-placement" interim behaviour, task 13.5).
func TestPlacementMarksMisplacedAndMovesNothing(t *testing.T) {
	f := newPlacementFixture(t)
	f.createCluster(t, "east-1", map[string]string{"env": "prod"}, nil)
	f.createChannel(t, "payments", 2, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	// A second, still-matching cluster proves placement does not relocate onto it.
	f.createCluster(t, "east-2", map[string]string{"env": "prod"}, nil)

	before := f.shardRows(t, "payments")
	if len(before) != 2 {
		t.Fatalf("shards = %v, want 2 on east-1", before)
	}

	f.relabelCluster(t, "east-1", map[string]string{"env": "staging"})

	for name, wantCluster := range before {
		shard, err := f.topicRepo.Get(f.ctx, f.realm.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		if !shard.Misplaced {
			t.Errorf("%s: misplaced = false, want true", name)
		}
		if shard.MisplacedReason == "" {
			t.Errorf("%s: no misplaced_reason recorded", name)
		}
		if shard.ClusterName != wantCluster {
			t.Errorf("%s: moved to %q; only migration (003.13) may move a shard",
				name, shard.ClusterName)
		}
	}

	// Putting the label back clears the marker.
	f.relabelCluster(t, "east-1", map[string]string{"env": "prod"})
	for name := range before {
		shard, err := f.topicRepo.Get(f.ctx, f.realm.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		if shard.Misplaced || shard.MisplacedReason != "" {
			t.Errorf("%s: marker survived the cluster matching again (%q)",
				name, shard.MisplacedReason)
		}
	}
}

// Two channels with identical labels over the identical cluster set get
// byte-identical shard → cluster assignments (003.7 determinism invariant, the
// deliverable's first "Done when").
func TestPlacementIsDeterministicAcrossRuns(t *testing.T) {
	f := newPlacementFixture(t)
	labels := map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
		placementdomain.LabelShardSize:        "2",
	}
	f.createCluster(t, "east-1", map[string]string{"env": "prod"}, nil)
	f.createCluster(t, "east-2", map[string]string{
		"env": "prod", placementdomain.LabelWeight: "5",
	}, nil)
	f.createCluster(t, "east-3", map[string]string{"env": "prod"}, nil)

	f.createChannel(t, "run-one", 5, labels)
	f.createChannel(t, "run-two", 5, labels)

	// shard-size 2 over (east-2 weight 5, then east-1 and east-3 by name):
	// east-2 and east-1, with the earlier cluster taking the uneven remainder.
	want := []string{"east-2", "east-1", "east-2", "east-1", "east-2"}
	for _, channelName := range []string{"run-one", "run-two"} {
		rows := f.shardRows(t, channelName)
		for index, wantCluster := range want {
			name := fmt.Sprintf("%s-%d", channelName, index)
			if rows[name] != wantCluster {
				t.Fatalf("%s = %q, want %q (full assignment %v)",
					name, rows[name], wantCluster, rows)
			}
		}
	}
}

// A `no-creation` taint blocks new placement unless tolerated; a `drain` taint
// blocks it outright and unseats what is already there (003.7 "Taints").
func TestPlacementHonoursTaintsAgainstTheDatabase(t *testing.T) {
	f := newPlacementFixture(t)
	f.createCluster(t, "east-1", map[string]string{
		"env": "prod", placementdomain.LabelTaint: "dedicated-billing:no-creation",
	}, nil)

	f.createChannel(t, "orders", 1, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	if got := f.shardRows(t, "orders"); len(got) != 0 {
		t.Fatalf("an untolerated no-creation taint was placed on: %v", got)
	}

	f.createChannel(t, "billing", 1, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
		placementdomain.LabelToleration:       "dedicated-billing:no-creation",
	})
	if got := f.shardRows(t, "billing"); got["billing-0"] != "east-1" {
		t.Fatalf("a tolerating channel was not placed: %v", got)
	}

	// Draining the cluster leaves the shard where it is, but marks it.
	f.relabelCluster(t, "east-1", map[string]string{
		"env": "prod", placementdomain.LabelTaint: "decommission:drain",
	})
	shard, err := f.topicRepo.Get(f.ctx, f.realm.ID, "billing-0")
	if err != nil {
		t.Fatal(err)
	}
	if !shard.Misplaced || shard.ClusterName != "east-1" {
		t.Fatalf("drain taint: misplaced=%v cluster=%q", shard.Misplaced, shard.ClusterName)
	}
}

// The retry sweep is the safety net for a channel whose shards could not be
// placed when it was created (003.7 "retry sweep", task 13.4).
func TestPlacementSweepFillsInMissingShards(t *testing.T) {
	f := newPlacementFixture(t)
	f.createChannel(t, "late-comer", 2, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})

	// Register the cluster behind placement's back, so only the sweep can see it.
	c, err := cluster.New(f.realm, "east-1", plain("east-1:9092"),
		map[string]string{"env": "prod"}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewClusterRepo(f.db).Create(f.ctx, c); err != nil {
		t.Fatal(err)
	}
	if got := f.shardRows(t, "late-comer"); len(got) != 0 {
		t.Fatalf("shards appeared without a placement trigger: %v", got)
	}

	created, err := f.placer.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if created != 2 {
		t.Fatalf("sweep created %d shards, want 2", created)
	}
	if got := f.shardRows(t, "late-comer"); !sameMap(got, map[string]string{
		"late-comer-0": "east-1", "late-comer-1": "east-1",
	}) {
		t.Fatalf("shards = %v", got)
	}

	// A second sweep has nothing left to do.
	if created, err := f.placer.Sweep(context.Background()); err != nil || created != 0 {
		t.Fatalf("second sweep created %d shards (err %v), want 0", created, err)
	}
}

// A malformed reserved placement label is rejected on the write, not stored
// (task 13.2).
func TestPlacementLabelsAreValidatedOnWrite(t *testing.T) {
	f := newPlacementFixture(t)

	_, err := f.channels.Create(f.ctx, in.CreateChannelInput{
		Name:              "bad-channel",
		Type:              channel.TypeKafkaTopic,
		ChannelPartitions: 1,
		Labels:            map[string]string{placementdomain.LabelShardSize: "many"},
	})
	if err == nil {
		t.Fatal("a malformed franz.affinity/shard-size was accepted")
	}
	if _, err := f.channels.Get(f.ctx, "bad-channel"); err == nil {
		t.Error("the rejected channel was written anyway")
	}

	_, err = f.clusters.Create(f.ctx, in.CreateClusterInput{
		Name:              "bad-cluster",
		ConnectionStrings: plain("bad:9092"),
		Labels:            map[string]string{placementdomain.LabelTaint: "unstable:cordon"},
	})
	if err == nil {
		t.Fatal("a malformed franz.taint effect was accepted")
	}
}
