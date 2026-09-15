package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	migrationdomain "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/migration"
	placementdomain "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/channels"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/migration"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/topics"
)

// migrationFixture wires the real migration engine (18) over a real database:
// the same channels/clusters/topics/placement services deliverable 13 already
// tests with, plus the migration service under test.
type migrationFixture struct {
	db        *postgres.DB
	realm     realm.Realm
	ctx       context.Context
	migration *migration.Service
	channels  *channels.Service
	clusters  *clusters.Service
	topics    *topics.Service
	topicRepo *postgres.TopicRepo
	migRepo   *postgres.ShardMigrationRepo
}

func newMigrationFixture(t *testing.T) *migrationFixture {
	t.Helper()
	db := openTestDB(t)
	cleanupTopics(t, db) // clears shard_migration first (topic_integration_test.go)
	r := seededRealm(t, db)

	channelRepo := postgres.NewChannelRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)
	topicRepo := postgres.NewTopicRepo(db)
	eventRepo := postgres.NewProviderEventRepo(db)
	realmRepo := postgres.NewRealmRepo(db)
	migRepo := postgres.NewShardMigrationRepo(db)
	sampleRepo := postgres.NewIndicatorSampleRepo(db)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	placer := placement.NewService(channelRepo, clusterRepo, topicRepo, realmRepo, nil, log)
	topicSvc := topics.NewService(topicRepo, clusterRepo, nil)
	migSvc := migration.NewService(migRepo, topicRepo, clusterRepo, channelRepo, realmRepo,
		sampleRepo, topicSvc, nil, log)

	return &migrationFixture{
		db:        db,
		realm:     r,
		ctx:       realm.NewContext(context.Background(), r),
		migration: migSvc,
		channels:  channels.NewService(channelRepo, nil, placer, nil),
		clusters:  clusters.NewService(clusterRepo, topicRepo, eventRepo, streamhub.New(), nil, placer, migSvc),
		topics:    topicSvc,
		topicRepo: topicRepo,
		migRepo:   migRepo,
	}
}

func (f *migrationFixture) createCluster(t *testing.T, name string) {
	t.Helper()
	if _, err := f.clusters.Create(f.ctx, in.CreateClusterInput{
		Name:              name,
		ConnectionStrings: plain(name + ":9092"),
		Labels:            map[string]string{"env": "prod"},
		Configuration:     map[string]string{"partitions": "1", "replication-factor": "1"},
	}); err != nil {
		t.Fatalf("create cluster %q: %v", name, err)
	}
}

func (f *migrationFixture) createChannel(t *testing.T, name string, shards int32) {
	t.Helper()
	if _, err := f.channels.Create(f.ctx, in.CreateChannelInput{
		Name: name, Type: channel.TypeKafkaTopic, ChannelPartitions: shards,
		Labels: map[string]string{placementdomain.LabelAffinitySelector: "env=prod"},
	}); err != nil {
		t.Fatalf("create channel %q: %v", name, err)
	}
}

// markReady simulates the Resource Provider agent's reconciliation report —
// there is no real agent in this test, so the target shard's PENDING → READY
// transition is applied directly.
func (f *migrationFixture) markReady(t *testing.T, shardName string) {
	t.Helper()
	shard, err := f.topicRepo.Get(f.ctx, f.realm.ID, shardName)
	if err != nil {
		t.Fatalf("get shard %q: %v", shardName, err)
	}
	if _, err := f.topicRepo.MutateChannelShards(f.ctx, f.realm.ID, shard.AsyncChannelID,
		func(shards []*topic.KafkaTopic) error {
			for _, sh := range shards {
				if sh.Name == shardName {
					return sh.SetState(topic.StateReady)
				}
			}
			return nil
		}); err != nil {
		t.Fatalf("mark %q ready: %v", shardName, err)
	}
}

func TestMigrationFullLifecycle(t *testing.T) {
	f := newMigrationFixture(t)
	f.createCluster(t, "cluster-a")
	f.createCluster(t, "cluster-b")
	f.createChannel(t, "orders", 1)

	before := f.topicRepo
	shard, err := before.Get(f.ctx, f.realm.ID, "orders-0")
	if err != nil {
		t.Fatalf("get orders-0: %v", err)
	}
	sourceCluster := shard.ClusterName
	targetCluster := "cluster-b"
	if sourceCluster == targetCluster {
		targetCluster = "cluster-a"
	}

	m, err := f.migration.MigrateKafkaTopic(f.ctx, "orders-0", targetCluster)
	if err != nil {
		t.Fatalf("MigrateKafkaTopic: %v", err)
	}
	if m.Phase != migrationdomain.PhaseProvisioning {
		t.Fatalf("initial phase = %v", m.Phase)
	}
	if m.SourceTopicName != "orders-0" || m.TargetTopicName != "orders-1" {
		t.Fatalf("shard names = %q / %q, want orders-0 / orders-1", m.SourceTopicName, m.TargetTopicName)
	}

	// A second migration for the same shard is rejected while this one is active.
	if _, err := f.migration.MigrateKafkaTopic(f.ctx, "orders-0", targetCluster); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("concurrent migration kind = %v", err)
	}

	// PROVISIONING → CUTOVER: waits for the target to report READY.
	if n, err := f.migration.Sweep(f.ctx); err != nil || n != 0 {
		t.Fatalf("sweep before target ready: n=%d err=%v", n, err)
	}
	f.markReady(t, "orders-1")
	if n, err := f.migration.Sweep(f.ctx); err != nil || n != 1 {
		t.Fatalf("sweep to cutover: n=%d err=%v", n, err)
	}
	got, err := f.migration.GetShardMigration(f.ctx, m.ID)
	if err != nil || got.Phase != migrationdomain.PhaseCutover {
		t.Fatalf("phase after target ready = %v (%v)", got.Phase, err)
	}

	// CUTOVER → DRAINING: applies SetConsumption(DISABLED) to the source.
	if n, err := f.migration.Sweep(f.ctx); err != nil || n != 1 {
		t.Fatalf("sweep to draining: n=%d err=%v", n, err)
	}
	got, _ = f.migration.GetShardMigration(f.ctx, m.ID)
	if got.Phase != migrationdomain.PhaseDraining || got.DrainDeadline.IsZero() {
		t.Fatalf("phase after cutover = %+v", got)
	}
	source, err := f.topicRepo.Get(f.ctx, f.realm.ID, "orders-0")
	if err != nil || source.Consumption != topic.ConsumptionDisabled {
		t.Fatalf("source consumption = %v (%v), want DISABLED", source.Consumption, err)
	}
	target, err := f.topicRepo.Get(f.ctx, f.realm.ID, "orders-1")
	if err != nil || target.TrafficShare.Value != 100 {
		t.Fatalf("target traffic_share = %+v (%v), want 100%% (sole ENABLED shard)", target.TrafficShare, err)
	}

	// DRAINING is a no-op sweep with no early-completion signal and the
	// deadline not yet reached.
	if n, err := f.migration.Sweep(f.ctx); err != nil || n != 0 {
		t.Fatalf("sweep mid-drain: n=%d err=%v", n, err)
	}

	// Force the deadline so the safety-net path fires without a real 1h wait.
	if _, err := f.migRepo.Mutate(f.ctx, f.realm.ID, m.ID, func(sm *migrationdomain.ShardMigration) error {
		sm.DrainDeadline = time.Now().Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatalf("force deadline: %v", err)
	}

	// DRAINING → RETIRING → DONE: the source shard is deleted through the
	// normal delete path.
	if n, err := f.migration.Sweep(f.ctx); err != nil || n != 1 {
		t.Fatalf("sweep to retiring: n=%d err=%v", n, err)
	}
	if n, err := f.migration.Sweep(f.ctx); err != nil || n != 1 {
		t.Fatalf("sweep to done: n=%d err=%v", n, err)
	}
	got, _ = f.migration.GetShardMigration(f.ctx, m.ID)
	if got.Phase != migrationdomain.PhaseDone || got.CompletedAt.IsZero() {
		t.Fatalf("final state = %+v", got)
	}
	source, err = f.topicRepo.Get(f.ctx, f.realm.ID, "orders-0")
	if err != nil || source.State != topic.StateDeleted {
		t.Fatalf("source state = %v (%v), want DELETED", source.State, err)
	}

	// A resumed sweep over an already-DONE migration is a pure no-op — 18.9's
	// "idempotent, resumable" requirement.
	if n, err := f.migration.Sweep(f.ctx); err != nil || n != 0 {
		t.Fatalf("sweep after done: n=%d err=%v", n, err)
	}
}

func TestMigrateKafkaTopicRejectsSameCluster(t *testing.T) {
	f := newMigrationFixture(t)
	f.createCluster(t, "cluster-a")
	f.createChannel(t, "orders", 1)
	shard, _ := f.topicRepo.Get(f.ctx, f.realm.ID, "orders-0")

	if _, err := f.migration.MigrateKafkaTopic(f.ctx, "orders-0", shard.ClusterName); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v", err)
	}
}

func TestMigrateKafkaTopicRejectsIneligibleTarget(t *testing.T) {
	f := newMigrationFixture(t)
	f.createCluster(t, "cluster-a")
	f.createChannel(t, "orders", 1)
	// staging does not match the channel's env=prod affinity.
	if _, err := f.clusters.Create(f.ctx, in.CreateClusterInput{
		Name: "staging-1", ConnectionStrings: plain("staging-1:9092"),
		Labels: map[string]string{"env": "staging"},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := f.migration.MigrateKafkaTopic(f.ctx, "orders-0", "staging-1"); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v", err)
	}
}

func TestMigrateClusterSkipsShardsWithNoEligibleTarget(t *testing.T) {
	f := newMigrationFixture(t)
	f.createCluster(t, "cluster-a")
	f.createChannel(t, "orders", 1)

	migrations, err := f.migration.MigrateCluster(f.ctx, "cluster-a", "")
	if err != nil {
		t.Fatalf("MigrateCluster: %v", err)
	}
	if len(migrations) != 0 {
		t.Fatalf("migrations = %+v, want none (no other eligible cluster)", migrations)
	}
}

func TestMigrateClusterMovesEveryLiveShard(t *testing.T) {
	f := newMigrationFixture(t)
	f.createCluster(t, "cluster-a")
	f.createCluster(t, "cluster-b")
	f.createChannel(t, "orders", 2)

	migrations, err := f.migration.MigrateCluster(f.ctx, "cluster-a", migrationdomain.ReasonDrainTaint)
	if err != nil {
		t.Fatalf("MigrateCluster: %v", err)
	}
	// Both shards placed on cluster-a (round-robin over one candidate) move;
	// any already on cluster-b are left alone since they are not the drained
	// cluster's shards.
	for _, m := range migrations {
		if m.Reason != migrationdomain.ReasonDrainTaint {
			t.Errorf("reason = %q, want %q", m.Reason, migrationdomain.ReasonDrainTaint)
		}
		if m.SourceClusterName != "cluster-a" || m.TargetClusterName != "cluster-b" {
			t.Errorf("migration = %+v, want cluster-a -> cluster-b", m)
		}
	}
}

// TestClusterDrainTaintAutoTriggersMigration is 18.4's drain-taint trigger:
// labelling a cluster franz.taint=drain starts moving every live shard off it
// automatically, without an explicit MigrateCluster call.
func TestClusterDrainTaintAutoTriggersMigration(t *testing.T) {
	f := newMigrationFixture(t)
	f.createCluster(t, "cluster-a")
	f.createCluster(t, "cluster-b")
	f.createChannel(t, "orders", 1)
	shard, _ := f.topicRepo.Get(f.ctx, f.realm.ID, "orders-0")
	drainedCluster := shard.ClusterName

	if _, err := f.clusters.Update(f.ctx, in.UpdateClusterInput{
		Name:   drainedCluster,
		Labels: &map[string]string{"env": "prod", "franz.taint": "drain-me:drain"},
	}); err != nil {
		t.Fatalf("Update (taint): %v", err)
	}

	migrations, err := f.migRepo.ListNonTerminal(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 1 || migrations[0].Reason != "drain-taint" {
		t.Fatalf("migrations = %+v, want one with reason drain-taint", migrations)
	}

	// A second, unrelated label edit while already drain-tainted must not
	// start a duplicate migration for the same shard.
	if _, err := f.clusters.Update(f.ctx, in.UpdateClusterInput{
		Name:   drainedCluster,
		Labels: &map[string]string{"env": "prod", "franz.taint": "drain-me:drain", "team": "infra"},
	}); err != nil {
		t.Fatalf("Update (relabel while tainted): %v", err)
	}
	migrations, err = f.migRepo.ListNonTerminal(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 1 {
		t.Fatalf("migrations after second label edit = %d, want still 1", len(migrations))
	}
}
