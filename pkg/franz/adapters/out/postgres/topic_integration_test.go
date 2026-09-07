package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/topics"
)

func cleanupTopics(t *testing.T, db *postgres.DB) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM kafka_topic`,
		`DELETE FROM async_channel`,
		`DELETE FROM cluster_provider_event`,
		`DELETE FROM kafka_cluster`,
	} {
		if _, err := db.Pool().Exec(context.Background(), stmt); err != nil {
			t.Fatalf("cleanup (%s): %v", stmt, err)
		}
	}
}

// insertChannel adds a minimal async_channel row (deliverable 10 owns the real
// entity) and returns its id.
func insertChannel(t *testing.T, db *postgres.DB, r realm.Realm, name string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	f, _ := frn.New(r.Slug, frn.TypeAsyncChannel, name)
	_, err := db.Pool().Exec(context.Background(),
		`INSERT INTO async_channel (id, realm_id, name, frn) VALUES ($1,$2,$3,$4)`,
		id, r.ID, name, f.Path())
	if err != nil {
		t.Fatalf("insert channel: %v", err)
	}
	return id
}

func makeShard(
	t *testing.T, repo *postgres.TopicRepo, r realm.Realm,
	channelID uuid.UUID, channelName string, idx int,
	clusterCfg, topicCfg map[string]string, clusterID *uuid.UUID,
) *topic.KafkaTopic {
	t.Helper()
	sh, err := topic.New(r, channelID, channelName, idx, clusterCfg, topicCfg, 3, 1)
	if err != nil {
		t.Fatalf("topic.New: %v", err)
	}
	sh.KafkaClusterID = clusterID
	if err := repo.Create(context.Background(), sh); err != nil {
		t.Fatalf("Create shard %d: %v", idx, err)
	}
	return sh
}

func TestTopicRepoMaterialisedConfigFrozen(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	topicRepo := postgres.NewTopicRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)

	// a cluster with configuration, and a channel with one shard placed on it
	c, err := cluster.New(r, "east-1", plain("localhost:9092"), nil,
		map[string]string{"retention.ms": "60000", "min.insync.replicas": "1"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := clusterRepo.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	chID := insertChannel(t, db, r, "orders")
	clusterID := c.ID
	makeShard(t, topicRepo, r, chID, "orders", 0,
		c.Configuration, map[string]string{"cleanup.policy": "compact"}, &clusterID)

	// the materialised config is the merge at create time
	got, err := topicRepo.Get(ctx, r.ID, "orders-0")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"retention.ms": "60000", "min.insync.replicas": "1", "cleanup.policy": "compact",
	}
	if !sameMap(got.MaterializedConfiguration, want) {
		t.Fatalf("materialised = %v", got.MaterializedConfiguration)
	}
	if got.ChannelName != "orders" || got.ClusterName != "east-1" {
		t.Errorf("names not joined: channel=%q cluster=%q", got.ChannelName, got.ClusterName)
	}

	// editing the cluster's configuration must NOT touch the shard's frozen merge
	if _, err := clusterRepo.Mutate(ctx, r.ID, "east-1", func(cl *cluster.Cluster) error {
		cl.Configuration = map[string]string{"retention.ms": "1", "cleanup.policy": "delete"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, _ = topicRepo.Get(ctx, r.ID, "orders-0")
	if !sameMap(got.MaterializedConfiguration, want) {
		t.Fatalf("materialised config drifted after cluster edit: %v", got.MaterializedConfiguration)
	}
}

func TestTopicServiceSetConsumptionRenormalises(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	topicRepo := postgres.NewTopicRepo(db)
	svc := topics.NewService(topicRepo, postgres.NewClusterRepo(db), nil)

	chID := insertChannel(t, db, r, "orders")
	for i := 0; i < 4; i++ {
		makeShard(t, topicRepo, r, chID, "orders", i, nil, nil, nil)
	}

	// all 4 ENABLED → 25% each after the first SetConsumption call re-normalises
	sh, err := svc.SetConsumption(ctx, "orders-0", topic.ConsumptionEnabled)
	if err != nil {
		t.Fatal(err)
	}
	_ = sh
	assertShares(t, topicRepo, ctx, r, map[string]float64{
		"orders-0": 25, "orders-1": 25, "orders-2": 25, "orders-3": 25,
	})

	// drain orders-2 → siblings 33.33…, orders-2 = 0
	if _, err := svc.SetConsumption(ctx, "orders-2", topic.ConsumptionDisabled); err != nil {
		t.Fatal(err)
	}
	got, _ := topicRepo.Get(ctx, r.ID, "orders-2")
	if got.Consumption != topic.ConsumptionDisabled || got.TrafficShare.Value != 0 {
		t.Errorf("orders-2: consumption=%s share=%v", got.Consumption, got.TrafficShare.Value)
	}
	for _, n := range []string{"orders-0", "orders-1", "orders-3"} {
		g, _ := topicRepo.Get(ctx, r.ID, n)
		if g.TrafficShare.Value < 33.33 || g.TrafficShare.Value > 33.34 {
			t.Errorf("%s share = %v, want ~33.33", n, g.TrafficShare.Value)
		}
	}

	// restore → back to 25 each
	if _, err := svc.SetConsumption(ctx, "orders-2", topic.ConsumptionEnabled); err != nil {
		t.Fatal(err)
	}
	assertShares(t, topicRepo, ctx, r, map[string]float64{
		"orders-0": 25, "orders-1": 25, "orders-2": 25, "orders-3": 25,
	})
}

func TestTopicRepoListFiltersAndPagination(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	topicRepo := postgres.NewTopicRepo(db)

	a := insertChannel(t, db, r, "alpha")
	b := insertChannel(t, db, r, "beta")
	for i := 0; i < 3; i++ {
		makeShard(t, topicRepo, r, a, "alpha", i, nil, nil, nil)
	}
	makeShard(t, topicRepo, r, b, "beta", 0, nil, nil, nil)

	all, err := topicRepo.List(ctx, out.TopicQuery{RealmID: r.ID, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Topics) != 2 || all.LastName != "alpha-1" {
		t.Fatalf("page 1: %d rows, last=%q", len(all.Topics), all.LastName)
	}
	page2, _ := topicRepo.List(ctx, out.TopicQuery{RealmID: r.ID, Limit: 2, AfterName: all.LastName})
	if len(page2.Topics) != 2 || page2.LastName != "" {
		t.Fatalf("page 2: %d rows, last=%q", len(page2.Topics), page2.LastName)
	}

	byChannel, _ := topicRepo.List(ctx, out.TopicQuery{RealmID: r.ID, AsyncChannelID: &b, Limit: 50})
	if len(byChannel.Topics) != 1 || byChannel.Topics[0].Name != "beta-0" {
		t.Fatalf("channel filter: %+v", byChannel.Topics)
	}

	// soft-deleted rows are hidden
	if _, err := topicRepo.MutateChannelShards(ctx, r.ID, a, func(shards []*topic.KafkaTopic) error {
		return shards[0].SetState(topic.StateDeleted)
	}); err != nil {
		t.Fatal(err)
	}
	afterDelete, _ := topicRepo.List(ctx, out.TopicQuery{RealmID: r.ID, AsyncChannelID: &a, Limit: 50})
	if len(afterDelete.Topics) != 2 {
		t.Fatalf("deleted shard still listed: %d", len(afterDelete.Topics))
	}
}

func TestTopicGuardBlocksClusterDelete(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	topicRepo := postgres.NewTopicRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)

	c, _ := cluster.New(r, "east-1", plain("localhost:9092"), nil, nil, "")
	if err := clusterRepo.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	ch := insertChannel(t, db, r, "orders")
	clusterID := c.ID
	makeShard(t, topicRepo, r, ch, "orders", 0, nil, nil, &clusterID)

	// guard sees one live topic
	if n, _ := topicRepo.CountLiveTopics(ctx, c.ID); n != 1 {
		t.Fatalf("CountLiveTopics = %d, want 1", n)
	}

	// DeleteKafkaCluster refuses while a topic lives on it (003.3 done-when)
	var _ out.ClusterTopicGuard = topicRepo
	svc := clusters.NewService(clusterRepo, topicRepo, postgres.NewProviderEventRepo(db), streamhub.New(), nil)
	if err := svc.Delete(ctx, "east-1"); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("Delete with live topic → %v, want FAILED_PRECONDITION", err)
	}

	// drain + delete the shard, then the cluster deletes cleanly
	if _, err := topicRepo.MutateChannelShards(ctx, r.ID, ch, func(shards []*topic.KafkaTopic) error {
		return shards[0].SetState(topic.StateDeleted)
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(ctx, "east-1"); err != nil {
		t.Fatalf("Delete after drain: %v", err)
	}
}

func TestTopicRepoPartitionDecreaseRejected(t *testing.T) {
	db := openTestDB(t)
	cleanupTopics(t, db)
	r := seededRealm(t, db)
	ctx := realm.NewContext(context.Background(), r)
	topicRepo := postgres.NewTopicRepo(db)

	ch := insertChannel(t, db, r, "orders")
	makeShard(t, topicRepo, r, ch, "orders", 0, nil, nil, nil) // partitions 3

	_, err := topicRepo.MutateChannelShards(ctx, r.ID, ch, func(shards []*topic.KafkaTopic) error {
		return shards[0].IncreasePartitions(2)
	})
	if errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("partition decrease → %v, want INVALID_ARGUMENT", err)
	}
	// the row is unchanged (transaction rolled back)
	got, _ := topicRepo.Get(ctx, r.ID, "orders-0")
	if got.Partitions != 3 {
		t.Errorf("partitions = %d after rolled-back decrease", got.Partitions)
	}
}

func assertShares(t *testing.T, repo *postgres.TopicRepo, ctx context.Context, r realm.Realm, want map[string]float64) {
	t.Helper()
	for name, w := range want {
		g, err := repo.Get(ctx, r.ID, name)
		if err != nil {
			t.Fatalf("Get %s: %v", name, err)
		}
		if g.TrafficShare.Value != w {
			t.Errorf("%s traffic share = %v, want %v", name, g.TrafficShare.Value, w)
		}
	}
}

func sameMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
