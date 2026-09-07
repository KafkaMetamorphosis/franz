package placement_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	placementdomain "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/placement"
)

func testRealm() realm.Realm {
	return realm.Realm{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Slug: "default"}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// --- fakes ---------------------------------------------------------------

type fakeChannels struct {
	rows []*channel.AsyncChannel
	out.AsyncChannelRepository
}

func (f *fakeChannels) Get(_ context.Context, _ uuid.UUID, name string) (*channel.AsyncChannel, error) {
	for _, c := range f.rows {
		if c.Name == name {
			return c, nil
		}
	}
	panic("unexpected channel " + name)
}

func (f *fakeChannels) ListActive(context.Context, uuid.UUID) ([]*channel.AsyncChannel, error) {
	return f.rows, nil
}

func (f *fakeChannels) ListUnderplaced(context.Context) ([]*channel.AsyncChannel, error) {
	return f.rows, nil
}

type fakeClusters struct {
	rows []*cluster.Cluster
	out.ClusterRepository
}

func (f *fakeClusters) ListAll(context.Context, uuid.UUID) ([]*cluster.Cluster, error) {
	return f.rows, nil
}

type fakeRealms struct{ out.RealmRepository }

func (fakeRealms) GetByID(context.Context, uuid.UUID) (realm.Realm, error) {
	return testRealm(), nil
}

// fakeTopics keeps the channel's shard rows in memory and applies a ShardPlan
// exactly as the Postgres transaction does.
type fakeTopics struct {
	rows []*topic.KafkaTopic
	out.TopicRepository
}

func (f *fakeTopics) PlaceChannelShards(
	_ context.Context, _, _ uuid.UUID,
	plan func([]*topic.KafkaTopic) (out.ShardPlan, error),
) ([]*topic.KafkaTopic, error) {
	shardPlan, err := plan(f.rows)
	if err != nil {
		return nil, err
	}
	f.rows = append(f.rows, shardPlan.Create...)
	return append(append([]*topic.KafkaTopic{}, shardPlan.Create...), shardPlan.Update...), nil
}

type recordingNotifier struct {
	batches [][]*topic.KafkaTopic
}

func (r *recordingNotifier) ShardsChanged(_ context.Context, _ uuid.UUID, shards []*topic.KafkaTopic) {
	r.batches = append(r.batches, shards)
}

func (r *recordingNotifier) ClusterLabelsChanged(context.Context, uuid.UUID, string, map[string]string, map[string]string) {
}

func (r *recordingNotifier) AgentSelectorChanged(context.Context, uuid.UUID, string, map[string]string, map[string]string) {
}

// --- fixtures ------------------------------------------------------------

func activeChannel(name string, shards int32, labels map[string]string) *channel.AsyncChannel {
	return &channel.AsyncChannel{
		ID: uuid.New(), RealmID: testRealm().ID, Name: name,
		Type: channel.TypeKafkaTopic, ChannelPartitions: shards,
		Labels: labels, State: channel.StateActive,
	}
}

func activeCluster(name string, labels, config map[string]string) *cluster.Cluster {
	return &cluster.Cluster{
		ID: uuid.New(), Name: name, Labels: labels,
		Configuration: config, State: cluster.StateActive,
	}
}

func newFixture(
	channels []*channel.AsyncChannel, clusters []*cluster.Cluster,
) (*placement.Service, *fakeTopics, *recordingNotifier) {
	topics := &fakeTopics{}
	notifier := &recordingNotifier{}
	svc := placement.NewService(
		&fakeChannels{rows: channels}, &fakeClusters{rows: clusters},
		topics, fakeRealms{}, notifier, quietLogger())
	return svc, topics, notifier
}

// --- tests ---------------------------------------------------------------

func TestPlaceMaterialisesShardsAndSeedsTheKafkaShape(t *testing.T) {
	c := activeChannel("billing-events", 3, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	east := activeCluster("east-1", map[string]string{"env": "prod"}, map[string]string{
		"partitions": "6", "replication-factor": "3", "retention.ms": "604800000",
	})
	svc, topics, notifier := newFixture([]*channel.AsyncChannel{c}, []*cluster.Cluster{east})

	created, err := svc.Place(context.Background(), testRealm(), c)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if created != 3 {
		t.Fatalf("created = %d, want 3", created)
	}
	if len(topics.rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(topics.rows))
	}
	for index, shard := range topics.rows {
		if shard.Name != c.ShardName(index) {
			t.Errorf("shard %d name = %q", index, shard.Name)
		}
		if shard.KafkaClusterID == nil || *shard.KafkaClusterID != east.ID {
			t.Errorf("shard %d is not placed on east-1", index)
		}
		if shard.ClusterName != "east-1" || shard.ChannelName != "billing-events" {
			t.Errorf("shard %d projections = %q / %q", index, shard.ClusterName, shard.ChannelName)
		}
		if shard.Partitions != 6 || shard.ReplicationFactor != 3 {
			t.Errorf("shard %d shape = %d/%d, want 6/3",
				index, shard.Partitions, shard.ReplicationFactor)
		}
		if shard.State != topic.StatePending {
			t.Errorf("shard %d state = %s, want PENDING", index, shard.State)
		}
		if got := shard.MaterializedConfiguration; len(got) != 1 || got["retention.ms"] != "604800000" {
			t.Errorf("shard %d materialized config = %v", index, got)
		}
	}
	if len(notifier.batches) != 1 || len(notifier.batches[0]) != 3 {
		t.Fatalf("the Resource Provider was not told about the new shards: %v", notifier.batches)
	}

	// A second pass is a no-op: an already-placed shard is left alone.
	created, err = svc.Place(context.Background(), testRealm(), c)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 || len(topics.rows) != 3 {
		t.Fatalf("re-running placement created %d more rows (total %d)", created, len(topics.rows))
	}
	if len(notifier.batches) != 1 {
		t.Errorf("an idempotent pass still notified: %v", notifier.batches)
	}
}

func TestPlaceCreatesNothingWithoutAnEligibleCluster(t *testing.T) {
	noSelector := activeChannel("orders", 2, map[string]string{"my-fleet/tier": "high"})
	unmatched := activeChannel("payments", 2, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	staging := activeCluster("s-1", map[string]string{"env": "staging"}, nil)

	for _, c := range []*channel.AsyncChannel{noSelector, unmatched} {
		svc, topics, notifier := newFixture([]*channel.AsyncChannel{c}, []*cluster.Cluster{staging})
		created, err := svc.Place(context.Background(), testRealm(), c)
		if err != nil {
			t.Fatalf("%s: Place must not fail: %v", c.Name, err)
		}
		if created != 0 || len(topics.rows) != 0 {
			t.Fatalf("%s: created %d rows, want 0 (ADR-API-009)", c.Name, created)
		}
		if len(notifier.batches) != 0 {
			t.Errorf("%s: notified with nothing placed", c.Name)
		}
	}
}

func TestPlaceMarksMisplacedWithoutMoving(t *testing.T) {
	c := activeChannel("billing-events", 2, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	east := activeCluster("east-1", map[string]string{"env": "prod"}, nil)
	svc, topics, notifier := newFixture([]*channel.AsyncChannel{c}, []*cluster.Cluster{east})

	if _, err := svc.Place(context.Background(), testRealm(), c); err != nil {
		t.Fatal(err)
	}

	// The cluster is relabelled out of the channel's affinity.
	east.Labels = map[string]string{"env": "staging"}
	if _, err := svc.Place(context.Background(), testRealm(), c); err != nil {
		t.Fatal(err)
	}
	for _, shard := range topics.rows {
		if !shard.Misplaced {
			t.Fatalf("%s was not marked misplaced", shard.Name)
		}
		if shard.MisplacedReason == "" {
			t.Errorf("%s carries no reason", shard.Name)
		}
		if shard.KafkaClusterID == nil || *shard.KafkaClusterID != east.ID {
			t.Errorf("%s moved; only migration (003.13) may move a shard", shard.Name)
		}
	}
	if len(notifier.batches) != 2 {
		t.Fatalf("the misplaced marker was not announced: %v", notifier.batches)
	}

	// Relabelling it back clears the marker.
	east.Labels = map[string]string{"env": "prod"}
	if _, err := svc.Place(context.Background(), testRealm(), c); err != nil {
		t.Fatal(err)
	}
	for _, shard := range topics.rows {
		if shard.Misplaced || shard.MisplacedReason != "" {
			t.Errorf("%s stayed misplaced after the cluster matched again", shard.Name)
		}
	}
}

func TestPlaceSkipsANonActiveChannel(t *testing.T) {
	c := activeChannel("orders", 2, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	c.State = channel.StatePaused
	east := activeCluster("east-1", map[string]string{"env": "prod"}, nil)
	svc, topics, _ := newFixture([]*channel.AsyncChannel{c}, []*cluster.Cluster{east})

	created, err := svc.Place(context.Background(), testRealm(), c)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 || len(topics.rows) != 0 {
		t.Fatalf("a paused channel was placed: %d rows", len(topics.rows))
	}
}

func TestPlaceRejectsMalformedChannelLabels(t *testing.T) {
	c := activeChannel("orders", 1, map[string]string{placementdomain.LabelShardSize: "many"})
	svc, _, _ := newFixture([]*channel.AsyncChannel{c}, nil)

	if _, err := svc.Place(context.Background(), testRealm(), c); err == nil {
		t.Fatal("Place accepted a malformed franz.affinity/shard-size")
	}
}

func TestSweepPlacesEveryUnderplacedChannel(t *testing.T) {
	first := activeChannel("a-channel", 2, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	second := activeChannel("b-channel", 1, map[string]string{
		placementdomain.LabelAffinitySelector: "env=prod",
	})
	east := activeCluster("east-1", map[string]string{"env": "prod"}, nil)
	svc, _, _ := newFixture([]*channel.AsyncChannel{first, second}, []*cluster.Cluster{east})

	created, err := svc.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if created != 3 {
		t.Fatalf("sweep created %d shards, want 3", created)
	}
}
