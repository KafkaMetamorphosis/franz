package telemetry

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
)

// fakeMigrationAdmin implements only what observeMigrationSignals calls;
// everything else panics if reached, so a test that hits an unexpected method
// fails loudly instead of silently returning zero values.
type fakeMigrationAdmin struct {
	kafkaadmin.Admin
	offsets      []kafkaadmin.PartitionOffsets
	offsetsErr   error
	groupOffsets map[string][]int32 // group -> partitions it has committed to
	groupErr     error
}

func (f *fakeMigrationAdmin) ListOffsets(context.Context, string) ([]kafkaadmin.PartitionOffsets, error) {
	return f.offsets, f.offsetsErr
}

func (f *fakeMigrationAdmin) ListConsumerGroupOffsets(_ context.Context, group, _ string) ([]int32, error) {
	if f.groupErr != nil {
		return nil, f.groupErr
	}
	return f.groupOffsets[group], nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestObserveMigrationSignalsDrainedWhenNoDataAnywhere(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets: []kafkaadmin.PartitionOffsets{{Partition: 0, Earliest: 10, Latest: 10}},
	}
	s := &Sweeper{log: discardLogger()}
	drained, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0", nil)
	if !drained {
		t.Error("want drained (no partition has data)")
	}
	if connected {
		t.Error("want not connected (no groups)")
	}
}

func TestObserveMigrationSignalsNotDrainedWhenAnyPartitionHasData(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets: []kafkaadmin.PartitionOffsets{
			{Partition: 0, Earliest: 10, Latest: 10},
			{Partition: 1, Earliest: 5, Latest: 42}, // still has data
		},
	}
	s := &Sweeper{log: discardLogger()}
	drained, _ := s.observeMigrationSignals(context.Background(), admin, "orders-0", nil)
	if drained {
		t.Error("want not drained (partition 1 still has data)")
	}
}

func TestObserveMigrationSignalsConnectedWhenAGroupHasCommitted(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets:      []kafkaadmin.PartitionOffsets{{Partition: 0, Earliest: 0, Latest: 0}},
		groupOffsets: map[string][]int32{"billing.orders-0": {0, 1}},
	}
	s := &Sweeper{log: discardLogger()}
	_, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0",
		[]string{"billing.orders-0", "unrelated-group"})
	if !connected {
		t.Error("want connected (billing.orders-0 has committed offsets)")
	}
}

func TestObserveMigrationSignalsNotConnectedWhenNoGroupCommitted(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets:      []kafkaadmin.PartitionOffsets{{Partition: 0, Earliest: 0, Latest: 0}},
		groupOffsets: map[string][]int32{}, // every group returns empty for this topic
	}
	s := &Sweeper{log: discardLogger()}
	_, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0",
		[]string{"some-other-groups-topic-consumer"})
	if connected {
		t.Error("want not connected (no group has committed to this topic)")
	}
}

func TestObserveMigrationSignalsUnknownOnListOffsetsError(t *testing.T) {
	admin := &fakeMigrationAdmin{offsetsErr: context.DeadlineExceeded}
	s := &Sweeper{log: discardLogger()}
	drained, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0", nil)
	if drained || connected {
		t.Errorf("drained=%v connected=%v, want both false on error (conservative)", drained, connected)
	}
}

// --- cluster-scoped indicators without placement --------------------------
//
// A cluster's shape is a property of the cluster, not of whatever is placed on
// it. These pin that an in-scope cluster reports its 005 §2.1 cluster-level
// indicators with zero assignments — the gap that made every kafka.cluster.*
// indicator read STALE on a cluster whose shards had not been placed yet.

// fakeWorld is a Sweeper world assembled directly, so a test can hold a cluster
// in scope without a reconciler or a placed partition.
type fakeWorld struct {
	partitions []assign.Assignment
	clusters   map[string]string
	admins     map[string]kafkaadmin.Admin
	adminCalls int
}

func (w *fakeWorld) Partitions() []assign.Assignment { return w.partitions }
func (w *fakeWorld) Clusters() map[string]string     { return w.clusters }

func (w *fakeWorld) Admins(context.Context) map[string]kafkaadmin.Admin {
	w.adminCalls++
	return w.admins
}

// capture records every published batch.
type capture struct {
	batches [][]Sample
	err     error
}

func (c *capture) Publish(_ context.Context, samples []Sample) error {
	c.batches = append(c.batches, samples)
	return c.err
}

func (c *capture) all() []Sample {
	var out []Sample
	for _, b := range c.batches {
		out = append(out, b...)
	}
	return out
}

func find(samples []Sample, indicator, resourceFRN string) (Sample, bool) {
	for _, s := range samples {
		if s.Indicator == indicator && s.ResourceFRN == resourceFRN {
			return s, true
		}
	}
	return Sample{}, false
}

// inScopeSweeper holds one cluster in scope with the given broker shape and no
// assignments at all.
func inScopeSweeper(state kafkaadmin.Cluster) (*Sweeper, *capture) {
	broker := kafkaadmin.NewMem()
	broker.ClusterState = state
	world := &fakeWorld{
		clusters: map[string]string{"local-1": "frn:default:kafka-cluster:local-1"},
		admins:   map[string]kafkaadmin.Admin{"local-1": broker},
	}
	pub := &capture{}
	return NewSweeper(world, pub, time.Minute, discardLogger()), pub
}

func TestSweepReportsClusterIndicatorsWithNothingPlaced(t *testing.T) {
	sweeper, pub := inScopeSweeper(kafkaadmin.Cluster{
		BrokerCount: 1, OnlineBrokerCount: 1, ControllerID: 1,
		TotalPartitionReplicas: 12,
		ReplicasPerBroker:      map[int32]int32{1: 12},
		LeadersPerBroker:       map[int32]int32{1: 12},
	})

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	samples := pub.all()
	if len(samples) == 0 {
		t.Fatal("no samples published for an in-scope cluster with no assignments")
	}

	const clusterFRN = "frn:default:kafka-cluster:local-1"
	for _, tc := range []struct{ indicator, frn, want string }{
		{IndicatorClusterBrokerCount, clusterFRN, "1"},
		{IndicatorClusterOnlineBrokers, clusterFRN, "1"},
		{IndicatorClusterControllerID, clusterFRN, "1"},
		{IndicatorClusterTotalReplicas, clusterFRN, "12"},
		{IndicatorClusterUnderReplicated, clusterFRN, "0"},
		{IndicatorClusterOfflinePartns, clusterFRN, "0"},
		// Per-broker counts hang off a broker sub-resource (005 OQ6).
		{IndicatorClusterReplicasPerBrkr, clusterFRN + "/broker/1", "12"},
		{IndicatorClusterLeadersPerBrkr, clusterFRN + "/broker/1", "12"},
	} {
		got, ok := find(samples, tc.indicator, tc.frn)
		if !ok {
			t.Errorf("%s not reported for %s", tc.indicator, tc.frn)
			continue
		}
		if got.Value != tc.want {
			t.Errorf("%s = %q, want %q", tc.indicator, got.Value, tc.want)
		}
		if got.ResourceEntity != EntityKafkaCluster {
			t.Errorf("%s entity = %q, want KAFKA_CLUSTER", tc.indicator, got.ResourceEntity)
		}
	}

	// Nothing is placed, so no topic-scoped sample may appear.
	for _, s := range samples {
		if s.ResourceEntity == EntityKafkaTopic {
			t.Errorf("topic-scoped sample %q with no assignments", s.Indicator)
		}
	}
}

// TestSweepReportsZeroesForAnEmptyCluster is the "empty but valid" case: a
// freshly registered cluster with no topics reports 0, rather than not sampling
// and reading STALE.
func TestSweepReportsZeroesForAnEmptyCluster(t *testing.T) {
	sweeper, pub := inScopeSweeper(kafkaadmin.Cluster{
		BrokerCount: 1, OnlineBrokerCount: 1, ControllerID: 1,
		TotalPartitionReplicas: 0,
		ReplicasPerBroker:      map[int32]int32{1: 0},
		LeadersPerBroker:       map[int32]int32{1: 0},
	})

	if err := sweeper.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	const clusterFRN = "frn:default:kafka-cluster:local-1"
	got, ok := find(pub.all(), IndicatorClusterTotalReplicas, clusterFRN)
	if !ok {
		t.Fatal("an empty cluster must still report total_partition_replicas")
	}
	if got.Value != "0" {
		t.Errorf("total_partition_replicas = %q, want \"0\"", got.Value)
	}
	if perBroker, ok := find(pub.all(), IndicatorClusterReplicasPerBrkr, clusterFRN+"/broker/1"); !ok {
		t.Error("an empty cluster must still report replicas_per_broker for its brokers")
	} else if perBroker.Value != "0" {
		t.Errorf("replicas_per_broker = %q, want \"0\"", perBroker.Value)
	}
}

// TestSweepSkipsAClusterWithNoFRN keeps the guard that an unidentifiable cluster
// is skipped rather than published under an empty resource.
func TestSweepSkipsAClusterWithNoFRN(t *testing.T) {
	broker := kafkaadmin.NewMem()
	broker.ClusterState = kafkaadmin.Cluster{BrokerCount: 1}
	world := &fakeWorld{
		clusters: map[string]string{"local-1": ""},
		admins:   map[string]kafkaadmin.Admin{"local-1": broker},
	}
	pub := &capture{}
	s := NewSweeper(world, pub, time.Minute, discardLogger())

	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(pub.batches) != 0 {
		t.Errorf("published %d batches for a cluster with no FRN, want 0", len(pub.batches))
	}
}

// TestSweepPublishesNothingWithNoClustersInScope is the counterpart: an agent
// whose selectors match nothing must stay silent rather than publish an empty
// batch.
func TestSweepPublishesNothingWithNoClustersInScope(t *testing.T) {
	world := &fakeWorld{}
	pub := &capture{}
	s := NewSweeper(world, pub, time.Minute, discardLogger())

	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(pub.batches) != 0 {
		t.Errorf("published %d batches with nothing in scope, want 0", len(pub.batches))
	}
	if world.adminCalls != 1 {
		t.Errorf("Admins called %d times, want 1 — the sweep must ask the world to connect in-scope clusters", world.adminCalls)
	}
}

// TestSweepUsesTheAssignmentFRNWhenScopeHasNone guards the overlay: a world
// built from assignments alone (a reconnect where scope has not landed yet)
// still reports its cluster.
func TestSweepUsesTheAssignmentFRNWhenScopeHasNone(t *testing.T) {
	broker := kafkaadmin.NewMem()
	broker.ClusterState = kafkaadmin.Cluster{BrokerCount: 2, OnlineBrokerCount: 2}
	world := &fakeWorld{
		partitions: []assign.Assignment{{
			Change: assign.ChangeSet, PartitionFRN: "frn:default:kafka-topic:orders-0",
			TopicName: "orders-0", ClusterName: "local-1",
			ClusterFRN: "frn:default:kafka-cluster:local-1",
		}},
		admins: map[string]kafkaadmin.Admin{"local-1": broker},
	}
	pub := &capture{}
	s := NewSweeper(world, pub, time.Minute, discardLogger())

	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, ok := find(pub.all(), IndicatorClusterBrokerCount,
		"frn:default:kafka-cluster:local-1"); !ok || got.Value != "2" {
		t.Errorf("broker_count from an assignment-only world = %+v (found=%v)", got, ok)
	}
}
