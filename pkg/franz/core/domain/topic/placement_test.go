package topic

import (
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// partitions / replication-factor / kafka-version are Franz vocabulary that a
// cluster's cluster_configuration may carry (ADR-API-010) but are not real Kafka
// topic-config keys — a broker rejects createTopics with INVALID_CONFIG if any
// is passed as topic config (003.6). Materialize drops them from both layers.
func TestMaterializeDropsNonTopicConfigKeys(t *testing.T) {
	merged := Materialize(map[string]string{
		ConfigKeyPartitions:        "6",
		ConfigKeyReplicationFactor: "3",
		ConfigKeyKafkaVersion:      "3.9.0",
		"retention.ms":             "60000",
	}, map[string]string{
		"cleanup.policy":      "compact",
		ConfigKeyKafkaVersion: "4.0.0", // even if it somehow reaches the shard layer
	})

	want := map[string]string{"retention.ms": "60000", "cleanup.policy": "compact"}
	if !mapEq(merged, want) {
		t.Fatalf("materialized = %v, want %v", merged, want)
	}
}

func TestSeedPartitionsAndReplicationFactor(t *testing.T) {
	cases := []struct {
		name          string
		config        map[string]string
		wantPartition int32
		wantRF        int32
	}{
		{"seeded", map[string]string{"partitions": "6", "replication-factor": "3"}, 6, 3},
		{"absent", nil, 1, 1},
		{"blank", map[string]string{"partitions": "  ", "replication-factor": ""}, 1, 1},
		{"unparseable", map[string]string{"partitions": "lots", "replication-factor": "-2"}, 1, 1},
		{"padded", map[string]string{"partitions": " 4 "}, 4, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SeedPartitions(tc.config, 1); got != tc.wantPartition {
				t.Errorf("partitions = %d, want %d", got, tc.wantPartition)
			}
			if got := SeedReplicationFactor(tc.config, 1); got != tc.wantRF {
				t.Errorf("replication factor = %d, want %d", got, tc.wantRF)
			}
		})
	}
}

func TestMisplacedMarkerDoesNotMoveOrBumpGeneration(t *testing.T) {
	shard, err := New(testRealm(), uuid.New(), "orders", 0, nil, nil, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	clusterID := uuid.New()
	if err := shard.PlaceOn(clusterID, "east-1"); err != nil {
		t.Fatal(err)
	}
	generation := shard.Generation

	if !shard.MarkMisplaced("cluster east-1 is PAUSED") {
		t.Fatal("MarkMisplaced reported no change on a freshly placed shard")
	}
	if shard.MarkMisplaced("cluster east-1 is PAUSED") {
		t.Error("MarkMisplaced is not idempotent")
	}
	if shard.KafkaClusterID == nil || *shard.KafkaClusterID != clusterID {
		t.Error("the marker moved the shard's cluster")
	}
	if shard.Generation != generation {
		t.Errorf("generation = %d, want %d — a marker is not a desired-state change",
			shard.Generation, generation)
	}

	if !shard.ClearMisplaced() {
		t.Fatal("ClearMisplaced reported no change on a misplaced shard")
	}
	if shard.Misplaced || shard.MisplacedReason != "" {
		t.Errorf("marker survived the clear: %v / %q", shard.Misplaced, shard.MisplacedReason)
	}
	if shard.ClearMisplaced() {
		t.Error("ClearMisplaced is not idempotent")
	}
}

func TestAdoptPlacementCompletesAnUnplacedRow(t *testing.T) {
	shard, err := New(testRealm(), uuid.New(), "orders", 0,
		map[string]string{"retention.ms": "1", ConfigKeyKafkaVersion: "3.9.0"}, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	shard.KafkaClusterID = nil // an ADR-API-009 anomaly: a row with no cluster
	shard.MarkMisplaced("no eligible cluster")
	gen := shard.Generation

	clusterID := uuid.New()
	if err := shard.AdoptPlacement(clusterID, "east-1",
		map[string]string{"retention.ms": "60000", ConfigKeyPartitions: "6", ConfigKeyKafkaVersion: "4.0.0"},
		6, 3); err != nil {
		t.Fatal(err)
	}
	if shard.KafkaClusterID == nil || *shard.KafkaClusterID != clusterID || shard.ClusterName != "east-1" {
		t.Fatalf("not placed: %+v", shard)
	}
	if shard.Partitions != 6 || shard.ReplicationFactor != 3 {
		t.Errorf("shape not seeded: %d/%d", shard.Partitions, shard.ReplicationFactor)
	}
	if !mapEq(shard.MaterializedConfiguration, map[string]string{"retention.ms": "60000"}) {
		t.Errorf("config not re-materialised (or a seed key leaked): %v", shard.MaterializedConfiguration)
	}
	if shard.Misplaced || shard.MisplacedReason != "" {
		t.Error("misplaced marker survived adoption")
	}
	if shard.Generation != gen+1 {
		t.Errorf("generation = %d, want %d", shard.Generation, gen+1)
	}

	if err := shard.AdoptPlacement(uuid.New(), "west-1", nil, 1, 1); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("adopt on an already-placed shard = %v, want FAILED_PRECONDITION", err)
	}
}

// A placed shard is only ever moved by the migration flow (003.13), so PlaceOn
// refuses to re-point one.
func TestPlaceOnRejectsAnAlreadyPlacedShard(t *testing.T) {
	shard, err := New(testRealm(), uuid.New(), "orders", 0, nil, nil, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := shard.PlaceOn(uuid.New(), "east-1"); err != nil {
		t.Fatal(err)
	}
	if err := shard.PlaceOn(uuid.New(), "west-1"); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("second PlaceOn = %v, want FAILED_PRECONDITION", err)
	}
	if shard.ClusterName != "east-1" {
		t.Errorf("cluster name = %q, want east-1", shard.ClusterName)
	}
}
