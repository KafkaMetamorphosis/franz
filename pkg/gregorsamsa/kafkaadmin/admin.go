// Package kafkaadmin is Gregor Samsa's Kafka administrative surface: the narrow
// set of AdminClient calls the reconciler and the telemetry sweep need, behind
// an interface so both can be exercised against an in-memory fake.
//
// One Admin is held per in-scope Kafka cluster, built from the cluster's
// connection strings on the assignment and cached for the process's life
// (005 ADR "Key properties").
package kafkaadmin

import "context"

// Topic is the state of one real Kafka topic, as read off the broker.
type Topic struct {
	Name string
	// Partitions is the current partition count.
	Partitions int32
	// ReplicationFactor is the replica count of the topic's partitions. Kafka
	// allows an uneven per-partition replica count after a reassignment; this is
	// the count on partition 0, which is what a Franz-managed topic has
	// everywhere.
	ReplicationFactor int32
	// Config is the topic's effective configuration.
	Config map[string]string
	// UnderReplicatedPartitions counts partitions whose ISR is smaller than
	// their replica set.
	UnderReplicatedPartitions int32
}

// PartitionOffsets is the earliest/latest offset pair of one partition — the
// input to the "topic still holds unconsumed data" deletion safety check.
type PartitionOffsets struct {
	Partition int32
	Earliest  int64
	Latest    int64
}

// HasData reports whether the partition still holds records.
func (p PartitionOffsets) HasData() bool { return p.Earliest < p.Latest }

// Cluster is the structural state of one Kafka cluster, for the telemetry sweep
// (005 ADR §2.1 "Per cluster").
type Cluster struct {
	BrokerCount       int32
	OnlineBrokerCount int32
	ControllerID      int32

	TotalPartitionReplicas    int32
	ReplicasPerBroker         map[int32]int32
	LeadersPerBroker          map[int32]int32
	UnderReplicatedPartitions int32
	OfflinePartitions         int32
}

// Admin is the Kafka administrative surface Gregor Samsa uses. Every method is
// scoped to one cluster.
//
// Implementations must be safe for concurrent use: the reconciler serialises
// calls per cluster, but the telemetry sweep runs alongside it.
type Admin interface {
	// DescribeTopic returns the topic's current state, or (nil, nil) when the
	// topic does not exist — "absent" is an expected answer, not an error.
	DescribeTopic(ctx context.Context, topic string) (*Topic, error)

	// CreateTopic creates the topic with the given shape and configuration.
	CreateTopic(ctx context.Context, topic string, partitions, replicationFactor int32, config map[string]string) error

	// CreatePartitions raises the topic's partition count to total. Kafka has no
	// way to lower it; the caller guards that.
	CreatePartitions(ctx context.Context, topic string, total int32) error

	// AlterConfigs incrementally SETs each key. Keys absent from set are left
	// exactly as they are — Franz never resets an override it did not ask for
	// (005 ADR OQ4).
	AlterConfigs(ctx context.Context, topic string, set map[string]string) error

	// DeleteTopic deletes the topic. Deleting an absent topic is not an error.
	DeleteTopic(ctx context.Context, topic string) error

	// ListOffsets returns the earliest and latest offset of every partition.
	ListOffsets(ctx context.Context, topic string) ([]PartitionOffsets, error)

	// ListConsumerGroups returns every consumer group known to the cluster.
	ListConsumerGroups(ctx context.Context) ([]string, error)

	// ListConsumerGroupOffsets returns the partitions of topic on which group
	// holds a committed offset. Empty when the group never committed to it.
	ListConsumerGroupOffsets(ctx context.Context, group, topic string) ([]int32, error)

	// DescribeCluster returns the cluster's structural state.
	DescribeCluster(ctx context.Context) (*Cluster, error)

	// Close releases the underlying connections.
	Close()
}

// Factory opens an Admin against a cluster's bootstrap servers. Gregor Samsa
// calls it once per in-scope cluster and caches the result.
type Factory func(ctx context.Context, bootstrapServers []string) (Admin, error)
