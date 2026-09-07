// Package resource is the Resource Provider interaction domain (005 ADR): the
// desired state of one async channel partition, pushed to the label-scoped agent
// over WatchPartitionAssignments. It is a leaf package — no dependency on the
// topic or cluster entities, so both can be mapped onto it without a cycle.
package resource

import (
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
)

// Change is how an assignment changed for the in-scope agent (005 ADR §1.3).
type Change string

const (
	// ChangeSet — created or spec-changed; reconcile the Kafka topic to this.
	ChangeSet Change = "SET"
	// ChangePaused — owning channel paused; stop managing, leave the topic.
	ChangePaused Change = "PAUSED"
	// ChangeRemoved — partition deleted; delete the topic (safety-checked)
	// unless Reason says otherwise.
	ChangeRemoved Change = "REMOVED"
)

// Reason qualifies a ChangeRemoved. The zero value is an ordinary delete.
type Reason string

// ReasonScopeLoss marks a REMOVED emitted because the cluster left the agent's
// label scope. The agent drops the partition and does nothing to Kafka; whichever
// agent now matches picks it up as SET (005 ADR §1.4 "Scope loss").
const ReasonScopeLoss Reason = "SCOPE_LOSS"

// ConnectionString mirrors a cluster connection string for the agent's
// AdminClient (kept local so this package stays a leaf).
type ConnectionString struct {
	BootstrapURLs []string
	Type          string
}

// PartitionAssignment is the desired state of one async channel partition — the
// Franz-side record for exactly one real Kafka topic (003.6).
//
// PAUSED and REMOVED assignments carry only Change, Reason, PartitionFRN,
// Generation, TopicName, ClusterName and ConnectionStrings; the agent needs no
// desired configuration to stop managing or to delete.
type PartitionAssignment struct {
	Change Change
	Reason Reason

	PartitionFRN frn.FRN
	Generation   int64
	AsyncChannel string
	TopicName    string

	ClusterName       string
	ClusterFRN        frn.FRN
	ConnectionStrings []ConnectionString

	// DesiredConfig is the partition's materialized_configuration, frozen at
	// placement (003.6). The agent applies exactly these keys and leaves keys
	// Franz did not specify untouched.
	DesiredConfig     map[string]string
	Partitions        int32
	ReplicationFactor int32
}

// Removed builds the REMOVED assignment for a partition that left the agent's
// scope. It deliberately carries no desired state — the agent must not act on it.
func Removed(
	partitionFRN frn.FRN, generation int64, topicName, clusterName string,
	clusterFRN frn.FRN, reason Reason,
) PartitionAssignment {
	return PartitionAssignment{
		Change:       ChangeRemoved,
		Reason:       reason,
		PartitionFRN: partitionFRN,
		Generation:   generation,
		TopicName:    topicName,
		ClusterName:  clusterName,
		ClusterFRN:   clusterFRN,
	}
}
