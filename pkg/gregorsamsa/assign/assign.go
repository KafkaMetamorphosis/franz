// Package assign is Gregor Samsa's local view of one partition assignment: the
// proto stripped down to what the reconciler and the telemetry sweep read.
// Keeping it out of the reconciler means neither has to import generated code.
package assign

import (
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// Change is what Franz says happened to a partition.
type Change string

const (
	ChangeUnknown Change = ""
	ChangeSet     Change = "SET"
	ChangePaused  Change = "PAUSED"
	ChangeRemoved Change = "REMOVED"
)

// Reason qualifies a ChangeRemoved.
type Reason string

// ReasonScopeLoss means the cluster left this instance's scope: drop the
// partition and do nothing to Kafka (005 ADR §1.4).
const ReasonScopeLoss Reason = "SCOPE_LOSS"

// Assignment is one partition's desired state.
type Assignment struct {
	Change Change
	Reason Reason

	PartitionFRN string
	Generation   int64
	AsyncChannel string
	TopicName    string

	ClusterName      string
	ClusterFRN       string
	BootstrapServers []string

	DesiredConfig     map[string]string
	Partitions        int32
	ReplicationFactor int32
}

// ScopedCluster is one Kafka Cluster in this agent's label scope — the clusters
// its `franz.placement-selector/*` labels currently match (005 ADR §1.2). Franz
// sends the full set as the first message of every (re)connected stream.
//
// This is what lets the agent act on a cluster before anything is placed on it:
// the FRN to key a sample to and the brokers to connect to both arrive here, not
// only on a PartitionAssignment. The telemetry sweep's cluster-level indicators
// (005 §2.1) depend on it — without scope they could only be computed for a
// cluster that already had a placed shard, which is not a property of the
// cluster at all.
type ScopedCluster struct {
	Name             string
	FRN              string
	BootstrapServers []string
}

// ScopeFromProto maps the stream's scope message onto the local view. A cluster
// with no name is dropped: it cannot be keyed in the admin cache.
func ScopeFromProto(sc *franzv1.StreamScope) []ScopedCluster {
	if sc == nil {
		return nil
	}
	out := make([]ScopedCluster, 0, len(sc.GetClusters()))
	for _, c := range sc.GetClusters() {
		if c.GetName() == "" {
			continue
		}
		sc := ScopedCluster{Name: c.GetName(), FRN: c.GetKafkaClusterFrn()}
		for _, cs := range c.GetConnectionStrings() {
			sc.BootstrapServers = append(sc.BootstrapServers, cs.GetBootstrapUrls()...)
		}
		out = append(out, sc)
	}
	return out
}

// IsScopeLoss reports whether this REMOVED is a scope hand-off rather than a
// delete.
func (a Assignment) IsScopeLoss() bool {
	return a.Change == ChangeRemoved && a.Reason == ReasonScopeLoss
}

// SameDesiredState reports whether two assignments describe the same work. The
// reconciler uses it to skip a partition whose desired state has not moved since
// it was last applied, so a full resync of an unchanged fleet performs zero
// Kafka calls (005 ADR §1.3).
func (a Assignment) SameDesiredState(b Assignment) bool {
	if a.Change != b.Change || a.Reason != b.Reason || a.Generation != b.Generation {
		return false
	}
	if a.TopicName != b.TopicName || a.ClusterName != b.ClusterName {
		return false
	}
	if a.Partitions != b.Partitions || a.ReplicationFactor != b.ReplicationFactor {
		return false
	}
	return configsEqual(a.DesiredConfig, b.DesiredConfig)
}

func configsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// FromProto maps a stream message onto the local view.
func FromProto(p *franzv1.PartitionAssignment) Assignment {
	a := Assignment{
		Change:            changeFromProto(p.GetChange()),
		Reason:            reasonFromProto(p.GetReason()),
		PartitionFRN:      p.GetPartitionFrn(),
		Generation:        p.GetGeneration(),
		AsyncChannel:      p.GetAsyncChannel(),
		TopicName:         p.GetTopicName(),
		ClusterName:       p.GetKafkaCluster(),
		ClusterFRN:        p.GetKafkaClusterFrn(),
		DesiredConfig:     p.GetDesiredConfig(),
		Partitions:        p.GetPartitions(),
		ReplicationFactor: p.GetReplicationFactor(),
	}
	for _, cs := range p.GetConnectionStrings() {
		a.BootstrapServers = append(a.BootstrapServers, cs.GetBootstrapUrls()...)
	}
	return a
}

func changeFromProto(c franzv1.PartitionAssignment_Change) Change {
	switch c {
	case franzv1.PartitionAssignment_CHANGE_SET:
		return ChangeSet
	case franzv1.PartitionAssignment_CHANGE_PAUSED:
		return ChangePaused
	case franzv1.PartitionAssignment_CHANGE_REMOVED:
		return ChangeRemoved
	default:
		return ChangeUnknown
	}
}

func reasonFromProto(r franzv1.PartitionAssignment_Reason) Reason {
	if r == franzv1.PartitionAssignment_REASON_SCOPE_LOSS {
		return ReasonScopeLoss
	}
	return ""
}
