package out

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
)

// PartitionAssignmentPublisher fans a partition-assignment change out to the
// connected streams of one Resource Provider agent (in-memory; 005 ADR §1.3).
// No-op when the agent has no open stream — it will get the change in the full
// set on its next reconnect.
type PartitionAssignmentPublisher interface {
	PublishPartitionAssignment(agentName string, a resource.PartitionAssignment)

	// ConnectedPartitionAgents names the agents with an open stream. The
	// notifier resolves scope for these only; an agent that is not connected
	// picks the change up in the full set it receives on its next reconnect.
	ConnectedPartitionAgents() []string
}

// PartitionNotifier turns a Franz-side change into partition-assignment deltas
// for every Resource Provider agent whose label scope covers the affected
// clusters (005 ADR §1.3, Franz-side change #10). Implemented by the
// resourceprovider use case; the entity services depend on this interface only,
// so they never learn about agents or scope.
//
// Every method is best-effort and non-blocking on the caller's transaction: a
// missed delta is recovered by the agent's next reconnect resync, which replays
// the full in-scope set.
type PartitionNotifier interface {
	// ShardsChanged announces that the desired state of these shards changed
	// (created, re-placed, paused, resumed, deleted, config or generation bump).
	ShardsChanged(ctx context.Context, realmID uuid.UUID, shards []*topic.KafkaTopic)

	// ClusterLabelsChanged announces that a cluster's labels changed, so its
	// `franz.placement/*` coordinates — and therefore which agents hold its
	// partitions — may have moved. Agents that gained the cluster get SET for
	// its partitions; agents that lost it get REMOVED(reason=SCOPE_LOSS).
	ClusterLabelsChanged(ctx context.Context, realmID uuid.UUID, clusterName string,
		before, after map[string]string)

	// AgentSelectorChanged announces that one agent's
	// `franz.placement-selector/*` labels changed, with the same SET / SCOPE_LOSS
	// consequences for that agent alone.
	AgentSelectorChanged(ctx context.Context, realmID uuid.UUID, agentName string,
		before, after map[string]string)
}

// IndicatorSampleRepository persists the append-only indicator_sample time
// series (003.14). Deliverable 12 shipped ingest and the prune; deliverable 14
// added the history and current-value reads governance and ListIndicatorSamples
// need.
type IndicatorSampleRepository interface {
	// Append writes a batch in one statement and returns how many rows landed.
	Append(ctx context.Context, samples []*indicator.Sample) (int, error)

	// PruneOlderThan deletes samples with sample_at < cutoff (003.14 — nightly
	// 30-day prune) and returns the count removed.
	PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error)

	// List returns one page of the history, newest first (ListIndicatorSamples).
	List(ctx context.Context, q SampleQuery) (SamplePage, error)

	// LatestPerResource returns the newest sample per resource_frn for one
	// indicator — the "current" value view governance reads (003.14). Ordered by
	// resource_frn ascending, so a dry run is deterministic. limit caps the
	// result; <= 0 applies the repository's own bound.
	LatestPerResource(ctx context.Context, realmID uuid.UUID, name string, limit int) ([]*indicator.Sample, error)
}
