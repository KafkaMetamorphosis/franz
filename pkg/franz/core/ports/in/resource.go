package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
)

// ReportReconciliationInput is one ReportPartitionReconciliation call. The agent
// identity comes from context (agent-auth interceptor).
type ReportReconciliationInput struct {
	// PartitionFRN as the agent sent it — rendered with the deployment prefix.
	// The adapter parses it; the service takes the parsed path.
	PartitionFRNPath string
	Generation       int64
	Outcome          topic.Outcome
	Message          string
	Applied          *topic.AppliedState
}

// ResourceProviderService is the driving port for the Resource Provider contract
// (005 ADR). WatchPartitionAssignments' streaming lives in the adapter; the port
// exposes the pieces the handler composes.
type ResourceProviderService interface {
	// InitialPartitionAssignments returns the full in-scope assignment set for
	// the agent in context — every async channel partition on every cluster its
	// `franz.placement-selector/*` labels match (005 ADR §1.2, sent on stream
	// open).
	InitialPartitionAssignments(ctx context.Context) ([]resource.PartitionAssignment, error)

	// InScopeClusters returns the clusters the agent in context is responsible
	// for, ordered by name — the informational scope snapshot sent as the first
	// message on stream open. Empty when the agent has no
	// `franz.placement-selector/*` labels.
	InScopeClusters(ctx context.Context) ([]resource.ScopedCluster, error)

	// ReportReconciliation checks that the partition's cluster is in the agent's
	// scope (PERMISSION_DENIED otherwise), then applies the report to the
	// kafka_topic row under a row lock. It returns applied=false — not an error —
	// when the report's generation is stale (005 ADR §1.5).
	ReportReconciliation(ctx context.Context, in ReportReconciliationInput) (applied bool, err error)
}

// TelemetryIngestService is the driving port for TelemetryService — the two
// inbound agent streams of 003.14. The reads over what it writes are elsewhere
// by design: sample history is GovernanceService.ListIndicatorSamples, and the
// consumer-group views are ClientService's, so an agent's port grants no read
// access to the fleet.
type TelemetryIngestService interface {
	// IngestSamples validates and appends a batch for the agent in context and
	// returns how many rows landed.
	//
	// Validation is per 003.14 and the batch is atomic: an unregistered indicator
	// (FAILED_PRECONDITION), a resource_entity that disagrees with the indicator's
	// applies_to, or a value that does not parse in the indicator's unit rejects
	// the whole call. Nothing is written, because the response carries only a
	// count and so has no way to say which rows landed.
	//
	// A sample that advances an indicator's current value also triggers governance
	// evaluation (003.14 "Governance coupling"); an out-of-order sample is stored
	// as history and triggers nothing.
	IngestSamples(ctx context.Context, samples []indicator.Sample) (int, error)

	// IngestConsumerGroups appends a batch of consumer-group sightings for the
	// agent in context. They are read-only context: no governance evaluation
	// follows (003.14).
	IngestConsumerGroups(ctx context.Context, observations []consumergroup.Observation) (int, error)
}
