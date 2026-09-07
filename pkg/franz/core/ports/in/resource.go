package in

import (
	"context"

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

	// ReportReconciliation checks that the partition's cluster is in the agent's
	// scope (PERMISSION_DENIED otherwise), then applies the report to the
	// kafka_topic row under a row lock. It returns applied=false — not an error —
	// when the report's generation is stale (005 ADR §1.5).
	ReportReconciliation(ctx context.Context, in ReportReconciliationInput) (applied bool, err error)
}

// TelemetryIngestService is the driving port for TelemetryService's sample
// ingest (003.14). Deliverable 12 needs ingest only; the indicator registry,
// history queries, and governance trigger land with deliverable 14.
type TelemetryIngestService interface {
	// IngestSamples appends a batch for the agent in context and returns how many
	// rows landed.
	IngestSamples(ctx context.Context, samples []indicator.Sample) (int, error)
}
