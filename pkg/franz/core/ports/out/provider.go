package out

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
)

// ProviderEventPage is one page of provider-status history, newest first.
type ProviderEventPage struct {
	Events []*provider.Event
	// After is the opaque cursor for the next page ("" ⇒ no more).
	After string
}

// ProviderEventRepository persists the append-only cluster_provider_event log
// (004 ADR §4).
type ProviderEventRepository interface {
	// Append writes one status report.
	Append(ctx context.Context, e *provider.Event) error

	// ListByCluster returns history for a cluster, ordered by occurred_at
	// descending. after is "" for the first page or a cursor from a prior page.
	ListByCluster(ctx context.Context, clusterID uuid.UUID, limit int, after string) (ProviderEventPage, error)

	// PruneOlderThan deletes events with occurred_at < cutoff and returns the
	// count removed (004 ADR §4 — nightly 30-day prune).
	PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// ProviderStatusReader returns the current provider status of a cluster — the
// latest event — for GetKafkaCluster. Returns (nil, nil) when there is no event.
type ProviderStatusReader interface {
	LatestStatus(ctx context.Context, clusterID uuid.UUID) (*provider.Status, error)
}

// AssignmentPublisher fans an assignment change out to the connected streams of
// the owning agent (in-memory; 004 ADR §1). No-op when the agent has no open
// stream.
type AssignmentPublisher interface {
	PublishAssignment(agentName string, a provider.Assignment)
}
