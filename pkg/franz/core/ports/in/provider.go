package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
)

// ReportStatusInput is one ReportClusterStatus call from an agent. The agent
// identity comes from context (agent-auth interceptor).
type ReportStatusInput struct {
	ClusterName string
	Phase       provider.Phase
	Reachable   bool
	Message     string
	RecipeRef   string
}

// ProviderEventPage is a page of provider-status history for the console.
type ProviderEventPage struct {
	Events        []*provider.Event
	NextPageToken string
}

// ClusterProviderService is the driving port for the Cluster Provider contract
// (004 ADR). WatchClusterAssignments' streaming lives in the adapter; the port
// exposes the pieces the handler composes.
type ClusterProviderService interface {
	// InitialAssignments returns the full current assignment set for the agent in
	// context — one per cluster it owns (004 ADR §1, sent on stream open).
	InitialAssignments(ctx context.Context) ([]provider.Assignment, error)

	// ReportStatus validates that the agent in context owns the named cluster
	// (PERMISSION_DENIED otherwise) and appends a cluster_provider_event.
	ReportStatus(ctx context.Context, in ReportStatusInput) error

	// ListEvents returns provider-status history for a cluster (console-facing;
	// realm from context).
	ListEvents(ctx context.Context, clusterName string, pageSize int32, pageToken string) (ProviderEventPage, error)
}
