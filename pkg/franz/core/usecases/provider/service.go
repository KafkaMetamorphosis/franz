// Package provider is the Cluster Provider interaction application service
// (004 ADR): initial assignments for a connecting agent, status-report intake
// with an ownership check, and provider-status history for the console.
package provider

import (
	"context"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	prov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
)

// Service implements in.ClusterProviderService.
type Service struct {
	clusters out.ClusterRepository
	events   out.ProviderEventRepository
	now      func() time.Time
}

var _ in.ClusterProviderService = (*Service)(nil)

// NewService wires the service to its ports.
func NewService(clusters out.ClusterRepository, events out.ProviderEventRepository) *Service {
	return &Service{clusters: clusters, events: events, now: time.Now}
}

// InitialAssignments returns one assignment per cluster the authenticated agent
// owns (004 ADR §1). DELETED clusters are included as REMOVED so an agent that
// reconnects after a delete tears the substrate down.
func (s *Service) InitialAssignments(ctx context.Context) ([]prov.Assignment, error) {
	a := agent.MustFromContext(ctx)
	clusters, err := s.clusters.ListByProviderAgent(ctx, a.RealmID, a.Name)
	if err != nil {
		return nil, err
	}
	out := make([]prov.Assignment, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, c.ToAssignment())
	}
	return out, nil
}

// ReportStatus records a status report after checking the agent owns the cluster.
func (s *Service) ReportStatus(ctx context.Context, input in.ReportStatusInput) error {
	a := agent.MustFromContext(ctx)

	c, err := s.clusters.Get(ctx, a.RealmID, input.ClusterName)
	if err != nil {
		return err
	}
	if c.ProviderAgent != a.Name {
		return errs.Deniedf("agent %q does not own kafka cluster %q", a.Name, input.ClusterName)
	}

	event, err := prov.NewEvent(
		c.ID, c.RealmID, c.FRN,
		input.Phase, input.Reachable, input.Message, a.Name, input.RecipeRef,
		s.now().UTC(),
	)
	if err != nil {
		return err
	}
	return s.events.Append(ctx, event)
}

// ListEvents returns provider-status history for a cluster, newest first.
func (s *Service) ListEvents(
	ctx context.Context, clusterName string, pageSize int32, pageToken string,
) (in.ProviderEventPage, error) {
	r := realm.MustFromContext(ctx)

	c, err := s.clusters.Get(ctx, r.ID, clusterName)
	if err != nil {
		return in.ProviderEventPage{}, err
	}

	queryKey := pagetoken.QueryKey("cluster-provider-event", clusterName)
	after, err := pagetoken.Decode(pageToken, queryKey)
	if err != nil {
		return in.ProviderEventPage{}, err
	}

	page, err := s.events.ListByCluster(ctx, c.ID, pagetoken.ClampSize(pageSize), after)
	if err != nil {
		return in.ProviderEventPage{}, err
	}
	return in.ProviderEventPage{
		Events:        page.Events,
		NextPageToken: pagetoken.Encode(page.After, queryKey),
	}, nil
}
