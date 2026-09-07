// Package resourceprovider is the Resource Provider interaction application
// service (005 ADR): the in-scope assignment set for a connecting Gregor Samsa
// instance, and generation-gated intake of its reconciliation reports.
//
// Scope is resolved server-side on every call (domain/scope) — Franz holds both
// the agent's `franz.placement-selector/*` labels and every cluster's
// `franz.placement/*` labels, so the agent never runs selector logic and never
// sees a partition it does not own.
package resourceprovider

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/scope"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// Service implements in.ResourceProviderService.
type Service struct {
	clusters out.ClusterRepository
	topics   out.TopicRepository
}

var _ in.ResourceProviderService = (*Service)(nil)

// NewService wires the service to its ports.
func NewService(clusters out.ClusterRepository, topics out.TopicRepository) *Service {
	return &Service{clusters: clusters, topics: topics}
}

// InitialPartitionAssignments returns one assignment per async channel partition
// on a cluster in the authenticated agent's label scope (005 ADR §1.2, §1.3).
// An agent with no `franz.placement-selector/*` labels is inert and gets nothing.
func (s *Service) InitialPartitionAssignments(ctx context.Context) ([]resource.PartitionAssignment, error) {
	a := agent.MustFromContext(ctx)

	inScope, err := s.inScopeClusters(ctx, a.RealmID, a.Labels)
	if err != nil {
		return nil, err
	}
	if len(inScope) == 0 {
		return nil, nil
	}

	byID := make(map[uuid.UUID]*cluster.Cluster, len(inScope))
	ids := make([]uuid.UUID, 0, len(inScope))
	for _, c := range inScope {
		byID[c.ID] = c
		ids = append(ids, c.ID)
	}

	shards, err := s.topics.ListByClusters(ctx, a.RealmID, ids)
	if err != nil {
		return nil, err
	}
	assignments := make([]resource.PartitionAssignment, 0, len(shards))
	for _, sh := range shards {
		if sh.KafkaClusterID == nil {
			continue // unplaced; nothing to reconcile yet
		}
		c, ok := byID[*sh.KafkaClusterID]
		if !ok {
			continue
		}
		assignments = append(assignments, Assignment(sh, c))
	}
	return assignments, nil
}

// ReportReconciliation applies one agent report to the shard it names.
//
// Two guards, in order: the partition must sit on a cluster in the reporting
// agent's scope (PERMISSION_DENIED otherwise — 005 ADR §1.7), and the report's
// generation must still be the row's current generation. A stale report is
// acknowledged with applied=false and leaves the row untouched; Franz has
// already re-emitted the assignment with the new generation.
func (s *Service) ReportReconciliation(
	ctx context.Context, input in.ReportReconciliationInput,
) (bool, error) {
	a := agent.MustFromContext(ctx)

	inScope, err := s.inScopeClusters(ctx, a.RealmID, a.Labels)
	if err != nil {
		return false, err
	}
	scopedIDs := make(map[uuid.UUID]bool, len(inScope))
	for _, c := range inScope {
		scopedIDs[c.ID] = true
	}

	var applied bool
	_, err = s.topics.MutateByFRN(ctx, a.RealmID, input.PartitionFRNPath,
		func(t *topic.KafkaTopic) error {
			if t.KafkaClusterID == nil || !scopedIDs[*t.KafkaClusterID] {
				return errs.Deniedf(
					"agent %q is not in scope for async channel partition %q", a.Name, t.Name)
			}
			ok, err := t.RecordReconciliation(
				input.Generation, input.Outcome, input.Message, input.Applied)
			if err != nil {
				return err
			}
			applied = ok
			return nil
		})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// inScopeClusters resolves the agent's label scope against the realm's clusters.
func (s *Service) inScopeClusters(
	ctx context.Context, realmID uuid.UUID, agentLabels map[string]string,
) ([]*cluster.Cluster, error) {
	if scope.SelectorFromLabels(agentLabels).IsEmpty() {
		return nil, nil
	}
	all, err := s.clusters.ListAll(ctx, realmID)
	if err != nil {
		return nil, err
	}
	return scope.Resolve(agentLabels, all), nil
}

// Assignment maps a shard plus the cluster it sits on onto the desired state its
// agent reconciles against (005 ADR §1.3). The shard's state picks the change:
// PENDING / READY / ERROR → SET, PAUSED → PAUSED, DELETED → REMOVED.
//
// PAUSED and REMOVED deliberately carry no desired configuration — the agent
// must not create or alter anything from them.
func Assignment(t *topic.KafkaTopic, c *cluster.Cluster) resource.PartitionAssignment {
	conns := make([]resource.ConnectionString, len(c.ConnectionStrings))
	for i, cs := range c.ConnectionStrings {
		conns[i] = resource.ConnectionString{BootstrapURLs: cs.BootstrapURLs, Type: string(cs.Type)}
	}

	a := resource.PartitionAssignment{
		Change:            resource.ChangeSet,
		PartitionFRN:      t.FRN,
		Generation:        t.Generation,
		AsyncChannel:      t.ChannelName,
		TopicName:         t.Name,
		ClusterName:       c.Name,
		ClusterFRN:        c.FRN,
		ConnectionStrings: conns,
	}
	switch t.State {
	case topic.StatePaused:
		a.Change = resource.ChangePaused
		return a
	case topic.StateDeleted:
		a.Change = resource.ChangeRemoved
		return a
	}
	a.DesiredConfig = t.MaterializedConfiguration
	a.Partitions = t.Partitions
	a.ReplicationFactor = t.ReplicationFactor
	return a
}
