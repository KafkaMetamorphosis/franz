package resourceprovider

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/scope"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// Notifier turns a Franz-side change into partition-assignment deltas for the
// connected Resource Provider agents whose label scope covers the clusters
// involved (005 ADR §1.3, Franz-side change #10).
//
// It only resolves scope for agents that currently hold an open stream — a
// disconnected agent receives the change in the full set it gets on its next
// reconnect, which is also what makes every method here best-effort: a delta
// that cannot be computed is logged, never propagated to the caller's
// transaction.
type Notifier struct {
	agents    out.AgentRepository
	clusters  out.ClusterRepository
	topics    out.TopicRepository
	publisher out.PartitionAssignmentPublisher
	log       *slog.Logger
}

var _ out.PartitionNotifier = (*Notifier)(nil)

// NewNotifier wires the notifier to its ports.
func NewNotifier(
	agents out.AgentRepository,
	clusters out.ClusterRepository,
	topics out.TopicRepository,
	publisher out.PartitionAssignmentPublisher,
	log *slog.Logger,
) *Notifier {
	return &Notifier{agents: agents, clusters: clusters, topics: topics, publisher: publisher, log: log}
}

// ShardsChanged pushes the current desired state of each shard to every
// connected agent whose scope covers the shard's cluster.
func (n *Notifier) ShardsChanged(ctx context.Context, realmID uuid.UUID, shards []*topic.KafkaTopic) {
	placed := make([]*topic.KafkaTopic, 0, len(shards))
	for _, sh := range shards {
		if sh != nil && sh.KafkaClusterID != nil {
			placed = append(placed, sh)
		}
	}
	if len(placed) == 0 {
		return
	}
	listeners, byClusterID, ok := n.listeners(ctx, realmID)
	if !ok || len(listeners) == 0 {
		return
	}

	for _, l := range listeners {
		for _, sh := range placed {
			c, known := byClusterID[*sh.KafkaClusterID]
			if !known || !l.selector.Matches(c.Labels) {
				continue
			}
			n.publisher.PublishPartitionAssignment(l.name, Assignment(sh, c))
		}
	}
}

// ClusterLabelsChanged re-resolves scope for one cluster whose labels moved:
// agents that gained it get SET for its partitions, agents that lost it get
// REMOVED with reason SCOPE_LOSS (005 ADR §1.2 "Scope is dynamic").
func (n *Notifier) ClusterLabelsChanged(
	ctx context.Context, realmID uuid.UUID, clusterName string, before, after map[string]string,
) {
	if !placementChanged(before, after) {
		return
	}
	c, err := n.clusters.Get(ctx, realmID, clusterName)
	if err != nil {
		n.warn("resolve cluster for scope change", "cluster", clusterName, "err", err)
		return
	}
	shards, err := n.topics.ListByClusters(ctx, realmID, []uuid.UUID{c.ID})
	if err != nil {
		n.warn("list partitions for scope change", "cluster", clusterName, "err", err)
		return
	}
	if len(shards) == 0 {
		return
	}
	listeners, ok := n.connectedListeners(ctx, realmID)
	if !ok {
		return
	}

	for _, l := range listeners {
		was, is := l.selector.Matches(before), l.selector.Matches(after)
		switch {
		case is && !was:
			n.publishSet(l.name, shards, c)
		case was && !is:
			n.publishScopeLoss(l.name, shards, c)
		}
	}
}

// AgentSelectorChanged re-resolves scope for one agent whose
// `franz.placement-selector/*` labels moved, with the same SET / SCOPE_LOSS
// consequences confined to that agent.
func (n *Notifier) AgentSelectorChanged(
	ctx context.Context, realmID uuid.UUID, agentName string, before, after map[string]string,
) {
	beforeSel := scope.SelectorFromLabels(before)
	afterSel := scope.SelectorFromLabels(after)
	if selectorsEqual(beforeSel, afterSel) {
		return
	}
	if !n.isConnected(agentName) {
		return
	}
	clusters, err := n.clusters.ListAll(ctx, realmID)
	if err != nil {
		n.warn("list clusters for agent scope change", "agent", agentName, "err", err)
		return
	}

	var gained, lost []*cluster.Cluster
	for _, c := range clusters {
		if c.State == cluster.StateDeleted {
			continue
		}
		was, is := beforeSel.Matches(c.Labels), afterSel.Matches(c.Labels)
		switch {
		case is && !was:
			gained = append(gained, c)
		case was && !is:
			lost = append(lost, c)
		}
	}
	if len(gained) == 0 && len(lost) == 0 {
		return
	}

	for _, c := range gained {
		shards, err := n.topics.ListByClusters(ctx, realmID, []uuid.UUID{c.ID})
		if err != nil {
			n.warn("list partitions for agent scope gain", "cluster", c.Name, "err", err)
			continue
		}
		n.publishSet(agentName, shards, c)
	}
	for _, c := range lost {
		shards, err := n.topics.ListByClusters(ctx, realmID, []uuid.UUID{c.ID})
		if err != nil {
			n.warn("list partitions for agent scope loss", "cluster", c.Name, "err", err)
			continue
		}
		n.publishScopeLoss(agentName, shards, c)
	}
}

// --- internals ----------------------------------------------------------

// listener is one connected agent and the scope its labels declare.
type listener struct {
	name     string
	selector scope.Selector
}

func (n *Notifier) publishSet(agentName string, shards []*topic.KafkaTopic, c *cluster.Cluster) {
	for _, sh := range shards {
		n.publisher.PublishPartitionAssignment(agentName, Assignment(sh, c))
	}
}

func (n *Notifier) publishScopeLoss(agentName string, shards []*topic.KafkaTopic, c *cluster.Cluster) {
	for _, sh := range shards {
		n.publisher.PublishPartitionAssignment(agentName,
			resource.Removed(sh.FRN, sh.Generation, sh.Name, c.Name, c.FRN, resource.ReasonScopeLoss))
	}
}

func (n *Notifier) isConnected(agentName string) bool {
	for _, name := range n.publisher.ConnectedPartitionAgents() {
		if name == agentName {
			return true
		}
	}
	return false
}

// listeners returns the connected agents with a non-empty scope, together with
// every non-deleted cluster in the realm indexed by id. ok is false when the
// lookup failed (already logged) — callers skip the notification.
func (n *Notifier) listeners(
	ctx context.Context, realmID uuid.UUID,
) ([]listener, map[uuid.UUID]*cluster.Cluster, bool) {
	ls, ok := n.connectedListeners(ctx, realmID)
	if !ok || len(ls) == 0 {
		return nil, nil, ok
	}
	clusters, err := n.clusters.ListAll(ctx, realmID)
	if err != nil {
		n.warn("list clusters for partition notification", "err", err)
		return nil, nil, false
	}
	byID := make(map[uuid.UUID]*cluster.Cluster, len(clusters))
	for _, c := range clusters {
		if c.State != cluster.StateDeleted {
			byID[c.ID] = c
		}
	}
	return ls, byID, true
}

// connectedListeners resolves each connected agent name to its declared scope,
// dropping agents that are gone, soft-deleted, or declare no scope at all.
func (n *Notifier) connectedListeners(ctx context.Context, realmID uuid.UUID) ([]listener, bool) {
	names := n.publisher.ConnectedPartitionAgents()
	if len(names) == 0 {
		return nil, true
	}
	ls := make([]listener, 0, len(names))
	for _, name := range names {
		a, err := n.agents.Get(ctx, realmID, name)
		if err != nil {
			continue // another realm's agent, or deleted mid-flight
		}
		if a.Status == agent.StatusDeleted {
			continue
		}
		sel := scope.SelectorFromLabels(a.Labels)
		if sel.IsEmpty() {
			continue
		}
		ls = append(ls, listener{name: name, selector: sel})
	}
	return ls, true
}

func (n *Notifier) warn(msg string, args ...any) {
	if n.log != nil {
		n.log.Warn(msg, args...)
	}
}

// placementChanged reports whether the reserved `franz.placement/*` subset of
// two label maps differs. A change to free-form labels moves no scope.
func placementChanged(before, after map[string]string) bool {
	b, a := scope.PlacementFromLabels(before), scope.PlacementFromLabels(after)
	if len(b) != len(a) {
		return true
	}
	for k, v := range b {
		if a[k] != v {
			return true
		}
	}
	return false
}

func selectorsEqual(a, b scope.Selector) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
