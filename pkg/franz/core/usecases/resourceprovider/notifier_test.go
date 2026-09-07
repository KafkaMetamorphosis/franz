package resourceprovider_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/resourceprovider"
)

type fakeAgents struct{ rows []*agent.Agent }

func (f *fakeAgents) Get(_ context.Context, _ uuid.UUID, name string) (*agent.Agent, error) {
	for _, a := range f.rows {
		if a.Name == name {
			return a, nil
		}
	}
	return nil, errs.NotFoundf("agent %q not found", name)
}
func (f *fakeAgents) Create(context.Context, *agent.Agent) error { panic("unused") }
func (f *fakeAgents) List(context.Context, out.AgentQuery) (out.AgentPage, error) {
	panic("unused")
}
func (f *fakeAgents) Mutate(context.Context, uuid.UUID, string, func(*agent.Agent) error) (*agent.Agent, error) {
	panic("unused")
}
func (f *fakeAgents) GetByTokenHash(context.Context, string) (*agent.Agent, error) { panic("unused") }

// capturePublisher records what was published to whom, and which agents it
// claims are connected.
type capturePublisher struct {
	connected []string
	published map[string][]resource.PartitionAssignment
}

func newCapturePublisher(connected ...string) *capturePublisher {
	return &capturePublisher{
		connected: connected,
		published: map[string][]resource.PartitionAssignment{},
	}
}

func (p *capturePublisher) PublishPartitionAssignment(agentName string, a resource.PartitionAssignment) {
	p.published[agentName] = append(p.published[agentName], a)
}
func (p *capturePublisher) ConnectedPartitionAgents() []string { return p.connected }

func agentRow(name string, labels map[string]string) *agent.Agent {
	return &agent.Agent{
		ID: uuid.New(), Name: name, RealmID: realmID,
		Type: agent.TypeResourceProvider, Status: agent.StatusActive, Labels: labels,
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestShardsChangedPublishesOnlyToInScopeConnectedAgents(t *testing.T) {
	east := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	shard := shardOn(east, "billing-events-0", topic.StatePending, 2)

	pub := newCapturePublisher("gs-prod", "gs-staging")
	notifier := resourceprovider.NewNotifier(
		&fakeAgents{rows: []*agent.Agent{
			agentRow("gs-prod", map[string]string{prodSelector: "prod"}),
			agentRow("gs-staging", map[string]string{prodSelector: "staging"}),
			agentRow("gs-offline", map[string]string{prodSelector: "prod"}),
		}},
		&fakeClusters{rows: []*cluster.Cluster{east}},
		&fakeTopics{rows: []*topic.KafkaTopic{shard}},
		pub, quietLogger())

	notifier.ShardsChanged(context.Background(), realmID, []*topic.KafkaTopic{shard})

	if got := len(pub.published["gs-prod"]); got != 1 {
		t.Fatalf("gs-prod got %d assignments, want 1", got)
	}
	if a := pub.published["gs-prod"][0]; a.Change != resource.ChangeSet || a.TopicName != "billing-events-0" {
		t.Errorf("assignment = %+v", a)
	}
	if got := len(pub.published["gs-staging"]); got != 0 {
		t.Errorf("out-of-scope agent got %d assignments, want 0", got)
	}
	if got := len(pub.published["gs-offline"]); got != 0 {
		t.Errorf("disconnected agent got %d assignments, want 0 (it resyncs on reconnect)", got)
	}
}

func TestShardsChangedSkipsUnplacedShards(t *testing.T) {
	east := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	unplaced := &topic.KafkaTopic{Name: "billing-events-0", State: topic.StatePending}

	pub := newCapturePublisher("gs-prod")
	notifier := resourceprovider.NewNotifier(
		&fakeAgents{rows: []*agent.Agent{agentRow("gs-prod", map[string]string{prodSelector: "prod"})}},
		&fakeClusters{rows: []*cluster.Cluster{east}},
		&fakeTopics{}, pub, quietLogger())

	notifier.ShardsChanged(context.Background(), realmID, []*topic.KafkaTopic{unplaced})
	if len(pub.published) != 0 {
		t.Fatalf("an unplaced shard has no cluster and no agent: %+v", pub.published)
	}
}

func TestClusterLabelsChangedMovesScope(t *testing.T) {
	// The cluster ends up labelled staging; gs-prod loses it, gs-staging gains it.
	east := clusterRow("east-1", map[string]string{prodLabel: "staging"})
	shard := shardOn(east, "billing-events-0", topic.StatePending, 3)

	pub := newCapturePublisher("gs-prod", "gs-staging")
	notifier := resourceprovider.NewNotifier(
		&fakeAgents{rows: []*agent.Agent{
			agentRow("gs-prod", map[string]string{prodSelector: "prod"}),
			agentRow("gs-staging", map[string]string{prodSelector: "staging"}),
		}},
		&fakeClusters{rows: []*cluster.Cluster{east}},
		&fakeTopics{rows: []*topic.KafkaTopic{shard}},
		pub, quietLogger())

	notifier.ClusterLabelsChanged(context.Background(), realmID, "east-1",
		map[string]string{prodLabel: "prod"},
		map[string]string{prodLabel: "staging"})

	lost := pub.published["gs-prod"]
	if len(lost) != 1 {
		t.Fatalf("gs-prod got %d assignments, want 1 scope-loss REMOVED", len(lost))
	}
	if lost[0].Change != resource.ChangeRemoved || lost[0].Reason != resource.ReasonScopeLoss {
		t.Errorf("scope loss = %+v, want REMOVED/SCOPE_LOSS", lost[0])
	}
	if lost[0].DesiredConfig != nil || lost[0].Partitions != 0 {
		t.Errorf("a scope-loss REMOVED must carry no desired state: %+v", lost[0])
	}

	gained := pub.published["gs-staging"]
	if len(gained) != 1 || gained[0].Change != resource.ChangeSet {
		t.Fatalf("gs-staging got %+v, want one SET", gained)
	}
}

func TestClusterLabelsChangedIgnoresFreeFormLabels(t *testing.T) {
	east := clusterRow("east-1", map[string]string{prodLabel: "prod", "team": "billing"})
	shard := shardOn(east, "billing-events-0", topic.StatePending, 1)

	pub := newCapturePublisher("gs-prod")
	notifier := resourceprovider.NewNotifier(
		&fakeAgents{rows: []*agent.Agent{agentRow("gs-prod", map[string]string{prodSelector: "prod"})}},
		&fakeClusters{rows: []*cluster.Cluster{east}},
		&fakeTopics{rows: []*topic.KafkaTopic{shard}},
		pub, quietLogger())

	notifier.ClusterLabelsChanged(context.Background(), realmID, "east-1",
		map[string]string{prodLabel: "prod", "team": "payments"},
		map[string]string{prodLabel: "prod", "team": "billing"})

	if len(pub.published) != 0 {
		t.Fatalf("a change to non-reserved labels moves no scope: %+v", pub.published)
	}
}

func TestAgentSelectorChangedMovesScopeForThatAgentOnly(t *testing.T) {
	east := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	west := clusterRow("west-1", map[string]string{prodLabel: "staging"})
	eastShard := shardOn(east, "billing-events-0", topic.StatePending, 1)
	westShard := shardOn(west, "audit-events-0", topic.StatePending, 1)

	pub := newCapturePublisher("gs-1", "gs-2")
	notifier := resourceprovider.NewNotifier(
		&fakeAgents{rows: []*agent.Agent{
			agentRow("gs-1", map[string]string{prodSelector: "staging"}),
			agentRow("gs-2", map[string]string{prodSelector: "prod"}),
		}},
		&fakeClusters{rows: []*cluster.Cluster{east, west}},
		&fakeTopics{rows: []*topic.KafkaTopic{eastShard, westShard}},
		pub, quietLogger())

	// gs-1 re-points from prod to staging.
	notifier.AgentSelectorChanged(context.Background(), realmID, "gs-1",
		map[string]string{prodSelector: "prod"},
		map[string]string{prodSelector: "staging"})

	got := pub.published["gs-1"]
	if len(got) != 2 {
		t.Fatalf("gs-1 got %d assignments, want 2 (one SET, one SCOPE_LOSS)", len(got))
	}
	var sets, losses int
	for _, a := range got {
		switch {
		case a.Change == resource.ChangeSet && a.TopicName == "audit-events-0":
			sets++
		case a.Change == resource.ChangeRemoved && a.Reason == resource.ReasonScopeLoss &&
			a.TopicName == "billing-events-0":
			losses++
		default:
			t.Errorf("unexpected assignment: %+v", a)
		}
	}
	if sets != 1 || losses != 1 {
		t.Errorf("sets = %d, losses = %d; want 1 and 1", sets, losses)
	}
	if len(pub.published["gs-2"]) != 0 {
		t.Errorf("another agent's selector change must not touch gs-2: %+v", pub.published["gs-2"])
	}
}

func TestAgentSelectorChangedIsANoOpWhenNothingMoved(t *testing.T) {
	east := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	pub := newCapturePublisher("gs-1")
	notifier := resourceprovider.NewNotifier(
		&fakeAgents{rows: []*agent.Agent{agentRow("gs-1", map[string]string{prodSelector: "prod"})}},
		&fakeClusters{rows: []*cluster.Cluster{east}},
		&fakeTopics{}, pub, quietLogger())

	notifier.AgentSelectorChanged(context.Background(), realmID, "gs-1",
		map[string]string{prodSelector: "prod", "team": "a"},
		map[string]string{prodSelector: "prod", "team": "b"})

	if len(pub.published) != 0 {
		t.Fatalf("selector unchanged, nothing to publish: %+v", pub.published)
	}
}
