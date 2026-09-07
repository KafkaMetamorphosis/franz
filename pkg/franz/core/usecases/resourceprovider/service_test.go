package resourceprovider_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/resourceprovider"
)

var realmID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

// --- fakes ---------------------------------------------------------------

type fakeClusters struct{ rows []*cluster.Cluster }

func (f *fakeClusters) ListAll(context.Context, uuid.UUID) ([]*cluster.Cluster, error) {
	return f.rows, nil
}
func (f *fakeClusters) Get(_ context.Context, _ uuid.UUID, name string) (*cluster.Cluster, error) {
	for _, c := range f.rows {
		if c.Name == name {
			return c, nil
		}
	}
	return nil, errs.NotFoundf("kafka cluster %q not found", name)
}
func (f *fakeClusters) Create(context.Context, *cluster.Cluster) error { panic("unused") }
func (f *fakeClusters) List(context.Context, out.ClusterQuery) (out.ClusterPage, error) {
	panic("unused")
}
func (f *fakeClusters) Mutate(context.Context, uuid.UUID, string, func(*cluster.Cluster) error) (*cluster.Cluster, error) {
	panic("unused")
}
func (f *fakeClusters) ListByProviderAgent(context.Context, uuid.UUID, string) ([]*cluster.Cluster, error) {
	panic("unused")
}

type fakeTopics struct {
	rows []*topic.KafkaTopic
	// mutated records the FRN paths MutateByFRN persisted.
	mutated []string
}

func (f *fakeTopics) ListByClusters(_ context.Context, _ uuid.UUID, ids []uuid.UUID) ([]*topic.KafkaTopic, error) {
	want := map[uuid.UUID]bool{}
	for _, id := range ids {
		want[id] = true
	}
	var out []*topic.KafkaTopic
	for _, t := range f.rows {
		if t.KafkaClusterID != nil && want[*t.KafkaClusterID] {
			out = append(out, t)
		}
	}
	return out, nil
}

func (f *fakeTopics) MutateByFRN(
	_ context.Context, _ uuid.UUID, frnPath string, mutate func(*topic.KafkaTopic) error,
) (*topic.KafkaTopic, error) {
	for _, t := range f.rows {
		if t.FRN.Path() != frnPath {
			continue
		}
		if err := mutate(t); err != nil {
			return nil, err
		}
		f.mutated = append(f.mutated, frnPath)
		return t, nil
	}
	return nil, errs.NotFoundf("kafka topic not found")
}

func (f *fakeTopics) Create(context.Context, *topic.KafkaTopic) error { panic("unused") }
func (f *fakeTopics) Get(context.Context, uuid.UUID, string) (*topic.KafkaTopic, error) {
	panic("unused")
}
func (f *fakeTopics) List(context.Context, out.TopicQuery) (out.TopicPage, error) { panic("unused") }
func (f *fakeTopics) MutateChannelShards(context.Context, uuid.UUID, uuid.UUID, func([]*topic.KafkaTopic) error) ([]*topic.KafkaTopic, error) {
	panic("unused")
}
func (f *fakeTopics) ResolveChannelID(context.Context, uuid.UUID, string) (uuid.UUID, error) {
	panic("unused")
}
func (f *fakeTopics) CountLiveTopics(context.Context, uuid.UUID) (int, error) { panic("unused") }

func (f *fakeTopics) PlaceChannelShards(context.Context, uuid.UUID, uuid.UUID,
	func([]*topic.KafkaTopic) (out.ShardPlan, error),
) ([]*topic.KafkaTopic, error) {
	panic("unused")
}

// --- fixtures ------------------------------------------------------------

func clusterRow(name string, labels map[string]string) *cluster.Cluster {
	return &cluster.Cluster{
		ID:      uuid.New(),
		FRN:     frn.MustParse("default:kafka-cluster:" + name),
		RealmID: realmID,
		Name:    name,
		State:   cluster.StateActive,
		Labels:  labels,
		ConnectionStrings: []cluster.ConnectionString{
			{BootstrapURLs: []string{name + ":9092"}, Type: cluster.ConnectionPlaintext},
		},
		Configuration: map[string]string{},
	}
}

func shardOn(c *cluster.Cluster, name string, state topic.State, generation int64) *topic.KafkaTopic {
	clusterID := c.ID
	return &topic.KafkaTopic{
		ID:                        uuid.New(),
		FRN:                       frn.MustParse("default:kafka-topic:" + name),
		RealmID:                   realmID,
		Name:                      name,
		KafkaClusterID:            &clusterID,
		ClusterName:               c.Name,
		ChannelName:               "billing-events",
		MaterializedConfiguration: map[string]string{"retention.ms": "604800000"},
		Partitions:                3,
		ReplicationFactor:         1,
		State:                     state,
		Generation:                generation,
	}
}

func agentCtx(name string, labels map[string]string) context.Context {
	return agent.NewContext(context.Background(), &agent.Agent{
		ID: uuid.New(), Name: name, RealmID: realmID,
		Type: agent.TypeResourceProvider, Status: agent.StatusActive, Labels: labels,
	})
}

const (
	prodLabel     = "franz.placement/env"
	prodSelector  = "franz.placement-selector/env"
	orgLabel      = "franz.placement/org"
	orgSelector   = "franz.placement-selector/org"
	partitionFRN0 = "default:kafka-topic:billing-events-0"
)

// --- InitialPartitionAssignments ----------------------------------------

func TestInitialPartitionAssignmentsIsScoped(t *testing.T) {
	inScope := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	outOfScope := clusterRow("west-1", map[string]string{prodLabel: "staging"})
	topics := &fakeTopics{rows: []*topic.KafkaTopic{
		shardOn(inScope, "billing-events-0", topic.StatePending, 1),
		shardOn(outOfScope, "audit-events-0", topic.StatePending, 1),
	}}
	svc := resourceprovider.NewService(
		&fakeClusters{rows: []*cluster.Cluster{inScope, outOfScope}}, topics)

	got, err := svc.InitialPartitionAssignments(agentCtx("gs-1", map[string]string{prodSelector: "prod"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d assignments, want 1 (only the in-scope cluster's)", len(got))
	}
	a := got[0]
	if a.TopicName != "billing-events-0" || a.ClusterName != "east-1" {
		t.Fatalf("assignment = %+v", a)
	}
	if a.Change != resource.ChangeSet {
		t.Errorf("change = %s, want SET", a.Change)
	}
	if a.ClusterFRN.Path() != "default:kafka-cluster:east-1" {
		t.Errorf("cluster_frn = %q", a.ClusterFRN.Path())
	}
	if len(a.ConnectionStrings) != 1 || a.ConnectionStrings[0].BootstrapURLs[0] != "east-1:9092" {
		t.Errorf("connection strings = %+v", a.ConnectionStrings)
	}
	if a.DesiredConfig["retention.ms"] != "604800000" || a.Partitions != 3 || a.ReplicationFactor != 1 {
		t.Errorf("desired state missing on SET: %+v", a)
	}
}

func TestInScopeClustersListsScopeEvenWithNoPlacedShards(t *testing.T) {
	inScope := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	alsoInScope := clusterRow("east-2", map[string]string{prodLabel: "prod"})
	outOfScope := clusterRow("west-1", map[string]string{prodLabel: "staging"})
	svc := resourceprovider.NewService(
		&fakeClusters{rows: []*cluster.Cluster{outOfScope, alsoInScope, inScope}},
		&fakeTopics{})

	got, err := svc.InScopeClusters(agentCtx("gs-1", map[string]string{prodSelector: "prod"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "east-1" || got[1].Name != "east-2" {
		t.Fatalf("scope = %+v, want [east-1 east-2] sorted by name", got)
	}
	if got[0].FRN.Path() != "default:kafka-cluster:east-1" {
		t.Errorf("frn = %q", got[0].FRN.Path())
	}
	if len(got[0].ConnectionStrings) != 1 || got[0].ConnectionStrings[0].BootstrapURLs[0] != "east-1:9092" {
		t.Errorf("connection strings = %+v", got[0].ConnectionStrings)
	}
}

func TestInScopeClustersEmptyWhenSelectorIsEmpty(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, &fakeTopics{})

	got, err := svc.InScopeClusters(agentCtx("gs-1", map[string]string{"team": "platform"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %+v, want none", got)
	}
}

func TestInitialPartitionAssignmentsEmptySelectorIsInert(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	topics := &fakeTopics{rows: []*topic.KafkaTopic{shardOn(c, "billing-events-0", topic.StatePending, 1)}}
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, topics)

	got, err := svc.InitialPartitionAssignments(agentCtx("gs-1", map[string]string{"team": "platform"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("an agent with no franz.placement-selector/* labels must be inert, got %d", len(got))
	}
}

func TestInitialPartitionAssignmentsMapsStateToChange(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	topics := &fakeTopics{rows: []*topic.KafkaTopic{
		shardOn(c, "a-0", topic.StateReady, 1),
		shardOn(c, "b-0", topic.StatePaused, 1),
		shardOn(c, "c-0", topic.StateDeleted, 1),
		shardOn(c, "d-0", topic.StateError, 1),
	}}
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, topics)

	got, err := svc.InitialPartitionAssignments(agentCtx("gs-1", map[string]string{prodSelector: "prod"}))
	if err != nil {
		t.Fatal(err)
	}
	byTopic := map[string]resource.PartitionAssignment{}
	for _, a := range got {
		byTopic[a.TopicName] = a
	}
	for topicName, want := range map[string]resource.Change{
		"a-0": resource.ChangeSet,
		"b-0": resource.ChangePaused,
		"c-0": resource.ChangeRemoved,
		"d-0": resource.ChangeSet,
	} {
		if got := byTopic[topicName].Change; got != want {
			t.Errorf("%s: change = %s, want %s", topicName, got, want)
		}
	}
	// PAUSED and REMOVED must carry no desired state — the agent must not act on it.
	if a := byTopic["b-0"]; a.DesiredConfig != nil || a.Partitions != 0 || a.ReplicationFactor != 0 {
		t.Errorf("PAUSED assignment carries desired state: %+v", a)
	}
	if a := byTopic["c-0"]; a.DesiredConfig != nil || a.Partitions != 0 {
		t.Errorf("REMOVED assignment carries desired state: %+v", a)
	}
}

// --- ReportReconciliation -------------------------------------------------

func TestReportReconciliationAppliesInScopeReport(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	shard := shardOn(c, "billing-events-0", topic.StatePending, 5)
	topics := &fakeTopics{rows: []*topic.KafkaTopic{shard}}
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, topics)

	applied, err := svc.ReportReconciliation(
		agentCtx("gs-1", map[string]string{prodSelector: "prod"}),
		in.ReportReconciliationInput{
			PartitionFRNPath: partitionFRN0,
			Generation:       5,
			Outcome:          topic.OutcomeCreated,
			Applied:          &topic.AppliedState{Partitions: 3, ReplicationFactor: 1},
		})
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	if shard.State != topic.StateReady {
		t.Errorf("state = %s, want READY", shard.State)
	}
	if shard.ReconciledGeneration == nil || *shard.ReconciledGeneration != 5 {
		t.Errorf("reconciled_generation = %v, want 5", shard.ReconciledGeneration)
	}
}

func TestReportReconciliationStaleGenerationIsANoOp(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	shard := shardOn(c, "billing-events-0", topic.StatePending, 9)
	topics := &fakeTopics{rows: []*topic.KafkaTopic{shard}}
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, topics)

	applied, err := svc.ReportReconciliation(
		agentCtx("gs-1", map[string]string{prodSelector: "prod"}),
		in.ReportReconciliationInput{
			PartitionFRNPath: partitionFRN0, Generation: 8, Outcome: topic.OutcomeCreated,
		})
	if err != nil {
		t.Fatalf("a stale report must be acknowledged, not rejected: %v", err)
	}
	if applied {
		t.Error("applied = true, want false")
	}
	if shard.State != topic.StatePending {
		t.Errorf("state = %s, want PENDING — a stale report must not move the row to READY", shard.State)
	}
}

func TestReportReconciliationDeniesOutOfScopeAgent(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	shard := shardOn(c, "billing-events-0", topic.StatePending, 1)
	topics := &fakeTopics{rows: []*topic.KafkaTopic{shard}}
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, topics)

	cases := []struct {
		name   string
		labels map[string]string
	}{
		{name: "different value", labels: map[string]string{prodSelector: "staging"}},
		{name: "no selector at all", labels: map[string]string{"team": "platform"}},
		{name: "narrower selector the cluster does not satisfy",
			labels: map[string]string{prodSelector: "prod", orgSelector: "payments"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.ReportReconciliation(agentCtx("intruder", tc.labels),
				in.ReportReconciliationInput{
					PartitionFRNPath: partitionFRN0, Generation: 1, Outcome: topic.OutcomeCreated,
				})
			if errs.KindOf(err) != errs.PermissionDenied {
				t.Fatalf("err = %v, want PERMISSION_DENIED", err)
			}
			if shard.State != topic.StatePending {
				t.Fatalf("state = %s, want PENDING — a denied report must change nothing", shard.State)
			}
		})
	}
}

func TestReportReconciliationAcceptsAWiderScope(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod", orgLabel: "payments"})
	shard := shardOn(c, "billing-events-0", topic.StatePending, 1)
	topics := &fakeTopics{rows: []*topic.KafkaTopic{shard}}
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, topics)

	// The agent selects on env only; the cluster's extra franz.placement/org is
	// ignored, so it is in scope.
	applied, err := svc.ReportReconciliation(agentCtx("gs-1", map[string]string{prodSelector: "prod"}),
		in.ReportReconciliationInput{
			PartitionFRNPath: partitionFRN0, Generation: 1, Outcome: topic.OutcomeNoop,
		})
	if err != nil || !applied {
		t.Fatalf("applied = %v, err = %v; want true, nil", applied, err)
	}
}

func TestReportReconciliationErrorOutcomeStoresTheMessage(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	shard := shardOn(c, "billing-events-0", topic.StatePending, 1)
	topics := &fakeTopics{rows: []*topic.KafkaTopic{shard}}
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, topics)

	const detail = "topic has unconsumed data (partition 0: earliest=0 latest=45201)"
	applied, err := svc.ReportReconciliation(agentCtx("gs-1", map[string]string{prodSelector: "prod"}),
		in.ReportReconciliationInput{
			PartitionFRNPath: partitionFRN0, Generation: 1,
			Outcome: topic.OutcomeError, Message: detail,
		})
	if err != nil || !applied {
		t.Fatalf("applied = %v, err = %v; want true, nil", applied, err)
	}
	if shard.State != topic.StateError {
		t.Errorf("state = %s, want ERROR", shard.State)
	}
	if shard.LastReconcileMessage != detail {
		t.Errorf("last_reconcile_message = %q, want %q", shard.LastReconcileMessage, detail)
	}
}

func TestReportReconciliationUnknownPartitionIsNotFound(t *testing.T) {
	c := clusterRow("east-1", map[string]string{prodLabel: "prod"})
	svc := resourceprovider.NewService(&fakeClusters{rows: []*cluster.Cluster{c}}, &fakeTopics{})

	_, err := svc.ReportReconciliation(agentCtx("gs-1", map[string]string{prodSelector: "prod"}),
		in.ReportReconciliationInput{
			PartitionFRNPath: "default:kafka-topic:nope-0", Generation: 1, Outcome: topic.OutcomeNoop,
		})
	if errs.KindOf(err) != errs.NotFound {
		t.Fatalf("err = %v, want NOT_FOUND", err)
	}
}
