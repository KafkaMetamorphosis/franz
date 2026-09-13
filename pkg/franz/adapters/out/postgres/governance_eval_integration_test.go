package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/channels"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/topics"
)

// governanceFixture is the governance wire over a real database: the three
// governance repositories, the three entity repositories and services the
// applier writes through, and the service + evaluator on top.
type governanceFixture struct {
	db    *postgres.DB
	realm realm.Realm
	ctx   context.Context
	svc   *governance.Service
	eval  *governance.Evaluator

	channelSvc in.AsyncChannelService
	clusterSvc in.KafkaClusterService
}

func newGovernanceFixture(t *testing.T) governanceFixture {
	t.Helper()
	db := openTestDB(t)
	cleanupGovernance(t, db)
	cleanupClusters(t, db)
	r := seededRealm(t, db)

	policyRepo := postgres.NewPolicyRepo(db)
	indicatorRepo := postgres.NewIndicatorRepo(db)
	actionRepo := postgres.NewPolicyActionRepo(db)
	sampleRepo := postgres.NewIndicatorSampleRepo(db)
	channelRepo := postgres.NewChannelRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)
	topicRepo := postgres.NewTopicRepo(db)

	// nil placer / notifier / publisher: this fixture exercises governance's own
	// writes, not placement or the agent wire. The provider-status reader is real
	// because clusters.Service.Get always consults it.
	channelSvc := channels.NewService(channelRepo, nil, nil)
	clusterSvc := clusters.NewService(clusterRepo, topicRepo,
		postgres.NewProviderEventRepo(db), nil, nil, nil)
	topicSvc := topics.NewService(topicRepo, clusterRepo, nil)

	return governanceFixture{
		db:    db,
		realm: r,
		ctx:   realm.NewContext(context.Background(), r),
		svc: governance.NewService(policyRepo, indicatorRepo, actionRepo, sampleRepo,
			channelRepo, clusterRepo, topicRepo),
		eval: governance.NewEvaluator(policyRepo, indicatorRepo, actionRepo,
			channelRepo, clusterRepo, topicRepo,
			channelSvc, clusterSvc, topicSvc, nil,
			slog.New(slog.NewTextHandler(io.Discard, nil))),
		channelSvc: channelSvc,
		clusterSvc: clusterSvc,
	}
}

// TestGovernanceEndToEndOnAChannel walks the whole 003.8 loop against a real
// database: register an indicator, save a policy, feed the evaluator a
// limit-crossing value, and check the channel actually moved and the change was
// audited.
func TestGovernanceEndToEndOnAChannel(t *testing.T) {
	f := newGovernanceFixture(t)

	if _, err := f.svc.CreateIndicator(f.ctx, in.CreateIndicatorInput{
		Name: "consumer-lag", Unit: indicator.UnitCount,
		AppliesTo: indicator.EntityAsyncChannel, StalenessThreshold: "1h",
	}); err != nil {
		t.Fatalf("CreateIndicator: %v", err)
	}

	orders, err := f.channelSvc.Create(f.ctx, in.CreateChannelInput{
		Name: "orders", Type: channel.TypeKafkaTopic, ChannelPartitions: 2,
		Labels: map[string]string{"tier": "gold"},
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if _, err := f.svc.CreatePolicy(f.ctx, in.CreatePolicyInput{
		Name: "pause-lagging-gold",
		Definition: gov.Definition{
			Indicator: "consumer-lag",
			Matcher:   gov.Matcher{Entity: indicator.EntityAsyncChannel, Selector: "tier=gold"},
			Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "10000"},
			Actions: []gov.Action{
				{Kind: gov.ActionAddLabel, Args: []string{"incident", "lag"}},
				{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}},
			},
		},
		Weight: 10, Enabled: true,
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	// The evaluator reads the indicator's own freshness, so the projection has to
	// have been advanced — which is what ingest does before calling Evaluate.
	sampleAt := time.Now().UTC()
	indicatorRepo := postgres.NewIndicatorRepo(f.db)
	advanced, err := indicatorRepo.RecordSample(f.ctx, f.realm.ID, "consumer-lag",
		orders.FRN.Path(), "50000", sampleAt)
	if err != nil || !advanced {
		t.Fatalf("RecordSample: advanced=%v err=%v", advanced, err)
	}

	if err := f.eval.Evaluate(f.ctx, "consumer-lag", orders.FRN.Path(), "50000"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// The declared state moved: both actions applied, in order.
	got, err := f.channelSvc.Get(f.ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != channel.StatePaused {
		t.Errorf("state = %s, want PAUSED", got.State)
	}
	if got.Labels["incident"] != "lag" {
		t.Errorf("labels = %v, want the incident label", got.Labels)
	}

	// Every automated change is a PolicyAction — one per action (003.8).
	page, err := f.svc.ListPolicyActions(f.ctx, in.ListPolicyActionsInput{
		PolicyName: "pause-lagging-gold", PageSize: 10,
	})
	if err != nil {
		t.Fatalf("ListPolicyActions: %v", err)
	}
	if len(page.Actions) != 2 {
		t.Fatalf("PolicyActions = %d, want 2", len(page.Actions))
	}
	// Newest first, so the SET_STATUS that ran second comes back first.
	if page.Actions[0].Action.Kind != gov.ActionSetStatus || page.Actions[0].Result != "state=PAUSED" {
		t.Errorf("newest record = %+v", page.Actions[0])
	}
	if page.Actions[1].Action.Kind != gov.ActionAddLabel || page.Actions[1].Result != "incident=lag" {
		t.Errorf("older record = %+v", page.Actions[1])
	}
	for _, rec := range page.Actions {
		if rec.ResourceFRN != orders.FRN.Path() || rec.IndicatorValue != "50000" {
			t.Errorf("record subject = (%s, %s)", rec.ResourceFRN, rec.IndicatorValue)
		}
	}

	stored, err := f.svc.GetPolicy(f.ctx, "pause-lagging-gold")
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastFiredAt == nil {
		t.Error("a triggered policy must stamp last_fired_at")
	}

	// The indicator is now referenced, so it cannot be deleted out from under the
	// policy.
	if err := f.svc.DeleteIndicator(f.ctx, "consumer-lag"); err == nil {
		t.Error("DeleteIndicator must refuse while a policy references it")
	}
}

// TestGovernanceEndToEndOnACluster covers the cluster half: an arithmetic action
// with a cap, applied through the cluster service against a real row.
func TestGovernanceEndToEndOnACluster(t *testing.T) {
	f := newGovernanceFixture(t)

	if _, err := f.svc.CreateIndicator(f.ctx, in.CreateIndicatorInput{
		Name: "disk-used", Unit: indicator.UnitBytes,
		AppliesTo: indicator.EntityKafkaCluster, StalenessThreshold: "1h",
	}); err != nil {
		t.Fatalf("CreateIndicator: %v", err)
	}

	east, err := f.clusterSvc.Create(f.ctx, in.CreateClusterInput{
		Name:              "east-1",
		ConnectionStrings: []cluster.ConnectionString{{BootstrapURLs: []string{"east-1:9092"}, Type: cluster.ConnectionPlaintext}},
		Labels:            map[string]string{"env": "prod"},
		Brokers:           3,
		DiskSize:          "100Gi",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	if _, err := f.svc.CreatePolicy(f.ctx, in.CreatePolicyInput{
		Name: "scale-out-full-clusters",
		Definition: gov.Definition{
			Indicator: "disk-used",
			Matcher:   gov.Matcher{Entity: indicator.EntityKafkaCluster, Selector: "env=prod"},
			Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "150Gi"},
			Actions: []gov.Action{
				{Kind: gov.ActionIncreaseFieldBy, Args: []string{"brokers", "2", "max=4"}},
			},
		},
		Enabled: true,
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	indicatorRepo := postgres.NewIndicatorRepo(f.db)
	if _, err := indicatorRepo.RecordSample(f.ctx, f.realm.ID, "disk-used",
		east.FRN.Path(), "200Gi", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := f.eval.Evaluate(f.ctx, "disk-used", east.FRN.Path(), "200Gi"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// 3 + 2 is 5, clamped to the declared ceiling of 4.
	got, err := f.clusterSvc.Get(f.ctx, "east-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Brokers != 4 {
		t.Fatalf("brokers = %d, want 4 (clamped by max=4)", got.Brokers)
	}

	page, err := f.svc.ListPolicyActions(f.ctx, in.ListPolicyActionsInput{
		PolicyName: "scale-out-full-clusters", PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 1 {
		t.Fatalf("PolicyActions = %d, want 1", len(page.Actions))
	}
	if page.Actions[0].Result != "brokers=4 (capped at max=4)" {
		t.Fatalf("result = %q, want it to name the cap that bit", page.Actions[0].Result)
	}
}

// TestDryRunAgainstARealStoreMutatesNothing is the 003.8 dry-run contract at the
// persistence level: after a run that reports would_trigger, the resource and
// the audit series are both untouched.
func TestDryRunAgainstARealStoreMutatesNothing(t *testing.T) {
	f := newGovernanceFixture(t)

	if _, err := f.svc.CreateIndicator(f.ctx, in.CreateIndicatorInput{
		Name: "consumer-lag", Unit: indicator.UnitCount,
		AppliesTo: indicator.EntityAsyncChannel, StalenessThreshold: "1h",
	}); err != nil {
		t.Fatal(err)
	}
	orders, err := f.channelSvc.Create(f.ctx, in.CreateChannelInput{
		Name: "orders", Type: channel.TypeKafkaTopic, ChannelPartitions: 1,
		Labels: map[string]string{"tier": "gold"},
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	sample, err := indicator.NewSample(f.realm.ID, "consumer-lag", orders.FRN.Path(),
		indicator.EntityAsyncChannel, "50000", "agent-a", now, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.NewIndicatorSampleRepo(f.db).Append(f.ctx, []*indicator.Sample{sample}); err != nil {
		t.Fatal(err)
	}

	matches, err := f.svc.DryRunPolicy(f.ctx, gov.Definition{
		Indicator: "consumer-lag",
		Matcher:   gov.Matcher{Entity: indicator.EntityAsyncChannel, Selector: "tier=gold"},
		Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "10000"},
		Actions:   []gov.Action{{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}}},
	})
	if err != nil {
		t.Fatalf("DryRunPolicy: %v", err)
	}
	if len(matches) != 1 || !matches[0].WouldTrigger || matches[0].IndicatorValue != "50000" {
		t.Fatalf("matches = %+v, want one would-trigger match", matches)
	}

	got, err := f.channelSvc.Get(f.ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != channel.StateActive {
		t.Fatalf("state = %s — a dry run must not mutate", got.State)
	}
	var auditRows int
	if err := f.db.Pool().QueryRow(f.ctx, `SELECT count(*) FROM policy_action`).Scan(&auditRows); err != nil {
		t.Fatal(err)
	}
	if auditRows != 0 {
		t.Fatalf("a dry run wrote %d PolicyAction row(s)", auditRows)
	}
	var policyRows int
	if err := f.db.Pool().QueryRow(f.ctx, `SELECT count(*) FROM policy`).Scan(&policyRows); err != nil {
		t.Fatal(err)
	}
	if policyRows != 0 {
		t.Fatalf("a dry run stored %d policy row(s)", policyRows)
	}
}
