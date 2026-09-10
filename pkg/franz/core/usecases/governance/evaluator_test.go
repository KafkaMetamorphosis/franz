package governance_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/governance"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// evalFixture wires the evaluator over in-memory fakes and records every write
// the applier makes through the entity services.
type evalFixture struct {
	eval       *governance.Evaluator
	policies   *fakePolicyRepo
	indicators *fakeIndicatorRepo
	actions    *fakeActionRepo
	channels   *fakeChannelRepo
	clusters   *fakeClusterRepo
	topics     *fakeTopicRepo
	calls      *[]serviceCall
}

func newEvalFixture(
	indicators []*indicator.Indicator, policies []*gov.Policy,
	channels map[string]*channel.AsyncChannel,
	clusters map[string]*cluster.Cluster,
	topics map[string]*topic.KafkaTopic,
) evalFixture {
	calls := &[]serviceCall{}
	f := evalFixture{
		policies:   newPolicyRepo(policies...),
		indicators: newIndicatorRepo(indicators...),
		actions:    &fakeActionRepo{},
		channels:   &fakeChannelRepo{rows: channels},
		clusters:   &fakeClusterRepo{rows: clusters},
		topics:     &fakeTopicRepo{rows: topics},
		calls:      calls,
	}
	f.eval = governance.NewEvaluator(
		f.policies, f.indicators, f.actions,
		f.channels, f.clusters, f.topics,
		fakeChannelSvc{repo: f.channels, calls: calls},
		fakeClusterSvc{repo: f.clusters, calls: calls},
		fakeTopicSvc{repo: f.topics, calls: calls},
		nil, quietLogger(),
	)
	return f
}

// policy builds an enabled policy over an existing definition.
func policy(name string, weight int32, def gov.Definition) *gov.Policy {
	p, err := gov.New(testRealm(), name, def, weight, true)
	if err != nil {
		panic(err)
	}
	return p
}

// TestEvaluateAppliesActionAndRecordsIt is the "Done when" check: a
// limit-crossing value applies the action and produces a PolicyAction.
func TestEvaluateAppliesActionAndRecordsIt(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	orders := testChannel("orders", map[string]string{"tier": "gold"})
	p := policy("pause-hot", 0, channelDefinition("lag", "tier=gold"))

	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{p},
		map[string]*channel.AsyncChannel{"orders": orders}, nil, nil)

	if err := f.eval.Evaluate(ctxWithRealm(), "lag", orders.FRN.Path(), "150"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if len(*f.calls) != 1 {
		t.Fatalf("service calls = %+v, want exactly one pause", *f.calls)
	}
	if got := (*f.calls)[0]; got.Entity != "channel" || got.Op != "pause" || got.Name != "orders" {
		t.Fatalf("call = %+v, want a channel pause on orders", got)
	}

	if len(f.actions.records) != 1 {
		t.Fatalf("PolicyActions = %d, want 1", len(f.actions.records))
	}
	rec := f.actions.records[0]
	if rec.PolicyName != "pause-hot" || rec.PolicyID != p.ID {
		t.Errorf("record identity = (%s, %s)", rec.PolicyName, rec.PolicyID)
	}
	if rec.ResourceFRN != orders.FRN.Path() || rec.IndicatorValue != "150" {
		t.Errorf("record subject = (%s, %s)", rec.ResourceFRN, rec.IndicatorValue)
	}
	if rec.Action.Kind != gov.ActionSetStatus || rec.Result != "state=PAUSED" {
		t.Errorf("record outcome = (%s, %q)", rec.Action.Kind, rec.Result)
	}

	if f.policies.rows["pause-hot"].LastFiredAt == nil {
		t.Error("a triggered policy must stamp last_fired_at")
	}
}

// TestEvaluateSkipsStaleIndicator is step 1 of 003.8 "Evaluation" and the other
// half of the "Done when" line: a STALE indicator is skipped entirely.
func TestEvaluateSkipsStaleIndicator(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	// The producer went quiet well beyond the 1h threshold.
	longAgo := time.Now().Add(-48 * time.Hour).UTC()
	ind.LastSampleAt = &longAgo

	orders := testChannel("orders", nil)
	p := policy("pause-hot", 0, channelDefinition("lag", ""))
	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{p},
		map[string]*channel.AsyncChannel{"orders": orders}, nil, nil)

	if err := f.eval.Evaluate(ctxWithRealm(), "lag", orders.FRN.Path(), "150"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(*f.calls) != 0 || len(f.actions.records) != 0 {
		t.Fatalf("a stale indicator acted: calls=%+v records=%d", *f.calls, len(f.actions.records))
	}
	if orders.State != channel.StateActive {
		t.Fatal("a stale indicator must leave the resource untouched")
	}
}

// TestEvaluateSkipsNeverSampledIndicator: "no data" is STALE, not HEALTHY. An
// indicator that has never been sampled has no value for a policy to compare.
func TestEvaluateSkipsNeverSampledIndicator(t *testing.T) {
	ind, err := indicator.NewIndicator(testRealm(), "lag", indicator.UnitCount,
		indicator.EntityAsyncChannel, "1h", nil)
	if err != nil {
		t.Fatal(err)
	}
	orders := testChannel("orders", nil)
	p := policy("pause-hot", 0, channelDefinition("lag", ""))
	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{p},
		map[string]*channel.AsyncChannel{"orders": orders}, nil, nil)

	if err := f.eval.Evaluate(ctxWithRealm(), "lag", orders.FRN.Path(), "150"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(f.actions.records) != 0 {
		t.Fatal("a never-sampled indicator must not act")
	}
}

// TestEvaluateSkipsDisabledPolicyAndNonMatchingSelector covers steps 2 and 3.
func TestEvaluateSkipsDisabledPolicyAndNonMatchingSelector(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	bronze := testChannel("telemetry", map[string]string{"tier": "bronze"})

	disabled := policy("disabled", 0, channelDefinition("lag", ""))
	disabled.Enabled = false
	unmatched := policy("gold-only", 0, channelDefinition("lag", "tier=gold"))
	belowLimit := policy("very-hot", 0, channelDefinition("lag", ""))
	belowLimit.Limit = gov.Limit{Operator: gov.OpGreaterThan, Value: "10000"}

	f := newEvalFixture([]*indicator.Indicator{ind},
		[]*gov.Policy{disabled, unmatched, belowLimit},
		map[string]*channel.AsyncChannel{"telemetry": bronze}, nil, nil)

	if err := f.eval.Evaluate(ctxWithRealm(), "lag", bronze.FRN.Path(), "150"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(*f.calls) != 0 || len(f.actions.records) != 0 {
		t.Fatalf("nothing should have triggered: calls=%+v records=%d", *f.calls, len(f.actions.records))
	}
}

// TestEvaluateConflictOrder is the last "Done when" check: several triggered
// policies on the same (resource, field) apply in (weight desc, name asc) order,
// last write wins, and every action is logged.
func TestEvaluateConflictOrder(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	orders := testChannel("orders", nil)

	label := func(value string) gov.Definition {
		def := channelDefinition("lag", "")
		def.Actions = []gov.Action{{Kind: gov.ActionAddLabel, Args: []string{"state", value}}}
		return def
	}

	// zulu and alpha share a weight, so they must resolve by name ascending;
	// heavy outranks both and therefore runs first.
	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{
		policy("zulu", 5, label("from-zulu")),
		policy("alpha", 5, label("from-alpha")),
		policy("heavy", 10, label("from-heavy")),
	}, map[string]*channel.AsyncChannel{"orders": orders}, nil, nil)

	if err := f.eval.Evaluate(ctxWithRealm(), "lag", orders.FRN.Path(), "150"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// Every action is logged, in application order.
	if len(f.actions.records) != 3 {
		t.Fatalf("PolicyActions = %d, want 3 — every action is logged", len(f.actions.records))
	}
	wantOrder := []string{"heavy", "alpha", "zulu"}
	for i, name := range wantOrder {
		if got := f.actions.records[i].PolicyName; got != name {
			t.Fatalf("record %d is from %q, want %q (order is weight desc, name asc)", i, got, name)
		}
	}
	// Last write wins: zulu ran last, so its value is the one on the resource.
	if got := orders.Labels["state"]; got != "from-zulu" {
		t.Fatalf("label state = %q, want from-zulu (last write wins)", got)
	}
	if len(f.policies.fired) != 3 {
		t.Fatalf("fired = %v, want all three stamped", f.policies.fired)
	}
}

// TestEvaluateRecordsFailureWithoutFailingThePass: one bad rule must not block
// telemetry ingest, and the failure must still be visible in the audit trail.
func TestEvaluateRecordsFailureWithoutFailingThePass(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	orders := testChannel("orders", nil)

	// A cluster-only action reaching a channel resource: the applier refuses it.
	broken := channelDefinition("lag", "")
	broken.Actions = []gov.Action{{Kind: gov.ActionUpdateField, Args: []string{"brokers", "5"}}}

	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{
		policy("broken", 10, broken),
		policy("healthy", 1, channelDefinition("lag", "")),
	}, map[string]*channel.AsyncChannel{"orders": orders}, nil, nil)

	if err := f.eval.Evaluate(ctxWithRealm(), "lag", orders.FRN.Path(), "150"); err != nil {
		t.Fatalf("one failing action must not fail the pass: %v", err)
	}
	if len(f.actions.records) != 2 {
		t.Fatalf("PolicyActions = %d, want 2 (the failure is recorded too)", len(f.actions.records))
	}
	if got := f.actions.records[0].Result; got == "" || got[:8] != "failed: " {
		t.Fatalf("failed action result = %q, want a 'failed: ...' note", got)
	}
	// The healthy policy still ran.
	if len(*f.calls) != 1 || (*f.calls)[0].Op != "pause" {
		t.Fatalf("calls = %+v, want the healthy policy to still have paused", *f.calls)
	}
}

// TestEvaluateAppliesCapOnTopicFields exercises the arithmetic + cap path end to
// end on a shard, which is where OQ1's encoding actually bites.
func TestEvaluateAppliesCapOnTopicFields(t *testing.T) {
	ind := registeredIndicator("throughput", indicator.UnitCount, indicator.EntityKafkaTopic)
	shard := testShard("orders", 60)

	def := gov.Definition{
		Indicator: "throughput",
		Matcher:   gov.Matcher{Entity: indicator.EntityKafkaTopic},
		Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "1000"},
		Actions: []gov.Action{
			{Kind: gov.ActionIncreaseFieldBy, Args: []string{"partitions", "100%", "max=64"}},
		},
	}
	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{policy("grow", 0, def)},
		nil, nil, map[string]*topic.KafkaTopic{"orders-0": shard})

	if err := f.eval.Evaluate(ctxWithRealm(), "throughput", shard.FRN.Path(), "5000"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	// 60 doubled is 120, clamped to the declared ceiling of 64.
	if shard.Partitions != 64 {
		t.Fatalf("partitions = %d, want 64 (clamped by max=64)", shard.Partitions)
	}
	if len(f.actions.records) != 1 {
		t.Fatalf("PolicyActions = %d, want 1", len(f.actions.records))
	}
	if got := f.actions.records[0].Result; got != "partitions=64 (capped at max=64)" {
		t.Fatalf("result = %q, want it to name the cap that bit", got)
	}

	// Firing again is a no-op: the field is already at its ceiling, which is the
	// intended steady state given there is no cooldown (003.8 OQ2).
	if err := f.eval.Evaluate(ctxWithRealm(), "throughput", shard.FRN.Path(), "5000"); err != nil {
		t.Fatalf("second Evaluate: %v", err)
	}
	if shard.Partitions != 64 {
		t.Fatalf("partitions = %d after a second pass, want 64", shard.Partitions)
	}
	if got := f.actions.records[1].Result; got != "no change" {
		t.Fatalf("second result = %q, want \"no change\"", got)
	}
}

// TestEvaluateOnClusterBrokers covers the cluster arithmetic path through the
// cluster service.
func TestEvaluateOnClusterBrokers(t *testing.T) {
	ind := registeredIndicator("disk-used", indicator.UnitBytes, indicator.EntityKafkaCluster)
	east, err := cluster.New(testRealm(), "east-1",
		[]cluster.ConnectionString{{BootstrapURLs: []string{"b:9092"}, Type: cluster.ConnectionPlaintext}},
		map[string]string{"env": "prod"}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := east.SetShape(3, "100Gi"); err != nil {
		t.Fatal(err)
	}

	def := gov.Definition{
		Indicator: "disk-used",
		Matcher:   gov.Matcher{Entity: indicator.EntityKafkaCluster, Selector: "env=prod"},
		Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "150Gi"},
		Actions: []gov.Action{
			{Kind: gov.ActionIncreaseFieldBy, Args: []string{"brokers", "2", "max=9"}},
		},
	}
	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{policy("scale-out", 0, def)},
		nil, map[string]*cluster.Cluster{"east-1": east}, nil)

	if err := f.eval.Evaluate(ctxWithRealm(), "disk-used", east.FRN.Path(), "200Gi"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if east.Brokers != 5 {
		t.Fatalf("brokers = %d, want 5", east.Brokers)
	}
	if got := f.actions.records[0].Result; got != "brokers=5" {
		t.Fatalf("result = %q", got)
	}
}

// TestEvaluateResolvesClusterSubResource: 005 ADR §2.1 samples per-broker FRNs,
// which are not Franz resources. The governed parent must still be found.
func TestEvaluateResolvesClusterSubResource(t *testing.T) {
	ind := registeredIndicator("broker-disk", indicator.UnitBytes, indicator.EntityKafkaCluster)
	east, err := cluster.New(testRealm(), "east-1",
		[]cluster.ConnectionString{{BootstrapURLs: []string{"b:9092"}, Type: cluster.ConnectionPlaintext}},
		nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}

	def := gov.Definition{
		Indicator: "broker-disk",
		Matcher:   gov.Matcher{Entity: indicator.EntityKafkaCluster},
		Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "150Gi"},
		Actions:   []gov.Action{{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}}},
	}
	f := newEvalFixture([]*indicator.Indicator{ind}, []*gov.Policy{policy("pause-full", 0, def)},
		nil, map[string]*cluster.Cluster{"east-1": east}, nil)

	subResource := east.FRN.Path() + "/broker/3"
	if err := f.eval.Evaluate(ctxWithRealm(), "broker-disk", subResource, "200Gi"); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(*f.calls) != 1 || (*f.calls)[0].Op != "pause" || (*f.calls)[0].Name != "east-1" {
		t.Fatalf("calls = %+v, want a pause on the parent cluster", *f.calls)
	}
}

// TestNoopEvaluatorDoesNothing pins the zero-value port used where governance is
// not wired.
func TestNoopEvaluatorDoesNothing(t *testing.T) {
	if err := (governance.NoopEvaluator{}).Evaluate(
		ctxWithRealm(), "lag", "default:async-channel:orders", "1"); err != nil {
		t.Fatalf("NoopEvaluator: %v", err)
	}
}

// testShard builds shard 0 of the named channel. topic.New derives the shard's
// own name as "<channel>-<index>", so the caller names the channel.
func testShard(channelName string, partitions int32) *topic.KafkaTopic {
	t, err := topic.New(testRealm(), uuid.New(), channelName, 0, nil, nil, partitions, 3)
	if err != nil {
		panic(err)
	}
	return t
}
