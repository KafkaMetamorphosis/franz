package telemetry_test

import (
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/telemetry"
)

const clusterFRN = "default:kafka-cluster:east-1"

// fixture wires the service to fresh fakes and hands the test the fakes it
// asserts on.
type fixture struct {
	svc        *telemetry.Service
	samples    *fakeSampleRepo
	indicators *fakeIndicatorRepo
	groups     *fakeGroupRepo
	evaluator  *fakeEvaluator
}

func newFixture(indicators ...*indicator.Indicator) *fixture {
	f := &fixture{
		samples:    &fakeSampleRepo{},
		indicators: newIndicatorRepo(indicators...),
		groups:     &fakeGroupRepo{},
		evaluator:  &fakeEvaluator{},
	}
	f.svc = telemetry.NewService(
		f.samples, f.indicators, f.groups, f.evaluator, discardLogger())
	return f
}

// brokerCount is the registered indicator most of these tests publish to.
func brokerCount() *indicator.Indicator {
	return registered("kafka.cluster.broker_count",
		indicator.UnitCount, indicator.EntityKafkaCluster)
}

func sample(name, value string, at time.Time) indicator.Sample {
	return indicator.Sample{
		Indicator:      name,
		ResourceFRN:    clusterFRN,
		ResourceEntity: indicator.EntityKafkaCluster,
		Value:          value,
		SampleAt:       at,
	}
}

// 003.14 "Indicators are pre-registered": no auto-creation, and the rejection is
// FAILED_PRECONDITION because CreateIndicator is the fix.
func TestIngestRejectsUnregisteredIndicator(t *testing.T) {
	f := newFixture()

	_, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "3", time.Now())})

	if errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v (err %v), want FailedPrecondition", errs.KindOf(err), err)
	}
	if len(f.samples.appended) != 0 {
		t.Errorf("a rejected batch must write nothing, wrote %d", len(f.samples.appended))
	}
}

// The whole batch is rejected, not just the offending row: the response carries
// a count and nothing else, so a partial accept would leave the agent unable to
// tell which samples it still owes.
func TestIngestRejectsWholeBatchOnOneBadSample(t *testing.T) {
	f := newFixture(brokerCount())
	now := time.Now()

	_, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"), []indicator.Sample{
		sample("kafka.cluster.broker_count", "3", now),
		sample("kafka.cluster.ghost", "1", now),
		sample("kafka.cluster.broker_count", "4", now),
	})

	if errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v (err %v)", errs.KindOf(err), err)
	}
	if len(f.samples.appended) != 0 {
		t.Fatalf("the good rows must not land either, wrote %d", len(f.samples.appended))
	}
	if len(f.evaluator.calls) != 0 {
		t.Errorf("a rejected batch must not evaluate, got %v", f.evaluator.calls)
	}
}

// A sample whose resource_entity disagrees with the indicator's applies_to is
// rejected (003.14) — the sample describes a kind of resource no policy on this
// indicator could match.
func TestIngestRejectsEntityMismatch(t *testing.T) {
	f := newFixture(brokerCount())

	bad := sample("kafka.cluster.broker_count", "3", time.Now())
	bad.ResourceEntity = indicator.EntityKafkaTopic
	bad.ResourceFRN = "default:kafka-topic:orders-0"

	_, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"), []indicator.Sample{bad})

	if errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v (err %v), want InvalidArgument", errs.KindOf(err), err)
	}
	if len(f.samples.appended) != 0 {
		t.Errorf("wrote %d rows on a rejected batch", len(f.samples.appended))
	}
}

// `value` must parse in the indicator's unit (003.14). Franz validates, it does
// not bound.
func TestIngestRejectsUnparseableValue(t *testing.T) {
	f := newFixture(brokerCount())

	_, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "three", time.Now())})

	if errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v (err %v), want InvalidArgument", errs.KindOf(err), err)
	}
	if len(f.samples.appended) != 0 {
		t.Errorf("wrote %d rows on a rejected batch", len(f.samples.appended))
	}
}

// A categorical indicator accepts its labels — 005 ADR §2.1's `kafka.topic.state`
// is an enum, and rejecting "provisioned" would make Gregor Samsa's structural
// sweep unpublishable.
func TestIngestAcceptsEnumValues(t *testing.T) {
	state := registered("kafka.topic.state", indicator.UnitEnum, indicator.EntityKafkaTopic)
	f := newFixture(state)

	accepted, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"), []indicator.Sample{{
		Indicator:      "kafka.topic.state",
		ResourceFRN:    "default:kafka-topic:orders-0",
		ResourceEntity: indicator.EntityKafkaTopic,
		Value:          "provisioned",
		SampleAt:       time.Now(),
	}})
	if err != nil {
		t.Fatalf("IngestSamples: %v", err)
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1", accepted)
	}
}

// 003.14 "Governance coupling" / 003.8 OQ4: a sample that advances the current
// value triggers exactly one evaluation, carrying the new value.
func TestIngestFiresEvalOncePerAdvancingSample(t *testing.T) {
	f := newFixture(brokerCount())
	now := time.Now()

	if _, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"), []indicator.Sample{
		sample("kafka.cluster.broker_count", "3", now),
		sample("kafka.cluster.broker_count", "4", now.Add(time.Second)),
	}); err != nil {
		t.Fatalf("IngestSamples: %v", err)
	}

	if len(f.evaluator.calls) != 2 {
		t.Fatalf("evaluations = %d, want one per advancing sample", len(f.evaluator.calls))
	}
	last := f.evaluator.calls[1]
	if last.indicator != "kafka.cluster.broker_count" ||
		last.resourceFRN != clusterFRN || last.value != "4" {
		t.Errorf("last evaluation = %+v", last)
	}
}

// 003.14 "Out-of-order": a sample older than the current latest is stored (it is
// history) but does not become current and does not trigger evaluation.
func TestIngestStoresOutOfOrderSampleWithoutEvaluating(t *testing.T) {
	f := newFixture(brokerCount())
	now := time.Now()

	if _, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "4", now)}); err != nil {
		t.Fatal(err)
	}
	if len(f.evaluator.calls) != 1 {
		t.Fatalf("the first sample must evaluate, got %d", len(f.evaluator.calls))
	}

	accepted, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "1", now.Add(-time.Hour))})
	if err != nil {
		t.Fatalf("a late sample is history, not an error: %v", err)
	}

	if accepted != 1 || len(f.samples.appended) != 2 {
		t.Errorf("the late sample must still be stored (accepted %d, rows %d)",
			accepted, len(f.samples.appended))
	}
	if len(f.evaluator.calls) != 1 {
		t.Errorf("evaluations = %d, want no second one", len(f.evaluator.calls))
	}
	// The late sample was still offered to the projection — the store, not the
	// service, decides it does not advance, so the ordering rule holds under
	// concurrent ingest too.
	if len(f.indicators.recorded) != 2 {
		t.Errorf("RecordSample calls = %d, want one per stored sample",
			len(f.indicators.recorded))
	}
	current := f.indicators.rows["kafka.cluster.broker_count"]
	if current.CurrentValue != "4" {
		t.Errorf("current value = %q, want the newer sample's 4", current.CurrentValue)
	}
}

// A failing evaluation must not fail ingest: the samples are already durable,
// and one bad rule cannot be allowed to stop the fleet reporting.
func TestIngestSurvivesEvaluationFailure(t *testing.T) {
	f := newFixture(brokerCount())
	f.evaluator.err = errStoreDown

	accepted, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "3", time.Now())})
	if err != nil {
		t.Fatalf("IngestSamples: %v", err)
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1", accepted)
	}
	if len(f.evaluator.calls) != 1 {
		t.Errorf("the hook must still have fired, got %d calls", len(f.evaluator.calls))
	}
}

// The same reasoning for the projection write: a failed RecordSample loses the
// current-value advance (the next sample restores it) but never the samples.
func TestIngestSurvivesRecordSampleFailure(t *testing.T) {
	f := newFixture(brokerCount())
	f.indicators.recordErr = errStoreDown

	accepted, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "3", time.Now())})
	if err != nil {
		t.Fatalf("IngestSamples: %v", err)
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1", accepted)
	}
	if len(f.evaluator.calls) != 0 {
		t.Errorf("nothing advanced, so nothing must evaluate: %v", f.evaluator.calls)
	}
}

// A store that cannot take the batch fails the call — unlike the projection and
// the eval hook, the append is the point of the RPC, and the agent has to know
// to retry.
func TestIngestPropagatesAppendFailure(t *testing.T) {
	f := newFixture(brokerCount())
	f.samples.appendErr = errStoreDown

	if _, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "3", time.Now())}); err == nil {
		t.Fatal("expected the append failure to surface")
	}
	if len(f.evaluator.calls) != 0 {
		t.Errorf("nothing was stored, so nothing must evaluate: %v", f.evaluator.calls)
	}
}

// The authenticated identity is authoritative: the row is attributed to the
// agent in context, never to whatever the request claimed.
func TestIngestAttributesToAuthenticatedAgent(t *testing.T) {
	f := newFixture(brokerCount())

	if _, err := f.svc.IngestSamples(ctxWithAgent("odradek-prod"),
		[]indicator.Sample{sample("kafka.cluster.broker_count", "3", time.Now())}); err != nil {
		t.Fatal(err)
	}
	if got := f.samples.appended[0].ReportingAgent; got != "odradek-prod" {
		t.Fatalf("ReportingAgent = %q", got)
	}
}

// An empty batch is a no-op so a client stream can send a keepalive, and an
// over-long one is rejected before it reaches the store (003.14 OQ2).
func TestIngestBatchBounds(t *testing.T) {
	f := newFixture(brokerCount())

	if n, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"), nil); n != 0 || err != nil {
		t.Fatalf("empty batch = (%d, %v), want (0, nil)", n, err)
	}

	huge := make([]indicator.Sample, 1001)
	for i := range huge {
		huge[i] = sample("kafka.cluster.broker_count", "3", time.Now())
	}
	if _, err := f.svc.IngestSamples(ctxWithAgent("gregor-samsa"), huge); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("over-long batch kind = %v", errs.KindOf(err))
	}
	if len(f.samples.appended) != 0 {
		t.Errorf("wrote %d rows", len(f.samples.appended))
	}
}

// --- consumer groups -------------------------------------------------------

// Sightings are read-only context (003.14): they land in the series and trigger
// no evaluation.
func TestIngestConsumerGroups(t *testing.T) {
	f := newFixture()
	observed := time.Now().Add(-time.Minute)

	accepted, err := f.svc.IngestConsumerGroups(ctxWithAgent("odradek-prod"),
		[]consumergroup.Observation{
			{
				Group: "billing.orders-0", ClientFRN: "default:client:billing",
				Owner: "payments", AsyncChannel: "orders", KafkaTopic: "orders-0",
				ObservedAt: observed,
			},
			{
				Group: "legacy-batch-reader", ClientFRN: "default:client:billing",
				KafkaTopic: "orders-0", ObservedAt: observed,
			},
		})
	if err != nil {
		t.Fatalf("IngestConsumerGroups: %v", err)
	}
	if accepted != 2 {
		t.Fatalf("accepted = %d, want 2", accepted)
	}
	if f.groups.appended[0].Custom {
		t.Error("the default <client>.<topic> form must not be flagged custom")
	}
	if !f.groups.appended[1].Custom {
		t.Error("an operator-chosen name must be flagged custom")
	}
	if got := f.groups.appended[0].ReportingAgent; got != "odradek-prod" {
		t.Errorf("ReportingAgent = %q", got)
	}
	if len(f.evaluator.calls) != 0 {
		t.Errorf("observations must not trigger governance: %v", f.evaluator.calls)
	}
}

func TestIngestConsumerGroupsRejectsBlankGroup(t *testing.T) {
	f := newFixture()

	_, err := f.svc.IngestConsumerGroups(ctxWithAgent("odradek-prod"),
		[]consumergroup.Observation{{KafkaTopic: "orders-0", ObservedAt: time.Now()}})

	if errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v (err %v)", errs.KindOf(err), err)
	}
	if len(f.groups.appended) != 0 {
		t.Errorf("wrote %d rows on a rejected batch", len(f.groups.appended))
	}
}

func TestIngestConsumerGroupsPropagatesAppendFailure(t *testing.T) {
	f := newFixture()
	f.groups.appendErr = errStoreDown

	if _, err := f.svc.IngestConsumerGroups(ctxWithAgent("odradek-prod"),
		[]consumergroup.Observation{{Group: "g", KafkaTopic: "t", ObservedAt: time.Now()}}); err == nil {
		t.Fatal("expected the append failure to surface")
	}
}
