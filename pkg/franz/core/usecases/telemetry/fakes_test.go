package telemetry_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// The fakes below stand in for the three out ports and the evaluation entry
// point. They record every call, so a test can assert on the *absence* of an
// evaluation as directly as on its presence — which is what 003.14's
// out-of-order rule is about.

var testRealmID = realm.DefaultID

// ctxWithAgent builds the context an authenticated agent RPC runs under: the
// agent-auth interceptor supplies the agent, the realm interceptor the realm.
func ctxWithAgent(name string) context.Context {
	ctx := realm.NewContext(context.Background(),
		realm.Realm{ID: testRealmID, Slug: realm.DefaultSlug})
	return agent.NewContext(ctx, &agent.Agent{
		ID: uuid.New(), RealmID: testRealmID, Name: name,
		Type: agent.TypeTelemetryAgent, Status: agent.StatusActive,
	})
}

// registered builds an Indicator the way CreateIndicator would.
func registered(name string, unit indicator.Unit, appliesTo indicator.Entity) *indicator.Indicator {
	i, err := indicator.NewIndicator(
		realm.Realm{ID: testRealmID, Slug: realm.DefaultSlug},
		name, unit, appliesTo, "1h", nil)
	if err != nil {
		panic(err)
	}
	return i
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- IndicatorSampleRepository -------------------------------------------

type fakeSampleRepo struct {
	appended  []*indicator.Sample
	appendErr error
}

var _ out.IndicatorSampleRepository = (*fakeSampleRepo)(nil)

func (f *fakeSampleRepo) Append(_ context.Context, samples []*indicator.Sample) (int, error) {
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	f.appended = append(f.appended, samples...)
	return len(samples), nil
}

func (f *fakeSampleRepo) PruneOlderThan(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func (f *fakeSampleRepo) List(context.Context, out.SampleQuery) (out.SamplePage, error) {
	return out.SamplePage{}, nil
}

func (f *fakeSampleRepo) LatestPerResource(
	context.Context, uuid.UUID, string, int,
) ([]*indicator.Sample, error) {
	return nil, nil
}

// --- IndicatorRepository --------------------------------------------------

// recordedSample is one RecordSample call, in order.
type recordedSample struct {
	name        string
	resourceFRN string
	value       string
	sampleAt    time.Time
}

// fakeIndicatorRepo keeps the registry and reproduces RecordSample's
// only-if-newer semantics in memory, including its `advanced` return.
type fakeIndicatorRepo struct {
	rows      map[string]*indicator.Indicator
	recorded  []recordedSample
	recordErr error
}

func newIndicatorRepo(indicators ...*indicator.Indicator) *fakeIndicatorRepo {
	r := &fakeIndicatorRepo{rows: map[string]*indicator.Indicator{}}
	for _, i := range indicators {
		r.rows[i.Name] = i
	}
	return r
}

var _ out.IndicatorRepository = (*fakeIndicatorRepo)(nil)

func (f *fakeIndicatorRepo) Create(_ context.Context, i *indicator.Indicator) error {
	f.rows[i.Name] = i
	return nil
}

func (f *fakeIndicatorRepo) Get(
	_ context.Context, _ uuid.UUID, name string,
) (*indicator.Indicator, error) {
	i, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("indicator %q not found", name)
	}
	return i, nil
}

func (f *fakeIndicatorRepo) List(context.Context, out.IndicatorQuery) (out.IndicatorPage, error) {
	return out.IndicatorPage{}, nil
}

func (f *fakeIndicatorRepo) Mutate(
	_ context.Context, _ uuid.UUID, name string, mutate func(*indicator.Indicator) error,
) (*indicator.Indicator, error) {
	i, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("indicator %q not found", name)
	}
	return i, mutate(i)
}

func (f *fakeIndicatorRepo) Delete(_ context.Context, _ uuid.UUID, name string) error {
	delete(f.rows, name)
	return nil
}

func (f *fakeIndicatorRepo) RecordSample(
	_ context.Context, _ uuid.UUID, name, resourceFRN, value string, sampleAt time.Time,
) (bool, error) {
	if f.recordErr != nil {
		return false, f.recordErr
	}
	f.recorded = append(f.recorded,
		recordedSample{name: name, resourceFRN: resourceFRN, value: value, sampleAt: sampleAt})
	i, ok := f.rows[name]
	if !ok {
		return false, errs.NotFoundf("indicator %q not found", name)
	}
	if i.LastSampleAt != nil && !sampleAt.After(*i.LastSampleAt) {
		return false, nil
	}
	at := sampleAt
	i.LastSampleAt = &at
	i.CurrentValue = value
	i.CurrentResourceFRN = resourceFRN
	return true, nil
}

// --- ObservedConsumerGroupRepository --------------------------------------

type fakeGroupRepo struct {
	appended  []*consumergroup.Observation
	appendErr error
}

var _ out.ObservedConsumerGroupRepository = (*fakeGroupRepo)(nil)

func (f *fakeGroupRepo) Append(
	_ context.Context, observations []*consumergroup.Observation,
) (int, error) {
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	f.appended = append(f.appended, observations...)
	return len(observations), nil
}

func (f *fakeGroupRepo) ListCurrent(
	context.Context, out.ObservedGroupQuery,
) (out.ObservationPage, error) {
	return out.ObservationPage{}, nil
}

func (f *fakeGroupRepo) ListObservations(
	context.Context, out.ObservationQuery,
) (out.ObservationPage, error) {
	return out.ObservationPage{}, nil
}

func (f *fakeGroupRepo) PruneOlderThan(context.Context, time.Time) (int64, error) {
	return 0, nil
}

// --- GovernanceEvaluator --------------------------------------------------

// evaluation is one Evaluate call, in order.
type evaluation struct {
	indicator   string
	resourceFRN string
	value       string
}

type fakeEvaluator struct {
	calls []evaluation
	err   error
}

var _ in.GovernanceEvaluator = (*fakeEvaluator)(nil)

func (f *fakeEvaluator) Evaluate(_ context.Context, name, resourceFRN, value string) error {
	f.calls = append(f.calls,
		evaluation{indicator: name, resourceFRN: resourceFRN, value: value})
	return f.err
}

var errStoreDown = errors.New("store unreachable")
