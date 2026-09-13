// Package telemetry is the telemetry-ingest application service (003.14): the
// two inbound agent streams — indicator samples and consumer-group observations
// — the write side that maintains each Indicator's current value, and the hook
// that fires governance evaluation.
package telemetry

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// maxBatch bounds one publish call so a runaway agent cannot pin a connection
// writing a single statement (003.14 OQ2 leaves the real limit open).
const maxBatch = 1000

// Service implements in.TelemetryIngestService.
type Service struct {
	samples    out.IndicatorSampleRepository
	indicators out.IndicatorRepository
	groups     out.ObservedConsumerGroupRepository
	evaluator  in.GovernanceEvaluator
	log        *slog.Logger
	now        func() time.Time
}

var _ in.TelemetryIngestService = (*Service)(nil)

// NewService wires ingest to the two append-only stores, the Indicator registry
// it validates against, and the governance evaluation entry point it triggers.
func NewService(
	samples out.IndicatorSampleRepository,
	indicators out.IndicatorRepository,
	groups out.ObservedConsumerGroupRepository,
	evaluator in.GovernanceEvaluator,
	log *slog.Logger,
) *Service {
	return &Service{
		samples:    samples,
		indicators: indicators,
		groups:     groups,
		evaluator:  evaluator,
		log:        log,
		now:        time.Now,
	}
}

// IngestSamples validates and appends one batch, attributing every row to the
// agent in context, then advances the current value of each indicator the batch
// touched and fires governance evaluation for the ones that moved. An empty
// batch is a no-op, so a client stream can send a keepalive.
//
// The whole batch is validated before anything is written and any rejection
// fails the call (see the port doc). The alternative — accept the good rows and
// drop the rest — cannot be expressed: `PublishIndicatorSamplesResponse` carries
// a count and nothing else, so a partial accept would leave the agent unable to
// tell which samples it still owes, and it would silently normalise a
// misconfigured agent that is publishing an indicator nobody registered.
func (s *Service) IngestSamples(ctx context.Context, batch []indicator.Sample) (int, error) {
	a := agent.MustFromContext(ctx)
	if len(batch) == 0 {
		return 0, nil
	}
	if len(batch) > maxBatch {
		return 0, errs.InvalidField("samples", "at most 1000 samples per batch")
	}

	receivedAt := s.clock()
	rows, err := s.validateBatch(ctx, a.RealmID, a.Name, batch, receivedAt)
	if err != nil {
		return 0, err
	}

	accepted, err := s.samples.Append(ctx, rows)
	if err != nil {
		return 0, err
	}
	s.advanceCurrentValues(ctx, a.RealmID, rows)
	return accepted, nil
}

// validateBatch turns the wire batch into domain samples, enforcing 003.14's
// three pre-registration rules in the order the spec states them: the indicator
// must be registered, the sample's entity must match what it applies to, and the
// value must parse in its unit.
//
// Indicators are resolved once per distinct name — an agent's sweep publishes
// many resources per indicator, and a Get per row would multiply a 1000-sample
// batch into 1000 round trips.
func (s *Service) validateBatch(
	ctx context.Context, realmID uuid.UUID, agentName string,
	batch []indicator.Sample, receivedAt time.Time,
) ([]*indicator.Sample, error) {
	registry := map[string]*indicator.Indicator{}
	rows := make([]*indicator.Sample, 0, len(batch))

	for _, raw := range batch {
		sample, err := indicator.NewSample(
			realmID, raw.Indicator, raw.ResourceFRN, raw.ResourceEntity,
			raw.Value, agentName, raw.SampleAt, receivedAt)
		if err != nil {
			return nil, err
		}

		registered, ok := registry[sample.Indicator]
		if !ok {
			registered, err = s.indicators.Get(ctx, realmID, sample.Indicator)
			if err != nil {
				if errs.KindOf(err) == errs.NotFound {
					// 003.14: no auto-creation. FAILED_PRECONDITION rather than
					// NOT_FOUND — the request is well-formed, the fleet is not yet in
					// a state that can accept it, and CreateIndicator is the fix.
					return nil, errs.Preconditionf(
						"indicator %q is not registered; register it with CreateIndicator "+
							"before publishing samples", sample.Indicator)
				}
				return nil, err
			}
			registry[sample.Indicator] = registered
		}

		if err := registered.EnsureSampleEntity(sample.ResourceEntity); err != nil {
			return nil, err
		}
		// Parsed to validate, not to store: the encoded string is what the series
		// keeps, so a later unit change re-interprets history rather than losing it.
		if _, err := indicator.ParseValue(registered.Unit, "value", sample.Value); err != nil {
			return nil, err
		}
		rows = append(rows, sample)
	}
	return rows, nil
}

// advanceCurrentValues maintains the Indicator projection and fires the
// ingest→eval hook (003.14 "Governance coupling", 003.8 OQ4 — event-driven).
//
// RecordSample only advances when sample_at is newer than the stored
// last_sample_at, and its `advanced` return is exactly 003.14's out-of-order
// rule: an older sample is already stored as history but does not become current
// and does not trigger evaluation.
//
// Evaluation is synchronous, resolving 003.14 OQ4 in favour of the simple option
// for now: it couples ingest latency to policy application, but it needs no
// durable queue and no extra component, and a lost evaluation is worse than a
// slow one. Neither a failed projection write nor a failed evaluation fails the
// batch — the samples are durable either way, and the next sample re-triggers
// both.
func (s *Service) advanceCurrentValues(
	ctx context.Context, realmID uuid.UUID, rows []*indicator.Sample,
) {
	for _, sample := range rows {
		advanced, err := s.indicators.RecordSample(
			ctx, realmID, sample.Indicator, sample.ResourceFRN, sample.Value, sample.SampleAt)
		if err != nil {
			s.log.Warn("telemetry: could not advance indicator current value",
				"indicator", sample.Indicator, "resource", sample.ResourceFRN, "err", err)
			continue
		}
		if !advanced {
			continue
		}
		if err := s.evaluator.Evaluate(
			ctx, sample.Indicator, sample.ResourceFRN, sample.Value); err != nil {
			s.log.Warn("telemetry: governance evaluation failed",
				"indicator", sample.Indicator, "resource", sample.ResourceFRN, "err", err)
		}
	}
}

// IngestConsumerGroups appends one batch of sightings, attributing every row to
// the agent in context and deriving each group's `custom` flag from its name.
// Nothing downstream is triggered: 003.14 is explicit that consumer-group
// observations are read-only context, not a governance signal.
func (s *Service) IngestConsumerGroups(
	ctx context.Context, batch []consumergroup.Observation,
) (int, error) {
	a := agent.MustFromContext(ctx)
	if len(batch) == 0 {
		return 0, nil
	}
	if len(batch) > maxBatch {
		return 0, errs.InvalidField("observations", "at most 1000 observations per batch")
	}

	receivedAt := s.clock()
	rows := make([]*consumergroup.Observation, 0, len(batch))
	for _, raw := range batch {
		observation, err := consumergroup.NewObservation(
			a.RealmID, raw.Group, raw.ClientFRN, raw.Owner, raw.AsyncChannel,
			raw.KafkaTopic, a.Name, raw.ObservedAt, receivedAt)
		if err != nil {
			return 0, err
		}
		rows = append(rows, observation)
	}
	return s.groups.Append(ctx, rows)
}

func (s *Service) clock() time.Time {
	if s.now == nil {
		return time.Now().UTC()
	}
	return s.now().UTC()
}
