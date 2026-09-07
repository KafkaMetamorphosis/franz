// Package telemetry is the telemetry-ingest application service (003.14): it
// accepts indicator samples from an authenticated agent and appends them to the
// 30-day time series.
//
// Deliverable 12 needs ingest only, so this is the minimal cut the 005 ADR Part
// 2 sweep requires. Deliverable 14 adds the `Indicator` registry and with it
// pre-registration enforcement ("samples for an unknown indicator are rejected"),
// unit validation, the history/current-value queries, and the governance
// evaluation trigger. Until then every well-formed sample is accepted.
package telemetry

import (
	"context"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
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
	samples out.IndicatorSampleRepository
	now     func() time.Time
}

var _ in.TelemetryIngestService = (*Service)(nil)

// NewService wires the service to its repository.
func NewService(samples out.IndicatorSampleRepository) *Service {
	return &Service{samples: samples, now: time.Now}
}

// IngestSamples validates and appends one batch, attributing every row to the
// agent in context. An empty batch is a no-op, so a client stream can send a
// keepalive.
func (s *Service) IngestSamples(ctx context.Context, batch []indicator.Sample) (int, error) {
	a := agent.MustFromContext(ctx)
	if len(batch) == 0 {
		return 0, nil
	}
	if len(batch) > maxBatch {
		return 0, errs.InvalidField("samples", "at most 1000 samples per batch")
	}

	receivedAt := s.now().UTC()
	rows := make([]*indicator.Sample, 0, len(batch))
	for _, raw := range batch {
		sample, err := indicator.NewSample(
			a.RealmID, raw.Indicator, raw.ResourceFRN, raw.ResourceEntity,
			raw.Value, a.Name, raw.SampleAt, receivedAt)
		if err != nil {
			return 0, err
		}
		rows = append(rows, sample)
	}
	return s.samples.Append(ctx, rows)
}
