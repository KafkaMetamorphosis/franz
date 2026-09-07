package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// IndicatorSampleRepo implements out.IndicatorSampleRepository with hand-written
// pgx (003.12 / ADR-API-005). The table is append-only; there is no update path.
type IndicatorSampleRepo struct {
	db *DB
}

// NewIndicatorSampleRepo wires the repository to the pool.
func NewIndicatorSampleRepo(db *DB) *IndicatorSampleRepo { return &IndicatorSampleRepo{db: db} }

var _ out.IndicatorSampleRepository = (*IndicatorSampleRepo)(nil)

// Append writes a batch in one round trip via CopyFrom and returns how many rows
// landed. A batch is all-or-nothing: CopyFrom runs in its own implicit
// transaction, so a rejected row fails the whole call rather than leaving a
// partial series.
func (r *IndicatorSampleRepo) Append(ctx context.Context, samples []*indicator.Sample) (int, error) {
	if len(samples) == 0 {
		return 0, nil
	}
	rows := make([][]any, len(samples))
	for i, s := range samples {
		if s.ID == uuid.Nil {
			id, err := uuid.NewV7()
			if err != nil {
				return 0, errs.Internalf("generate uuid").Wrap(err)
			}
			s.ID = id
		}
		rows[i] = []any{
			s.ID, s.RealmID, s.Indicator, s.ResourceFRN, string(s.ResourceEntity),
			s.Value, s.ReportingAgent, s.SampleAt, s.ReceivedAt,
		}
	}
	n, err := r.db.Pool().CopyFrom(ctx,
		pgx.Identifier{"indicator_sample"},
		[]string{"id", "realm_id", "indicator", "resource_frn", "resource_entity",
			"value", "reporting_agent", "sample_at", "received_at"},
		pgx.CopyFromRows(rows))
	if err != nil {
		return 0, errs.Internalf("append indicator samples").Wrap(err)
	}
	return int(n), nil
}

// PruneOlderThan deletes samples with sample_at < cutoff (003.14 — nightly
// 30-day prune).
func (r *IndicatorSampleRepo) PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := r.db.Pool().Exec(ctx,
		`DELETE FROM indicator_sample WHERE sample_at < $1`, cutoff)
	if err != nil {
		return 0, errs.Internalf("prune indicator samples").Wrap(err)
	}
	return tag.RowsAffected(), nil
}
