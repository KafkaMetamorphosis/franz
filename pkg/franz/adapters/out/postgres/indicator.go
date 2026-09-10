package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
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

const sampleColumns = `id, realm_id, indicator, resource_frn, resource_entity,
	value, reporting_agent, sample_at, received_at`

func scanSample(sc rowScanner) (*indicator.Sample, error) {
	var (
		s      indicator.Sample
		entity string
	)
	err := sc.Scan(&s.ID, &s.RealmID, &s.Indicator, &s.ResourceFRN, &entity,
		&s.Value, &s.ReportingAgent, &s.SampleAt, &s.ReceivedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("indicator sample not found")
		}
		return nil, errs.Internalf("scan indicator sample").Wrap(err)
	}
	s.ResourceEntity = indicator.Entity(entity)
	return &s, nil
}

// List returns one page of the history, newest first (ListIndicatorSamples).
// The (sample_at, id) cursor is stable under concurrent ingest: new samples land
// ahead of the cursor, so paging back through a live series never skips or
// repeats a row it has already returned.
func (r *IndicatorSampleRepo) List(ctx context.Context, q out.SampleQuery) (out.SamplePage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}

	sql := `SELECT ` + sampleColumns + ` FROM indicator_sample
		WHERE realm_id = $1 AND indicator = $2`
	args := []any{q.RealmID, q.Indicator}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if q.ResourceFRN != "" {
		sql += ` AND resource_frn = ` + arg(q.ResourceFRN)
	}
	if !q.From.IsZero() {
		sql += ` AND sample_at >= ` + arg(q.From)
	}
	if !q.To.IsZero() {
		sql += ` AND sample_at <= ` + arg(q.To)
	}
	if q.AfterCursor != "" {
		ts, id, err := decodeEventCursor(q.AfterCursor)
		if err != nil {
			return out.SamplePage{}, err
		}
		sql += ` AND (sample_at, id) < (` + arg(ts) + `, ` + arg(id) + `)`
	}
	sql += ` ORDER BY sample_at DESC, id DESC LIMIT ` + arg(limit+1)

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.SamplePage{}, errs.Internalf("list indicator samples").Wrap(err)
	}
	defer rows.Close()

	var samples []*indicator.Sample
	for rows.Next() {
		s, err := scanSample(rows)
		if err != nil {
			return out.SamplePage{}, err
		}
		samples = append(samples, s)
	}
	if err := rows.Err(); err != nil {
		return out.SamplePage{}, errs.Internalf("iterate indicator samples").Wrap(err)
	}

	var page out.SamplePage
	if len(samples) > limit {
		last := samples[limit-1]
		page.LastCursor = encodeEventCursor(last.SampleAt, last.ID)
		samples = samples[:limit]
	}
	page.Samples = samples
	return page, nil
}

// LatestPerResource returns the newest sample per resource_frn for one
// indicator — the "current" value view a dry run reads (003.14). DISTINCT ON
// needs its leading ORDER BY to be resource_frn, so the newest-per-resource pick
// happens inside a subquery and the caller-visible order stays resource_frn
// ascending, which is what makes a dry run deterministic.
func (r *IndicatorSampleRepo) LatestPerResource(
	ctx context.Context, realmID uuid.UUID, name string, limit int,
) ([]*indicator.Sample, error) {
	if limit <= 0 {
		limit = latestPerResourceCap
	}
	rows, err := r.db.Pool().Query(ctx, `
		SELECT `+sampleColumns+` FROM (
			SELECT DISTINCT ON (resource_frn) `+sampleColumns+`
			FROM indicator_sample
			WHERE realm_id = $1 AND indicator = $2
			ORDER BY resource_frn, sample_at DESC, id DESC
		) latest
		ORDER BY resource_frn ASC
		LIMIT $3`, realmID, name, limit)
	if err != nil {
		return nil, errs.Internalf("list latest indicator samples").Wrap(err)
	}
	defer rows.Close()

	var samples []*indicator.Sample
	for rows.Next() {
		s, err := scanSample(rows)
		if err != nil {
			return nil, err
		}
		samples = append(samples, s)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internalf("iterate latest indicator samples").Wrap(err)
	}
	return samples, nil
}

// latestPerResourceCap bounds an unbounded LatestPerResource call. A dry run
// renders every match to a human, so a realm-wide sweep is capped rather than
// streamed.
const latestPerResourceCap = 1000

// --- IndicatorRepo — the registry (003.14) -------------------------------

// IndicatorRepo implements out.IndicatorRepository.
type IndicatorRepo struct {
	db *DB
}

// NewIndicatorRepo wires the repository to the pool.
func NewIndicatorRepo(db *DB) *IndicatorRepo { return &IndicatorRepo{db: db} }

var _ out.IndicatorRepository = (*IndicatorRepo)(nil)

const indicatorColumns = `id, realm_id, name, frn, unit, applies_to,
	staleness_threshold, source_agents, current_value, current_resource_frn,
	last_sample_at, created_at, updated_at`

func scanIndicator(sc rowScanner) (*indicator.Indicator, error) {
	var (
		i                        indicator.Indicator
		frnPath, unit, appliesTo string
		stalenessSpec            string
		agentsRaw                []byte
		lastSampleAt             *time.Time
	)
	err := sc.Scan(&i.ID, &i.RealmID, &i.Name, &frnPath, &unit, &appliesTo,
		&stalenessSpec, &agentsRaw, &i.CurrentValue, &i.CurrentResourceFRN,
		&lastSampleAt, &i.CreatedAt, &i.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("indicator not found")
		}
		return nil, errs.Internalf("scan indicator").Wrap(err)
	}
	f, err := frn.ParsePath(frnPath)
	if err != nil {
		return nil, errs.Internalf("stored frn %q is malformed", frnPath).Wrap(err)
	}
	i.FRN = f
	i.Unit = indicator.Unit(unit)
	i.AppliesTo = indicator.Entity(appliesTo)
	i.LastSampleAt = lastSampleAt
	// The stored spec is the operator's text; re-parsing it here keeps
	// StalenessThreshold and StalenessSpec in agreement without a second column.
	if err := i.SetStalenessThreshold(stalenessSpec); err != nil {
		return nil, errs.Internalf("stored staleness_threshold %q is malformed", stalenessSpec).Wrap(err)
	}
	if err := json.Unmarshal(agentsRaw, &i.SourceAgents); err != nil {
		return nil, errs.Internalf("decode source_agents").Wrap(err)
	}
	return &i, nil
}

// Create inserts a new indicator row.
func (r *IndicatorRepo) Create(ctx context.Context, i *indicator.Indicator) error {
	if i.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		i.ID = id
	}
	agents, _ := json.Marshal(nonNilSlice(i.SourceAgents))

	stored, err := scanIndicator(r.db.Pool().QueryRow(ctx, `
		INSERT INTO indicator
			(id, realm_id, name, frn, unit, applies_to, staleness_threshold, source_agents)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING `+indicatorColumns,
		i.ID, i.RealmID, i.Name, i.FRN.Path(), string(i.Unit), string(i.AppliesTo),
		i.StalenessSpec, agents))
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("indicator %q already exists", i.Name)
		}
		return err
	}
	*i = *stored
	return nil
}

// Get returns the indicator by (realm, name).
func (r *IndicatorRepo) Get(
	ctx context.Context, realmID uuid.UUID, name string,
) (*indicator.Indicator, error) {
	return scanIndicator(r.db.Pool().QueryRow(ctx,
		`SELECT `+indicatorColumns+` FROM indicator WHERE realm_id=$1 AND name=$2`,
		realmID, name))
}

// List returns one page ordered by name ascending.
func (r *IndicatorRepo) List(ctx context.Context, q out.IndicatorQuery) (out.IndicatorPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.db.Pool().Query(ctx,
		`SELECT `+indicatorColumns+` FROM indicator
		 WHERE realm_id=$1 AND name > $2
		 ORDER BY name ASC LIMIT $3`, q.RealmID, q.AfterName, limit+1)
	if err != nil {
		return out.IndicatorPage{}, errs.Internalf("list indicators").Wrap(err)
	}
	defer rows.Close()

	var indicators []*indicator.Indicator
	for rows.Next() {
		i, err := scanIndicator(rows)
		if err != nil {
			return out.IndicatorPage{}, err
		}
		indicators = append(indicators, i)
	}
	if err := rows.Err(); err != nil {
		return out.IndicatorPage{}, errs.Internalf("iterate indicators").Wrap(err)
	}

	var page out.IndicatorPage
	if len(indicators) > limit {
		indicators = indicators[:limit]
		page.LastName = indicators[limit-1].Name
	}
	page.Indicators = indicators
	return page, nil
}

// Mutate loads the row FOR UPDATE, runs mutate, and persists — one transaction.
// `applies_to` is absent from the UPDATE statement, which is where 003.14's
// immutability is enforced: a mutate that changes it simply does not persist.
func (r *IndicatorRepo) Mutate(
	ctx context.Context, realmID uuid.UUID, name string,
	mutate func(*indicator.Indicator) error,
) (*indicator.Indicator, error) {
	var result *indicator.Indicator
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		i, err := scanIndicator(tx.QueryRow(ctx,
			`SELECT `+indicatorColumns+` FROM indicator
			 WHERE realm_id=$1 AND name=$2 FOR UPDATE`, realmID, name))
		if err != nil {
			return err
		}
		if err := mutate(i); err != nil {
			return err
		}
		agents, _ := json.Marshal(nonNilSlice(i.SourceAgents))
		updated, err := scanIndicator(tx.QueryRow(ctx, `
			UPDATE indicator SET
				unit=$1, staleness_threshold=$2, source_agents=$3, updated_at=now()
			WHERE id=$4
			RETURNING `+indicatorColumns,
			string(i.Unit), i.StalenessSpec, agents, i.ID))
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	return result, err
}

// Delete removes the indicator row. The caller checks the policy-reference guard
// (003.8) first.
func (r *IndicatorRepo) Delete(ctx context.Context, realmID uuid.UUID, name string) error {
	tag, err := r.db.Pool().Exec(ctx,
		`DELETE FROM indicator WHERE realm_id=$1 AND name=$2`, realmID, name)
	if err != nil {
		return errs.Internalf("delete indicator").Wrap(err)
	}
	if tag.RowsAffected() == 0 {
		return errs.NotFoundf("indicator %q not found", name)
	}
	return nil
}

// RecordSample advances the current-value projection only when sampleAt is
// newer than the stored last_sample_at. The comparison is in the WHERE clause,
// not in Go, so two concurrent ingest batches cannot interleave a read and a
// write and leave an older sample as "current".
func (r *IndicatorRepo) RecordSample(
	ctx context.Context, realmID uuid.UUID, name, resourceFRN, value string,
	sampleAt time.Time,
) (bool, error) {
	tag, err := r.db.Pool().Exec(ctx, `
		UPDATE indicator SET
			current_value=$3, current_resource_frn=$4, last_sample_at=$5, updated_at=now()
		WHERE realm_id=$1 AND name=$2
		  AND (last_sample_at IS NULL OR last_sample_at < $5)`,
		realmID, name, value, resourceFRN, sampleAt.UTC())
	if err != nil {
		return false, errs.Internalf("record indicator sample").Wrap(err)
	}
	return tag.RowsAffected() > 0, nil
}

func nonNilSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
