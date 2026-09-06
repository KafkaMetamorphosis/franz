package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// ProviderEventRepo is the append-only cluster_provider_event store (004 ADR §4).
type ProviderEventRepo struct {
	db *DB
}

// NewProviderEventRepo wires the repository to the pool.
func NewProviderEventRepo(db *DB) *ProviderEventRepo { return &ProviderEventRepo{db: db} }

var (
	_ out.ProviderEventRepository = (*ProviderEventRepo)(nil)
	_ out.ProviderStatusReader    = (*ProviderEventRepo)(nil)
)

// Append writes one status report.
func (r *ProviderEventRepo) Append(ctx context.Context, e *provider.Event) error {
	id, err := uuid.NewV7()
	if err != nil {
		return errs.Internalf("generate uuid").Wrap(err)
	}
	_, err = r.db.Pool().Exec(ctx, `
		INSERT INTO cluster_provider_event
			(id, realm_id, kafka_cluster_id, cluster_frn, phase, reachable,
			 message, reporting_agent, recipe_ref, occurred_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		id, e.RealmID, e.ClusterID, e.ClusterFRN.Path(), string(e.Phase), e.Reachable,
		e.Message, e.ReportingAgent, e.RecipeRef, e.OccurredAt)
	if err != nil {
		return errs.Internalf("append cluster_provider_event").Wrap(err)
	}
	return nil
}

// LatestStatus returns the newest event's projection, or (nil, nil).
func (r *ProviderEventRepo) LatestStatus(ctx context.Context, clusterID uuid.UUID) (*provider.Status, error) {
	var (
		s        provider.Status
		phase    string
		occurred time.Time
	)
	err := r.db.Pool().QueryRow(ctx, `
		SELECT phase, reachable, message, recipe_ref, reporting_agent, occurred_at
		FROM cluster_provider_event
		WHERE kafka_cluster_id = $1
		ORDER BY occurred_at DESC, id DESC
		LIMIT 1`, clusterID).
		Scan(&phase, &s.Reachable, &s.Message, &s.RecipeRef, &s.ReportingAgent, &occurred)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, errs.Internalf("read latest provider status").Wrap(err)
	}
	s.Phase = provider.Phase(phase)
	s.ReportedAt = occurred
	return &s, nil
}

const providerEventColumns = `cluster_frn, phase, reachable, message, reporting_agent, recipe_ref, occurred_at`

// ListByCluster returns history for a cluster, newest first.
func (r *ProviderEventRepo) ListByCluster(
	ctx context.Context, clusterID uuid.UUID, limit int, after string,
) (out.ProviderEventPage, error) {
	if limit <= 0 {
		limit = 50
	}

	sql := `SELECT id, ` + providerEventColumns + `
		FROM cluster_provider_event WHERE kafka_cluster_id = $1`
	args := []any{clusterID}
	if after != "" {
		ts, id, err := decodeEventCursor(after)
		if err != nil {
			return out.ProviderEventPage{}, err
		}
		args = append(args, ts, id)
		sql += ` AND (occurred_at, id) < ($2, $3)`
	}
	sql += ` ORDER BY occurred_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.ProviderEventPage{}, errs.Internalf("list cluster_provider_event").Wrap(err)
	}
	defer rows.Close()

	type row struct {
		id uuid.UUID
		ev *provider.Event
	}
	var scanned []row
	for rows.Next() {
		var (
			id      uuid.UUID
			frnPath string
			phase   string
			ev      provider.Event
		)
		if err := rows.Scan(&id, &frnPath, &phase, &ev.Reachable, &ev.Message,
			&ev.ReportingAgent, &ev.RecipeRef, &ev.OccurredAt); err != nil {
			return out.ProviderEventPage{}, errs.Internalf("scan cluster_provider_event").Wrap(err)
		}
		if f, perr := frn.ParsePath(frnPath); perr == nil {
			ev.ClusterFRN = f
		}
		ev.ClusterID = clusterID
		ev.Phase = provider.Phase(phase)
		scanned = append(scanned, row{id: id, ev: &ev})
	}
	if err := rows.Err(); err != nil {
		return out.ProviderEventPage{}, errs.Internalf("iterate cluster_provider_event").Wrap(err)
	}

	var page out.ProviderEventPage
	if len(scanned) > limit {
		last := scanned[limit-1]
		page.After = encodeEventCursor(last.ev.OccurredAt, last.id)
		scanned = scanned[:limit]
	}
	for _, r := range scanned {
		page.Events = append(page.Events, r.ev)
	}
	return page, nil
}

// PruneOlderThan deletes events older than cutoff.
func (r *ProviderEventRepo) PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := r.db.Pool().Exec(ctx,
		`DELETE FROM cluster_provider_event WHERE occurred_at < $1`, cutoff)
	if err != nil {
		return 0, errs.Internalf("prune cluster_provider_event").Wrap(err)
	}
	return tag.RowsAffected(), nil
}

// --- opaque cursor: "<unixnano>|<uuid>" base64url ----------------------

func encodeEventCursor(ts time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(strconv.FormatInt(ts.UTC().UnixNano(), 10) + "|" + id.String()))
}

func decodeEventCursor(s string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, uuid.Nil, errs.InvalidField("page_token", "malformed cursor")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, errs.InvalidField("page_token", "malformed cursor")
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, errs.InvalidField("page_token", "malformed cursor")
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, errs.InvalidField("page_token", "malformed cursor")
	}
	return time.Unix(0, nanos).UTC(), id, nil
}
