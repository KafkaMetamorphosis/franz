package postgres

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// ObservedConsumerGroupRepo implements out.ObservedConsumerGroupRepository. The
// table is append-only; there is no update path — a re-sighting is a new row,
// and the "current" view is a query over the series (003.14).
type ObservedConsumerGroupRepo struct {
	db *DB
}

// NewObservedConsumerGroupRepo wires the repository to the pool.
func NewObservedConsumerGroupRepo(db *DB) *ObservedConsumerGroupRepo {
	return &ObservedConsumerGroupRepo{db: db}
}

var _ out.ObservedConsumerGroupRepository = (*ObservedConsumerGroupRepo)(nil)

const observationColumns = `id, realm_id, group_name, client_frn, owner,
	async_channel, kafka_topic, custom, reported_by_agent, observed_at, received_at`

// Append writes a batch in one round trip via CopyFrom. Like indicator_sample,
// a batch is all-or-nothing: CopyFrom runs in its own implicit transaction, so a
// rejected row fails the whole call rather than leaving a partial series.
func (r *ObservedConsumerGroupRepo) Append(
	ctx context.Context, observations []*consumergroup.Observation,
) (int, error) {
	if len(observations) == 0 {
		return 0, nil
	}
	rows := make([][]any, len(observations))
	for i, o := range observations {
		if o.ID == uuid.Nil {
			id, err := uuid.NewV7()
			if err != nil {
				return 0, errs.Internalf("generate uuid").Wrap(err)
			}
			o.ID = id
		}
		rows[i] = []any{
			o.ID, o.RealmID, o.Group, o.ClientFRN, o.Owner, o.AsyncChannel,
			o.KafkaTopic, o.Custom, o.ReportingAgent, o.ObservedAt, o.ReceivedAt,
		}
	}
	n, err := r.db.Pool().CopyFrom(ctx,
		pgx.Identifier{"observed_consumer_group"},
		[]string{"id", "realm_id", "group_name", "client_frn", "owner",
			"async_channel", "kafka_topic", "custom", "reported_by_agent",
			"observed_at", "received_at"},
		pgx.CopyFromRows(rows))
	if err != nil {
		return 0, errs.Internalf("append consumer group observations").Wrap(err)
	}
	return int(n), nil
}

func scanObservation(sc rowScanner) (*consumergroup.Observation, error) {
	var o consumergroup.Observation
	err := sc.Scan(&o.ID, &o.RealmID, &o.Group, &o.ClientFRN, &o.Owner,
		&o.AsyncChannel, &o.KafkaTopic, &o.Custom, &o.ReportingAgent,
		&o.ObservedAt, &o.ReceivedAt)
	if err != nil {
		return nil, errs.Internalf("scan consumer group observation").Wrap(err)
	}
	return &o, nil
}

func collectObservations(rows pgx.Rows) ([]*consumergroup.Observation, error) {
	defer rows.Close()
	var observations []*consumergroup.Observation
	for rows.Next() {
		o, err := scanObservation(rows)
		if err != nil {
			return nil, err
		}
		observations = append(observations, o)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internalf("iterate consumer group observations").Wrap(err)
	}
	return observations, nil
}

// ListCurrent returns the newest sighting per (group, kafka_topic) — 003.14's
// current view. DISTINCT ON needs its leading ORDER BY to be the distinct key,
// so the newest-per-key pick happens in a subquery and the caller-visible order
// stays (group, topic) ascending. That order is also the page cursor: unlike a
// time-ordered series, this view is a *set* whose members re-sort as sightings
// arrive, and only a keyset on the identity columns pages it without skipping.
func (r *ObservedConsumerGroupRepo) ListCurrent(
	ctx context.Context, q out.ObservedGroupQuery,
) (out.ObservationPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultObservationLimit
	}

	inner := `SELECT DISTINCT ON (group_name, kafka_topic) ` + observationColumns + `
		FROM observed_consumer_group
		WHERE realm_id = $1`
	args := []any{q.RealmID}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if q.ClientFRN != "" {
		inner += ` AND client_frn = ` + arg(q.ClientFRN)
	}
	inner += ` ORDER BY group_name, kafka_topic, observed_at DESC, id DESC`

	sql := `SELECT ` + observationColumns + ` FROM (` + inner + `) current`
	if q.AfterCursor != "" {
		group, topic, err := decodePairCursor(q.AfterCursor)
		if err != nil {
			return out.ObservationPage{}, err
		}
		sql += ` WHERE (group_name, kafka_topic) > (` + arg(group) + `, ` + arg(topic) + `)`
	}
	sql += ` ORDER BY group_name ASC, kafka_topic ASC LIMIT ` + arg(limit+1)

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.ObservationPage{}, errs.Internalf("list observed consumer groups").Wrap(err)
	}
	observations, err := collectObservations(rows)
	if err != nil {
		return out.ObservationPage{}, err
	}

	var page out.ObservationPage
	if len(observations) > limit {
		last := observations[limit-1]
		page.LastCursor = encodePairCursor(last.Group, last.KafkaTopic)
		observations = observations[:limit]
	}
	page.Observations = observations
	return page, nil
}

// ListObservations returns the raw sightings, newest first. The (observed_at,
// id) cursor is the same one every other Franz time series pages by.
func (r *ObservedConsumerGroupRepo) ListObservations(
	ctx context.Context, q out.ObservationQuery,
) (out.ObservationPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultObservationLimit
	}

	sql := `SELECT ` + observationColumns + ` FROM observed_consumer_group
		WHERE realm_id = $1`
	args := []any{q.RealmID}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if q.ClientFRN != "" {
		sql += ` AND client_frn = ` + arg(q.ClientFRN)
	}
	if !q.From.IsZero() {
		sql += ` AND observed_at >= ` + arg(q.From)
	}
	if !q.To.IsZero() {
		sql += ` AND observed_at <= ` + arg(q.To)
	}
	if q.AfterCursor != "" {
		ts, id, err := decodeEventCursor(q.AfterCursor)
		if err != nil {
			return out.ObservationPage{}, err
		}
		sql += ` AND (observed_at, id) < (` + arg(ts) + `, ` + arg(id) + `)`
	}
	sql += ` ORDER BY observed_at DESC, id DESC LIMIT ` + arg(limit+1)

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.ObservationPage{}, errs.Internalf("list consumer group observations").Wrap(err)
	}
	observations, err := collectObservations(rows)
	if err != nil {
		return out.ObservationPage{}, err
	}

	var page out.ObservationPage
	if len(observations) > limit {
		last := observations[limit-1]
		page.LastCursor = encodeEventCursor(last.ObservedAt, last.ID)
		observations = observations[:limit]
	}
	page.Observations = observations
	return page, nil
}

// PruneOlderThan deletes sightings with observed_at < cutoff (003.14 — nightly
// 30-day prune).
func (r *ObservedConsumerGroupRepo) PruneOlderThan(
	ctx context.Context, cutoff time.Time,
) (int64, error) {
	tag, err := r.db.Pool().Exec(ctx,
		`DELETE FROM observed_consumer_group WHERE observed_at < $1`, cutoff)
	if err != nil {
		return 0, errs.Internalf("prune consumer group observations").Wrap(err)
	}
	return tag.RowsAffected(), nil
}

// defaultObservationLimit is the page size applied when the caller names none.
const defaultObservationLimit = 50

// --- opaque keyset cursor: "<group>|<topic>" base64url --------------------
//
// A group id may contain "|", a topic name may not, so the split is from the
// right.

func encodePairCursor(group, topic string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(group + "|" + topic))
}

func decodePairCursor(s string) (group, topic string, err error) {
	raw, decodeErr := base64.RawURLEncoding.DecodeString(s)
	if decodeErr != nil {
		return "", "", errs.InvalidField("page_token", "malformed cursor")
	}
	i := strings.LastIndex(string(raw), "|")
	if i < 0 {
		return "", "", errs.InvalidField("page_token", "malformed cursor")
	}
	return string(raw)[:i], string(raw)[i+1:], nil
}
