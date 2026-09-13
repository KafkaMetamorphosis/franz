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
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// PolicyActionRepo is the append-only policy_action audit store (003.8
// "Auditability"). There is no update path: a record of what a policy did is
// never rewritten, only pruned by age.
type PolicyActionRepo struct {
	db *DB
}

// NewPolicyActionRepo wires the repository to the pool.
func NewPolicyActionRepo(db *DB) *PolicyActionRepo { return &PolicyActionRepo{db: db} }

var _ out.PolicyActionRepository = (*PolicyActionRepo)(nil)

const policyActionColumns = `id, realm_id, policy_id, policy_name, occurred_at,
	resource_frn, indicator_value, action, result, received_at`

// Append writes a batch in one round trip. CopyFrom runs in its own implicit
// transaction, so a batch is all-or-nothing rather than a partial audit trail.
func (r *PolicyActionRepo) Append(ctx context.Context, records []*governance.ActionRecord) error {
	if len(records) == 0 {
		return nil
	}
	rows := make([][]any, len(records))
	for i, rec := range records {
		if rec.ID == uuid.Nil {
			id, err := uuid.NewV7()
			if err != nil {
				return errs.Internalf("generate uuid").Wrap(err)
			}
			rec.ID = id
		}
		if rec.ReceivedAt.IsZero() {
			rec.ReceivedAt = time.Now().UTC()
		}
		action, _ := json.Marshal(actionRow{
			Kind: string(rec.Action.Kind), Args: nonNilSlice(rec.Action.Args),
		})
		rows[i] = []any{
			rec.ID, rec.RealmID, rec.PolicyID, rec.PolicyName, rec.OccurredAt,
			rec.ResourceFRN, rec.IndicatorValue, action, rec.Result, rec.ReceivedAt,
		}
	}
	_, err := r.db.Pool().CopyFrom(ctx,
		pgx.Identifier{"policy_action"},
		[]string{"id", "realm_id", "policy_id", "policy_name", "occurred_at",
			"resource_frn", "indicator_value", "action", "result", "received_at"},
		pgx.CopyFromRows(rows))
	if err != nil {
		return errs.Internalf("append policy actions").Wrap(err)
	}
	return nil
}

func scanActionRecord(sc rowScanner) (*governance.ActionRecord, error) {
	var (
		rec       governance.ActionRecord
		actionRaw []byte
	)
	err := sc.Scan(&rec.ID, &rec.RealmID, &rec.PolicyID, &rec.PolicyName,
		&rec.OccurredAt, &rec.ResourceFRN, &rec.IndicatorValue, &actionRaw,
		&rec.Result, &rec.ReceivedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("policy action not found")
		}
		return nil, errs.Internalf("scan policy action").Wrap(err)
	}
	var a actionRow
	if err := json.Unmarshal(actionRaw, &a); err != nil {
		return nil, errs.Internalf("decode policy action").Wrap(err)
	}
	rec.Action = governance.Action{Kind: governance.ActionKind(a.Kind), Args: a.Args}
	return &rec, nil
}

// List returns one page of one policy's series, newest first. The query is keyed
// on policy_name, not policy_id, so a deleted policy's history is still readable
// under the name the operator remembers (003.8).
func (r *PolicyActionRepo) List(
	ctx context.Context, q out.PolicyActionQuery,
) (out.PolicyActionPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	sql := `SELECT ` + policyActionColumns + ` FROM policy_action
		WHERE realm_id = $1 AND policy_name = $2`
	args := []any{q.RealmID, q.PolicyName}
	if q.AfterCursor != "" {
		ts, id, err := decodeEventCursor(q.AfterCursor)
		if err != nil {
			return out.PolicyActionPage{}, err
		}
		args = append(args, ts, id)
		sql += ` AND (occurred_at, id) < ($3, $4)`
	}
	sql += ` ORDER BY occurred_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.PolicyActionPage{}, errs.Internalf("list policy actions").Wrap(err)
	}
	defer rows.Close()

	var records []*governance.ActionRecord
	for rows.Next() {
		rec, err := scanActionRecord(rows)
		if err != nil {
			return out.PolicyActionPage{}, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return out.PolicyActionPage{}, errs.Internalf("iterate policy actions").Wrap(err)
	}

	var page out.PolicyActionPage
	if len(records) > limit {
		last := records[limit-1]
		page.LastCursor = encodeEventCursor(last.OccurredAt, last.ID)
		records = records[:limit]
	}
	page.Actions = records
	return page, nil
}

// PruneOlderThan deletes audit rows older than cutoff (nightly 30-day prune).
func (r *PolicyActionRepo) PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := r.db.Pool().Exec(ctx,
		`DELETE FROM policy_action WHERE occurred_at < $1`, cutoff)
	if err != nil {
		return 0, errs.Internalf("prune policy actions").Wrap(err)
	}
	return tag.RowsAffected(), nil
}
