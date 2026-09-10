package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// PolicyRepo implements out.PolicyRepository with hand-written pgx (003.12).
type PolicyRepo struct {
	db *DB
}

// NewPolicyRepo wires the repository to the pool.
func NewPolicyRepo(db *DB) *PolicyRepo { return &PolicyRepo{db: db} }

var _ out.PolicyRepository = (*PolicyRepo)(nil)

const policyColumns = `id, realm_id, name, frn, indicator, matcher,
	limit_operator, limit_value, actions, weight, enabled, last_fired_at,
	created_at, updated_at`

// --- on-disk shapes (the domain stays tag-free) ---

type matcherRow struct {
	Entity   string `json:"entity"`
	Selector string `json:"selector,omitempty"`
}

type actionRow struct {
	Kind string   `json:"kind"`
	Args []string `json:"args"`
}

func marshalMatcher(m governance.Matcher) []byte {
	b, _ := json.Marshal(matcherRow{Entity: string(m.Entity), Selector: m.Selector})
	return b
}

func marshalActions(actions []governance.Action) []byte {
	rows := make([]actionRow, 0, len(actions))
	for _, a := range actions {
		rows = append(rows, actionRow{Kind: string(a.Kind), Args: nonNilSlice(a.Args)})
	}
	b, _ := json.Marshal(rows)
	return b
}

func unmarshalActions(raw []byte) ([]governance.Action, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var rows []actionRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, errs.Internalf("decode actions").Wrap(err)
	}
	actions := make([]governance.Action, 0, len(rows))
	for _, a := range rows {
		actions = append(actions, governance.Action{
			Kind: governance.ActionKind(a.Kind),
			Args: a.Args,
		})
	}
	return actions, nil
}

func scanPolicy(sc rowScanner) (*governance.Policy, error) {
	var (
		p                governance.Policy
		frnPath, limitOp string
		matcherRaw       []byte
		actionsRaw       []byte
		lastFiredAt      *time.Time
	)
	err := sc.Scan(&p.ID, &p.RealmID, &p.Name, &frnPath, &p.Indicator, &matcherRaw,
		&limitOp, &p.Limit.Value, &actionsRaw, &p.Weight, &p.Enabled, &lastFiredAt,
		&p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("policy not found")
		}
		return nil, errs.Internalf("scan policy").Wrap(err)
	}
	f, err := frn.ParsePath(frnPath)
	if err != nil {
		return nil, errs.Internalf("stored frn %q is malformed", frnPath).Wrap(err)
	}
	p.FRN = f
	p.Limit.Operator = governance.Operator(limitOp)
	p.LastFiredAt = lastFiredAt

	var m matcherRow
	if err := json.Unmarshal(matcherRaw, &m); err != nil {
		return nil, errs.Internalf("decode matcher").Wrap(err)
	}
	p.Matcher = governance.Matcher{
		Entity:   indicator.Entity(m.Entity),
		Selector: m.Selector,
	}
	if p.Actions, err = unmarshalActions(actionsRaw); err != nil {
		return nil, err
	}
	return &p, nil
}

// Create inserts a new policy row.
func (r *PolicyRepo) Create(ctx context.Context, p *governance.Policy) error {
	if p.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		p.ID = id
	}
	stored, err := scanPolicy(r.db.Pool().QueryRow(ctx, `
		INSERT INTO policy
			(id, realm_id, name, frn, indicator, matcher, limit_operator, limit_value,
			 actions, weight, enabled)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING `+policyColumns,
		p.ID, p.RealmID, p.Name, p.FRN.Path(), p.Indicator, marshalMatcher(p.Matcher),
		string(p.Limit.Operator), p.Limit.Value, marshalActions(p.Actions),
		p.Weight, p.Enabled))
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("policy %q already exists", p.Name)
		}
		return err
	}
	*p = *stored
	return nil
}

// Get returns the policy by (realm, name).
func (r *PolicyRepo) Get(
	ctx context.Context, realmID uuid.UUID, name string,
) (*governance.Policy, error) {
	return scanPolicy(r.db.Pool().QueryRow(ctx,
		`SELECT `+policyColumns+` FROM policy WHERE realm_id=$1 AND name=$2`,
		realmID, name))
}

// List returns one page ordered by name ascending.
func (r *PolicyRepo) List(ctx context.Context, q out.PolicyQuery) (out.PolicyPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	policies, err := r.collect(ctx,
		`SELECT `+policyColumns+` FROM policy
		 WHERE realm_id=$1 AND name > $2
		 ORDER BY name ASC LIMIT $3`, q.RealmID, q.AfterName, limit+1)
	if err != nil {
		return out.PolicyPage{}, err
	}
	var page out.PolicyPage
	if len(policies) > limit {
		policies = policies[:limit]
		page.LastName = policies[limit-1].Name
	}
	page.Policies = policies
	return page, nil
}

// ListEnabledByIndicator is the evaluation pass's work list, ordered by name so
// the pass is deterministic before Order re-sorts by (weight desc, name asc).
func (r *PolicyRepo) ListEnabledByIndicator(
	ctx context.Context, realmID uuid.UUID, indicatorName string,
) ([]*governance.Policy, error) {
	return r.collect(ctx,
		`SELECT `+policyColumns+` FROM policy
		 WHERE realm_id=$1 AND indicator=$2 AND enabled
		 ORDER BY name ASC`, realmID, indicatorName)
}

// CountByIndicator counts policies — enabled or not — naming the indicator.
func (r *PolicyRepo) CountByIndicator(
	ctx context.Context, realmID uuid.UUID, indicatorName string,
) (int, error) {
	var n int
	err := r.db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM policy WHERE realm_id=$1 AND indicator=$2`,
		realmID, indicatorName).Scan(&n)
	if err != nil {
		return 0, errs.Internalf("count policies by indicator").Wrap(err)
	}
	return n, nil
}

func (r *PolicyRepo) collect(
	ctx context.Context, sql string, args ...any,
) ([]*governance.Policy, error) {
	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return nil, errs.Internalf("list policies").Wrap(err)
	}
	defer rows.Close()

	var policies []*governance.Policy
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, p)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internalf("iterate policies").Wrap(err)
	}
	return policies, nil
}

// Mutate loads the policy FOR UPDATE, runs mutate, persists — one transaction.
func (r *PolicyRepo) Mutate(
	ctx context.Context, realmID uuid.UUID, name string,
	mutate func(*governance.Policy) error,
) (*governance.Policy, error) {
	var result *governance.Policy
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		p, err := scanPolicy(tx.QueryRow(ctx,
			`SELECT `+policyColumns+` FROM policy
			 WHERE realm_id=$1 AND name=$2 FOR UPDATE`, realmID, name))
		if err != nil {
			return err
		}
		if err := mutate(p); err != nil {
			return err
		}
		updated, err := scanPolicy(tx.QueryRow(ctx, `
			UPDATE policy SET
				indicator=$1, matcher=$2, limit_operator=$3, limit_value=$4,
				actions=$5, weight=$6, enabled=$7, updated_at=now()
			WHERE id=$8
			RETURNING `+policyColumns,
			p.Indicator, marshalMatcher(p.Matcher), string(p.Limit.Operator),
			p.Limit.Value, marshalActions(p.Actions), p.Weight, p.Enabled, p.ID))
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	return result, err
}

// Delete removes the policy row. Its policy_action audit rows survive — they
// carry no foreign key (003.8 "Auditability").
func (r *PolicyRepo) Delete(ctx context.Context, realmID uuid.UUID, name string) error {
	tag, err := r.db.Pool().Exec(ctx,
		`DELETE FROM policy WHERE realm_id=$1 AND name=$2`, realmID, name)
	if err != nil {
		return errs.Internalf("delete policy").Wrap(err)
	}
	if tag.RowsAffected() == 0 {
		return errs.NotFoundf("policy %q not found", name)
	}
	return nil
}

// MarkFired stamps last_fired_at. It deliberately does not touch updated_at:
// firing is not an edit to the rule, and an operator reading `updated_at` wants
// to know when someone last changed the policy, not when it last acted.
func (r *PolicyRepo) MarkFired(
	ctx context.Context, realmID uuid.UUID, name string, at time.Time,
) error {
	tag, err := r.db.Pool().Exec(ctx,
		`UPDATE policy SET last_fired_at=$3 WHERE realm_id=$1 AND name=$2`,
		realmID, name, at.UTC())
	if err != nil {
		return errs.Internalf("mark policy fired").Wrap(err)
	}
	if tag.RowsAffected() == 0 {
		return errs.NotFoundf("policy %q not found", name)
	}
	return nil
}
