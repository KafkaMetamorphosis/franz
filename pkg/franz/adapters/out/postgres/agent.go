package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// AgentRepo implements out.AgentRepository with hand-written pgx (003.12).
type AgentRepo struct {
	db *DB
}

// NewAgentRepo wires the repository to the pool.
func NewAgentRepo(db *DB) *AgentRepo { return &AgentRepo{db: db} }

var _ out.AgentRepository = (*AgentRepo)(nil)

const agentColumns = `id, realm_id, name, frn, type, labels, status, token_hash,
	created_at, updated_at`

func scanAgent(sc rowScanner) (*agent.Agent, error) {
	var (
		a           agent.Agent
		frnPath     string
		typ, status string
		labelsRaw   []byte
	)
	err := sc.Scan(&a.ID, &a.RealmID, &a.Name, &frnPath, &typ, &labelsRaw,
		&status, &a.TokenHash, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("agent not found")
		}
		return nil, errs.Internalf("scan agent").Wrap(err)
	}
	f, err := frn.ParsePath(frnPath)
	if err != nil {
		return nil, errs.Internalf("stored frn %q is malformed", frnPath).Wrap(err)
	}
	a.FRN = f
	a.Type = agent.Type(typ)
	a.Status = agent.Status(status)
	a.Labels = map[string]string{}
	if err := json.Unmarshal(labelsRaw, &a.Labels); err != nil {
		return nil, errs.Internalf("decode labels").Wrap(err)
	}
	return &a, nil
}

// Create inserts a new agent row.
func (r *AgentRepo) Create(ctx context.Context, a *agent.Agent) error {
	if a.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		a.ID = id
	}
	labels, _ := json.Marshal(nonNilMap(a.Labels))

	stored, err := scanAgent(r.db.Pool().QueryRow(ctx, `
		INSERT INTO agent (id, realm_id, name, frn, type, labels, status, token_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING `+agentColumns,
		a.ID, a.RealmID, a.Name, a.FRN.Path(), string(a.Type), labels,
		string(a.Status), a.TokenHash))
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("agent %q already exists", a.Name)
		}
		return err
	}
	*a = *stored
	return nil
}

// Get returns the agent by (realm, name), soft-deleted rows included.
func (r *AgentRepo) Get(ctx context.Context, realmID uuid.UUID, name string) (*agent.Agent, error) {
	return scanAgent(r.db.Pool().QueryRow(ctx,
		`SELECT `+agentColumns+` FROM agent WHERE realm_id=$1 AND name=$2`,
		realmID, name))
}

// GetByTokenHash resolves a token hash to its agent across realms.
func (r *AgentRepo) GetByTokenHash(ctx context.Context, tokenHash string) (*agent.Agent, error) {
	return scanAgent(r.db.Pool().QueryRow(ctx,
		`SELECT `+agentColumns+` FROM agent WHERE token_hash=$1`, tokenHash))
}

// List returns one page ordered by name, optionally filtered by type. DELETED
// agents are excluded. The type filter is pushed to SQL, so pagination is exact.
func (r *AgentRepo) List(ctx context.Context, q out.AgentQuery) (out.AgentPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	sql := `SELECT ` + agentColumns + ` FROM agent
		WHERE realm_id=$1 AND name > $2 AND status <> 'DELETED'`
	args := []any{q.RealmID, q.AfterName}
	if q.TypeFilter != "" {
		args = append(args, string(q.TypeFilter))
		sql += ` AND type = $3`
	}
	sql += ` ORDER BY name ASC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.AgentPage{}, errs.Internalf("list agents").Wrap(err)
	}
	defer rows.Close()

	var page out.AgentPage
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return out.AgentPage{}, err
		}
		page.Agents = append(page.Agents, a)
	}
	if err := rows.Err(); err != nil {
		return out.AgentPage{}, errs.Internalf("iterate agents").Wrap(err)
	}
	if len(page.Agents) > limit {
		page.Agents = page.Agents[:limit]
		page.LastName = page.Agents[limit-1].Name
	}
	return page, nil
}

// Mutate loads the row FOR UPDATE, runs mutate, and persists the result in one
// transaction.
func (r *AgentRepo) Mutate(
	ctx context.Context, realmID uuid.UUID, name string,
	mutate func(*agent.Agent) error,
) (*agent.Agent, error) {
	var result *agent.Agent
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		a, err := scanAgent(tx.QueryRow(ctx,
			`SELECT `+agentColumns+` FROM agent
			 WHERE realm_id=$1 AND name=$2 FOR UPDATE`, realmID, name))
		if err != nil {
			return err
		}
		if err := mutate(a); err != nil {
			return err
		}
		labels, _ := json.Marshal(nonNilMap(a.Labels))
		updated, err := scanAgent(tx.QueryRow(ctx, `
			UPDATE agent SET
				type=$1, labels=$2, status=$3, token_hash=$4, updated_at=now()
			WHERE id=$5
			RETURNING `+agentColumns,
			string(a.Type), labels, string(a.Status), a.TokenHash, a.ID))
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	return result, err
}
