package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// ClientRepo implements out.ClientRepository with hand-written pgx (003.12).
type ClientRepo struct {
	db *DB
}

// NewClientRepo wires the repository to the pool.
func NewClientRepo(db *DB) *ClientRepo { return &ClientRepo{db: db} }

var _ out.ClientRepository = (*ClientRepo)(nil)

const clientColumns = `id, realm_id, name, frn, labels, created_at, updated_at`

func scanClient(sc rowScanner) (*client.Client, error) {
	var (
		c         client.Client
		frnPath   string
		labelsRaw []byte
	)
	err := sc.Scan(&c.ID, &c.RealmID, &c.Name, &frnPath, &labelsRaw, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("client not found")
		}
		return nil, errs.Internalf("scan client").Wrap(err)
	}
	f, err := frn.ParsePath(frnPath)
	if err != nil {
		return nil, errs.Internalf("stored frn %q is malformed", frnPath).Wrap(err)
	}
	c.FRN = f
	c.Labels = map[string]string{}
	if err := json.Unmarshal(labelsRaw, &c.Labels); err != nil {
		return nil, errs.Internalf("decode labels").Wrap(err)
	}
	return &c, nil
}

// Create inserts a new client row. A name/FRN collision with an active row is
// errs.AlreadyExists; a name reserved by a previous DeleteClient (003.10 "name
// / FRN never freed") is checked first and rejected the same way.
func (r *ClientRepo) Create(ctx context.Context, c *client.Client) error {
	reserved, err := r.nameReserved(ctx, c.RealmID, c.Name)
	if err != nil {
		return err
	}
	if reserved {
		return errs.Existsf(
			"client %q was previously deleted; its name and FRN cannot be reused", c.Name)
	}

	if c.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		c.ID = id
	}
	labels, _ := json.Marshal(nonNilMap(c.Labels))

	stored, err := scanClient(r.db.Pool().QueryRow(ctx, `
		INSERT INTO client (id, realm_id, name, frn, labels)
		VALUES ($1,$2,$3,$4,$5)
		RETURNING `+clientColumns,
		c.ID, c.RealmID, c.Name, c.FRN.Path(), labels))
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("client %q already exists", c.Name)
		}
		return err
	}
	*c = *stored
	return nil
}

func (r *ClientRepo) nameReserved(ctx context.Context, realmID uuid.UUID, name string) (bool, error) {
	var exists bool
	err := r.db.Pool().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM deleted_client_frn WHERE realm_id=$1 AND name=$2)`,
		realmID, name).Scan(&exists)
	if err != nil {
		return false, errs.Internalf("check reserved client name").Wrap(err)
	}
	return exists, nil
}

// Get returns the client by (realm, name). A previously deleted client's row
// is gone (not soft-deleted), so this is errs.NotFound the same as one that
// never existed.
func (r *ClientRepo) Get(ctx context.Context, realmID uuid.UUID, name string) (*client.Client, error) {
	return scanClient(r.db.Pool().QueryRow(ctx,
		`SELECT `+clientColumns+` FROM client WHERE realm_id=$1 AND name=$2`,
		realmID, name))
}

// List returns one page ordered by name, with the 003.1 selector applied in Go
// (matching every other List — 003.12 OQ2).
func (r *ClientRepo) List(ctx context.Context, q out.ClientQuery) (out.ClientPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}

	rows, err := r.db.Pool().Query(ctx,
		`SELECT `+clientColumns+` FROM client WHERE realm_id=$1 AND name > $2
		 ORDER BY name ASC LIMIT 5000`,
		q.RealmID, q.AfterName)
	if err != nil {
		return out.ClientPage{}, errs.Internalf("list clients").Wrap(err)
	}
	defer rows.Close()

	var page out.ClientPage
	for rows.Next() {
		c, err := scanClient(rows)
		if err != nil {
			return out.ClientPage{}, err
		}
		if !q.Selector.Match(c.Labels) {
			continue
		}
		page.Clients = append(page.Clients, c)
		if len(page.Clients) > limit {
			page.Clients = page.Clients[:limit]
			page.LastName = page.Clients[limit-1].Name
			break
		}
	}
	if err := rows.Err(); err != nil {
		return out.ClientPage{}, errs.Internalf("iterate clients").Wrap(err)
	}
	return page, nil
}

// Update loads the row FOR UPDATE, runs mutate, and persists the result in one
// transaction.
func (r *ClientRepo) Update(
	ctx context.Context, realmID uuid.UUID, name string,
	mutate func(*client.Client) error,
) (*client.Client, error) {
	var result *client.Client
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		c, err := scanClient(tx.QueryRow(ctx,
			`SELECT `+clientColumns+` FROM client
			 WHERE realm_id=$1 AND name=$2 FOR UPDATE`, realmID, name))
		if err != nil {
			return err
		}
		if err := mutate(c); err != nil {
			return err
		}
		labels, _ := json.Marshal(nonNilMap(c.Labels))
		updated, err := scanClient(tx.QueryRow(ctx, `
			UPDATE client SET labels=$1, updated_at=now()
			WHERE id=$2
			RETURNING `+clientColumns,
			labels, c.ID))
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	return result, err
}

// Delete removes the row and reserves its (realm, name) and frn in
// deleted_client_frn, in one transaction (003.10 "DeleteClient does not free
// the name / FRN"). Client has no state column to soft-delete into — the row
// itself is the only place that fact could otherwise live.
func (r *ClientRepo) Delete(ctx context.Context, realmID uuid.UUID, name string) error {
	return r.db.WithTx(ctx, func(tx pgx.Tx) error {
		var id uuid.UUID
		var frnPath string
		err := tx.QueryRow(ctx,
			`SELECT id, frn FROM client WHERE realm_id=$1 AND name=$2 FOR UPDATE`,
			realmID, name).Scan(&id, &frnPath)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return errs.NotFoundf("client %q not found", name)
			}
			return errs.Internalf("load client for delete").Wrap(err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO deleted_client_frn (realm_id, name, frn) VALUES ($1,$2,$3)`,
			realmID, name, frnPath); err != nil {
			return errs.Internalf("reserve deleted client name").Wrap(err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM client WHERE id=$1`, id); err != nil {
			return errs.Internalf("delete client").Wrap(err)
		}
		return nil
	})
}
