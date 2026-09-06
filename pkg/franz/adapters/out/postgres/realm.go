package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// RealmRepo implements core/ports/out.RealmRepository.
type RealmRepo struct {
	db *DB
}

// NewRealmRepo wires the repository to the pool.
func NewRealmRepo(db *DB) *RealmRepo { return &RealmRepo{db: db} }

const realmColumns = `id, slug, name`

func scanRealm(row pgx.Row) (realm.Realm, error) {
	var r realm.Realm
	if err := row.Scan(&r.ID, &r.Slug, &r.Name); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return realm.Realm{}, errs.NotFoundf("realm not found")
		}
		return realm.Realm{}, errs.Internalf("scan realm").Wrap(err)
	}
	return r, nil
}

// GetBySlug implements RealmRepository.
func (repo *RealmRepo) GetBySlug(ctx context.Context, slug string) (realm.Realm, error) {
	row := repo.db.Pool().QueryRow(ctx,
		`SELECT `+realmColumns+` FROM realm WHERE slug = $1`, slug)
	return scanRealm(row)
}

// GetByID implements RealmRepository.
func (repo *RealmRepo) GetByID(ctx context.Context, id uuid.UUID) (realm.Realm, error) {
	row := repo.db.Pool().QueryRow(ctx,
		`SELECT `+realmColumns+` FROM realm WHERE id = $1`, id)
	return scanRealm(row)
}
