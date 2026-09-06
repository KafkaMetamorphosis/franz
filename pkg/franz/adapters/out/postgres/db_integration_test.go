package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// dsn returns the test database DSN, or skips the test. Point it at a disposable
// Postgres (docker-compose up -d postgres):
//
//	FRANZ_TEST_DB_DSN=postgres://franz:franz@localhost:5432/franz?sslmode=disable
func openTestDB(t *testing.T) *postgres.DB {
	t.Helper()
	dsn := os.Getenv("FRANZ_TEST_DB_DSN")
	if dsn == "" {
		t.Skip("set FRANZ_TEST_DB_DSN to run postgres integration tests")
	}
	db, err := postgres.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	// openTestDB already migrated once; a second pass must not error.
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestWithTxCommitsAndRollsBack(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// rollback path: the error propagates and no write survives
	sentinel := errors.New("boom")
	err := db.WithTx(ctx, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, `INSERT INTO realm (id, slug, name)
			VALUES ('00000000-0000-0000-0000-0000000000ff', 'rollback-me', 'x')`)
		if e != nil {
			return e
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx error = %v, want sentinel", err)
	}
	var n int
	if e := db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM realm WHERE slug = 'rollback-me'`).Scan(&n); e != nil {
		t.Fatal(e)
	}
	if n != 0 {
		t.Fatalf("rolled-back row survived: %d", n)
	}

	// commit path
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, `INSERT INTO realm (id, slug, name)
			VALUES ('00000000-0000-0000-0000-0000000000fe', 'commit-me', 'x')
			ON CONFLICT (id) DO NOTHING`)
		return e
	}); err != nil {
		t.Fatalf("commit WithTx: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Pool().Exec(ctx, `DELETE FROM realm WHERE slug = 'commit-me'`)
	})
	if e := db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM realm WHERE slug = 'commit-me'`).Scan(&n); e != nil {
		t.Fatal(e)
	}
	if n != 1 {
		t.Fatalf("committed row missing: %d", n)
	}
}

func TestRealmRepoGetsSeededDefault(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRealmRepo(db)
	ctx := context.Background()

	bySlug, err := repo.GetBySlug(ctx, realm.DefaultSlug)
	if err != nil {
		t.Fatalf("GetBySlug: %v", err)
	}
	if bySlug.ID != realm.DefaultID {
		t.Errorf("default realm id = %s, want %s", bySlug.ID, realm.DefaultID)
	}

	byID, err := repo.GetByID(ctx, realm.DefaultID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if byID.Slug != realm.DefaultSlug {
		t.Errorf("slug = %s", byID.Slug)
	}

	if _, err := repo.GetBySlug(ctx, "does-not-exist"); err == nil {
		t.Error("GetBySlug(missing) should error")
	}
}
