// Package postgres is the driven adapter for PostgreSQL. It implements the
// core/ports/out repository interfaces with hand-written pgx/v5 — no ORM, no
// query generator (003.12 "Access from Go"). core/domain and core/usecases hold
// no SQL and no driver imports.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KafkaMetamorphosis/franz/migrations"
)

// Querier is the subset of pgx shared by *pgxpool.Pool and pgx.Tx. Repository
// methods accept a Querier so the same method runs standalone (pool) or inside a
// caller's transaction (tx) — the 003.12 "compound operations run in one
// transaction" rule.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// DB owns the connection pool.
type DB struct {
	pool *pgxpool.Pool
}

// New opens the pool and verifies connectivity.
func New(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// Pool exposes the underlying pool for repositories that run outside a
// transaction.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Close drains the pool.
func (db *DB) Close() { db.pool.Close() }

// WithTx runs fn inside a single transaction. It commits when fn returns nil and
// rolls back (preserving fn's error) otherwise; a panic also triggers rollback.
func (db *DB) WithTx(ctx context.Context, fn func(pgx.Tx) error) (err error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("postgres: rollback: %w", rbErr))
			}
			return
		}
		if cErr := tx.Commit(ctx); cErr != nil {
			err = fmt.Errorf("postgres: commit: %w", cErr)
		}
	}()
	return fn(tx)
}

// Migrate applies every embedded migration in lexical order, each in its own
// transaction. The files are written idempotently, so this is safe to run on
// every boot; Flyway remains the migration authority once the schema freezes
// (003.12). Callers gate this on config db.auto_migrate.
func (db *DB) Migrate(ctx context.Context) error {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("postgres: read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sql" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		body, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return fmt.Errorf("postgres: read %s: %w", name, err)
		}
		if err := db.WithTx(ctx, func(tx pgx.Tx) error {
			_, execErr := tx.Exec(ctx, string(body))
			return execErr
		}); err != nil {
			return fmt.Errorf("postgres: apply %s: %w", name, err)
		}
	}
	return nil
}
