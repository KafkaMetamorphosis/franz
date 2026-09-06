// Package dbtest serialises database integration tests. Test packages that hit
// the shared Postgres (they blanket-DELETE and re-seed fixtures) must not run
// concurrently — `go test ./...` runs packages in parallel. Lock holds a
// Postgres session-level advisory lock on a dedicated connection for the test's
// lifetime, so every caller serialises regardless of package or process.
package dbtest

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// advisoryKey is "franz" as a bigint (0x6672616e7a).
const advisoryKey int64 = 0x6672616e7a

// Lock blocks until it holds the shared advisory lock, then releases it on test
// cleanup. Call it right after opening the pool in a DB integration test.
func Lock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("dbtest: acquire connection: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryKey); err != nil {
		conn.Release()
		t.Fatalf("dbtest: advisory lock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", advisoryKey)
		conn.Release()
	})
}
