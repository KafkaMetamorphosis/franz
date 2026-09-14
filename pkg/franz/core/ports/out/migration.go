package out

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/migration"
)

// ShardMigrationRepository persists shard_migration rows (003.12, 003.13).
type ShardMigrationRepository interface {
	// Create inserts a new row. errs.AlreadyExists if the source topic already
	// has a non-terminal migration (003.13's "one migration per shard at a
	// time" safety rule, enforced by a partial unique index).
	Create(ctx context.Context, m *migration.ShardMigration) error

	// Get returns a migration by id.
	Get(ctx context.Context, realmID, id uuid.UUID) (*migration.ShardMigration, error)

	// ListActiveByCluster returns every non-terminal migration whose source or
	// target is clusterID — the 18.5 concurrency-limit query.
	ListActiveByCluster(ctx context.Context, realmID, clusterID uuid.UUID) ([]*migration.ShardMigration, error)

	// ListNonTerminal returns every migration not yet DONE/FAILED, across the
	// whole realm — the sweep's work list. Not paginated: in-flight migrations
	// are expected to be few at once (18.5's concurrency limit keeps it so).
	ListNonTerminal(ctx context.Context) ([]*migration.ShardMigration, error)

	// ListByChannel returns every migration (terminal included) for one
	// channel, newest first — the audit/history view.
	ListByChannel(ctx context.Context, realmID, channelID uuid.UUID, limit int, afterCursor string) (page MigrationPage, err error)

	// Mutate loads the row FOR UPDATE, runs mutate, persists — one transaction.
	Mutate(ctx context.Context, realmID, id uuid.UUID,
		mutate func(*migration.ShardMigration) error) (*migration.ShardMigration, error)

	// PruneOlderThan deletes terminal (DONE/FAILED) rows completed before
	// cutoff — nightly retention, like every other Franz time series.
	PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// MigrationPage is one page of ListByChannel, ordered by started_at desc.
type MigrationPage struct {
	Migrations []*migration.ShardMigration
	LastCursor string
}
