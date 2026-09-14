package in

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/migration"
)

// ListShardMigrationsInput parameterises the audit/history view for one
// channel.
type ListShardMigrationsInput struct {
	AsyncChannel string
	PageSize     int32
	PageToken    string
}

// ShardMigrationPage is a page of ListShardMigrations results.
type ShardMigrationPage struct {
	Migrations    []*migration.ShardMigration
	NextPageToken string
}

// MigrationService is the driving port for shard migration (003.13): the
// explicit operator entry point (003.13 OQ1, resolved in favour of one) plus
// the read views over it. Internal triggers (drain taint, misplaced-shard
// relocation, governance, cluster-delete force) call the same Create path
// this exposes, not a separate one.
type MigrationService interface {
	// MigrateKafkaTopic starts moving one shard to targetCluster. Validates the
	// shard exists and is READY, the target cluster is ACTIVE and eligible
	// under the owning channel's placement rules, the two clusters differ, the
	// shard has no other non-terminal migration, and the target cluster's
	// concurrency limit (18.5) is not exceeded.
	MigrateKafkaTopic(ctx context.Context, topicName, targetCluster string) (*migration.ShardMigration, error)

	// MigrateCluster starts moving every live shard off sourceCluster, picking
	// each one's target independently via its channel's placement rules
	// (excluding sourceCluster itself). Shards with no eligible target are
	// skipped, not failed — the caller sees which in the returned list's
	// length vs. the cluster's live-shard count.
	MigrateCluster(ctx context.Context, sourceCluster string, reason string) ([]*migration.ShardMigration, error)

	// GetShardMigration returns one migration by id.
	GetShardMigration(ctx context.Context, id uuid.UUID) (*migration.ShardMigration, error)

	// ListShardMigrations returns one channel's migration history, newest
	// first.
	ListShardMigrations(ctx context.Context, in ListShardMigrationsInput) (ShardMigrationPage, error)
}
