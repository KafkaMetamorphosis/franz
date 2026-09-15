package postgres

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/migration"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// ShardMigrationRepo implements out.ShardMigrationRepository (003.12, 003.13).
type ShardMigrationRepo struct {
	db *DB
}

// NewShardMigrationRepo wires the repository to the pool.
func NewShardMigrationRepo(db *DB) *ShardMigrationRepo { return &ShardMigrationRepo{db: db} }

var _ out.ShardMigrationRepository = (*ShardMigrationRepo)(nil)

const migrationColumns = `id, realm_id, async_channel_id, source_topic_id, target_topic_id,
	source_cluster_id, target_cluster_id, phase, reason, drain_deadline,
	failure_reason, started_at, completed_at, created_at, updated_at`

func scanMigration(sc rowScanner) (*migration.ShardMigration, error) {
	var (
		m                          migration.ShardMigration
		phase                      string
		drainDeadline, completedAt *time.Time
	)
	err := sc.Scan(&m.ID, &m.RealmID, &m.AsyncChannelID, &m.SourceTopicID, &m.TargetTopicID,
		&m.SourceClusterID, &m.TargetClusterID, &phase, &m.Reason, &drainDeadline,
		&m.FailureReason, &m.StartedAt, &completedAt, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("shard migration not found")
		}
		return nil, errs.Internalf("scan shard migration").Wrap(err)
	}
	m.Phase = migration.Phase(phase)
	if drainDeadline != nil {
		m.DrainDeadline = *drainDeadline
	}
	if completedAt != nil {
		m.CompletedAt = *completedAt
	}
	return &m, nil
}

// Create inserts a new row. The partial unique index on (source_topic_id)
// WHERE phase not in (DONE, FAILED) turns a second active migration for the
// same shard into a unique violation.
func (r *ShardMigrationRepo) Create(ctx context.Context, m *migration.ShardMigration) error {
	if m.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		m.ID = id
	}
	var drainDeadline, completedAt *time.Time
	if !m.DrainDeadline.IsZero() {
		drainDeadline = &m.DrainDeadline
	}
	if !m.CompletedAt.IsZero() {
		completedAt = &m.CompletedAt
	}

	stored, err := scanMigration(r.db.Pool().QueryRow(ctx, `
		INSERT INTO shard_migration
			(id, realm_id, async_channel_id, source_topic_id, target_topic_id,
			 source_cluster_id, target_cluster_id, phase, reason, drain_deadline,
			 failure_reason, started_at, completed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		RETURNING `+migrationColumns,
		m.ID, m.RealmID, m.AsyncChannelID, m.SourceTopicID, m.TargetTopicID,
		m.SourceClusterID, m.TargetClusterID, string(m.Phase), m.Reason, drainDeadline,
		m.FailureReason, m.StartedAt, completedAt))
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("shard %s already has an in-flight migration", m.SourceTopicID)
		}
		return err
	}
	*m = *stored
	return nil
}

// Get returns a migration by id.
func (r *ShardMigrationRepo) Get(ctx context.Context, realmID, id uuid.UUID) (*migration.ShardMigration, error) {
	return scanMigration(r.db.Pool().QueryRow(ctx,
		`SELECT `+migrationColumns+` FROM shard_migration WHERE realm_id=$1 AND id=$2`,
		realmID, id))
}

// ListActiveByCluster returns every non-terminal migration whose source or
// target is clusterID.
func (r *ShardMigrationRepo) ListActiveByCluster(
	ctx context.Context, realmID, clusterID uuid.UUID,
) ([]*migration.ShardMigration, error) {
	rows, err := r.db.Pool().Query(ctx, `
		SELECT `+migrationColumns+` FROM shard_migration
		WHERE realm_id=$1 AND phase NOT IN ('DONE','FAILED')
		  AND (source_cluster_id=$2 OR target_cluster_id=$2)
		ORDER BY started_at ASC`,
		realmID, clusterID)
	if err != nil {
		return nil, errs.Internalf("list active migrations by cluster").Wrap(err)
	}
	return collectMigrations(rows)
}

// ListNonTerminal returns every migration not yet DONE/FAILED, realm-agnostic
// — the sweep's work list.
func (r *ShardMigrationRepo) ListNonTerminal(ctx context.Context) ([]*migration.ShardMigration, error) {
	rows, err := r.db.Pool().Query(ctx, `
		SELECT `+migrationColumns+` FROM shard_migration
		WHERE phase NOT IN ('DONE','FAILED')
		ORDER BY started_at ASC`)
	if err != nil {
		return nil, errs.Internalf("list non-terminal migrations").Wrap(err)
	}
	return collectMigrations(rows)
}

// ListByChannel returns one channel's migration history (terminal included),
// newest first.
func (r *ShardMigrationRepo) ListByChannel(
	ctx context.Context, realmID, channelID uuid.UUID, limit int, afterCursor string,
) (out.MigrationPage, error) {
	if limit <= 0 {
		limit = 50
	}
	sql := `SELECT ` + migrationColumns + ` FROM shard_migration
		WHERE realm_id=$1 AND async_channel_id=$2`
	args := []any{realmID, channelID}
	if afterCursor != "" {
		ts, id, err := decodeEventCursor(afterCursor)
		if err != nil {
			return out.MigrationPage{}, err
		}
		sql += ` AND (started_at, id) < ($3, $4)`
		args = append(args, ts, id)
	}
	sql += ` ORDER BY started_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.MigrationPage{}, errs.Internalf("list channel migrations").Wrap(err)
	}
	migrations, err := collectMigrations(rows)
	if err != nil {
		return out.MigrationPage{}, err
	}

	var page out.MigrationPage
	if len(migrations) > limit {
		last := migrations[limit-1]
		page.LastCursor = encodeEventCursor(last.StartedAt, last.ID)
		migrations = migrations[:limit]
	}
	page.Migrations = migrations
	return page, nil
}

// Mutate loads the row FOR UPDATE, runs mutate, persists — one transaction.
func (r *ShardMigrationRepo) Mutate(
	ctx context.Context, realmID, id uuid.UUID,
	mutate func(*migration.ShardMigration) error,
) (*migration.ShardMigration, error) {
	var result *migration.ShardMigration
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		m, err := scanMigration(tx.QueryRow(ctx,
			`SELECT `+migrationColumns+` FROM shard_migration
			 WHERE realm_id=$1 AND id=$2 FOR UPDATE`, realmID, id))
		if err != nil {
			return err
		}
		if err := mutate(m); err != nil {
			return err
		}
		var drainDeadline, completedAt *time.Time
		if !m.DrainDeadline.IsZero() {
			drainDeadline = &m.DrainDeadline
		}
		if !m.CompletedAt.IsZero() {
			completedAt = &m.CompletedAt
		}
		updated, err := scanMigration(tx.QueryRow(ctx, `
			UPDATE shard_migration SET
				phase=$1, drain_deadline=$2, failure_reason=$3, completed_at=$4,
				updated_at=now()
			WHERE id=$5
			RETURNING `+migrationColumns,
			string(m.Phase), drainDeadline, m.FailureReason, completedAt, m.ID))
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	return result, err
}

// PruneOlderThan deletes terminal rows completed before cutoff.
func (r *ShardMigrationRepo) PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := r.db.Pool().Exec(ctx,
		`DELETE FROM shard_migration WHERE phase IN ('DONE','FAILED') AND completed_at < $1`,
		cutoff)
	if err != nil {
		return 0, errs.Internalf("prune shard migrations").Wrap(err)
	}
	return tag.RowsAffected(), nil
}

func collectMigrations(rows pgx.Rows) ([]*migration.ShardMigration, error) {
	defer rows.Close()
	var migrations []*migration.ShardMigration
	for rows.Next() {
		m, err := scanMigration(rows)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, m)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internalf("iterate shard migrations").Wrap(err)
	}
	return migrations, nil
}
