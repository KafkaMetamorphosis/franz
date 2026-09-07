package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// TopicRepo implements out.TopicRepository with hand-written pgx (003.12).
type TopicRepo struct {
	db *DB
}

// NewTopicRepo wires the repository to the pool.
func NewTopicRepo(db *DB) *TopicRepo { return &TopicRepo{db: db} }

var _ out.TopicRepository = (*TopicRepo)(nil)

// topicCols are the base columns, unqualified — for INSERT ... RETURNING and
// UPDATE ... RETURNING (no join).
const topicCols = `id, realm_id, async_channel_id, kafka_cluster_id, name, frn,
	topic_configuration, materialized_configuration, partitions, replication_factor,
	state, consumption, traffic_share_value, traffic_share_unit, generation,
	reconciled_generation, last_reconcile_message, misplaced, misplaced_reason,
	created_at, updated_at`

// topicSelectJoined is the read query: base columns qualified as `t.*` plus the
// channel and cluster names the API renders.
const topicSelectJoined = `
	SELECT t.id, t.realm_id, t.async_channel_id, t.kafka_cluster_id, t.name, t.frn,
	       t.topic_configuration, t.materialized_configuration, t.partitions,
	       t.replication_factor, t.state, t.consumption, t.traffic_share_value,
	       t.traffic_share_unit, t.generation, t.reconciled_generation,
	       t.last_reconcile_message, t.misplaced, t.misplaced_reason,
	       t.created_at, t.updated_at,
	       ac.name, COALESCE(kc.name, '')
	FROM kafka_topic t
	JOIN async_channel ac ON ac.id = t.async_channel_id
	LEFT JOIN kafka_cluster kc ON kc.id = t.kafka_cluster_id`

// scanTopic reads a row. When joined is true it also reads the trailing
// channel-name + cluster-name columns from topicSelectJoined.
func scanTopic(sc rowScanner, joined bool) (*topic.KafkaTopic, error) {
	var (
		t                       topic.KafkaTopic
		frnPath, state, consume string
		clusterID               pgtype.UUID
		topicCfgRaw, matCfgRaw  []byte
		reconciledGen           *int64
	)
	dest := []any{
		&t.ID, &t.RealmID, &t.AsyncChannelID, &clusterID, &t.Name, &frnPath,
		&topicCfgRaw, &matCfgRaw, &t.Partitions, &t.ReplicationFactor,
		&state, &consume, &t.TrafficShare.Value, &t.TrafficShare.Unit, &t.Generation,
		&reconciledGen, &t.LastReconcileMessage, &t.Misplaced, &t.MisplacedReason,
		&t.CreatedAt, &t.UpdatedAt,
	}
	if joined {
		dest = append(dest, &t.ChannelName, &t.ClusterName)
	}
	if err := sc.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("kafka topic not found")
		}
		return nil, errs.Internalf("scan kafka topic").Wrap(err)
	}
	f, err := frn.ParsePath(frnPath)
	if err != nil {
		return nil, errs.Internalf("stored frn %q is malformed", frnPath).Wrap(err)
	}
	t.FRN = f
	t.State = topic.State(state)
	t.Consumption = topic.Consumption(consume)
	t.ReconciledGeneration = reconciledGen
	if clusterID.Valid {
		id := uuid.UUID(clusterID.Bytes)
		t.KafkaClusterID = &id
	}
	t.TopicConfiguration = map[string]string{}
	t.MaterializedConfiguration = map[string]string{}
	if err := json.Unmarshal(topicCfgRaw, &t.TopicConfiguration); err != nil {
		return nil, errs.Internalf("decode topic_configuration").Wrap(err)
	}
	if err := json.Unmarshal(matCfgRaw, &t.MaterializedConfiguration); err != nil {
		return nil, errs.Internalf("decode materialized_configuration").Wrap(err)
	}
	return &t, nil
}

// rowQuerier is the QueryRow surface shared by the pool and a transaction, so
// one INSERT statement serves both Create and the placement transaction.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Create inserts a new shard row.
func (r *TopicRepo) Create(ctx context.Context, t *topic.KafkaTopic) error {
	return insertTopic(ctx, r.db.Pool(), t)
}

// insertTopic writes one new shard row and refreshes it in place from what
// Postgres stored.
func insertTopic(ctx context.Context, q rowQuerier, t *topic.KafkaTopic) error {
	if t.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		t.ID = id
	}
	if t.TrafficShare.Unit == "" {
		t.TrafficShare.Unit = topic.TrafficShareUnit
	}
	channelName, clusterName := t.ChannelName, t.ClusterName
	topicCfg, _ := json.Marshal(nonNilMap(t.TopicConfiguration))
	matCfg, _ := json.Marshal(nonNilMap(t.MaterializedConfiguration))

	stored, err := scanTopic(q.QueryRow(ctx, `
		INSERT INTO kafka_topic
			(id, realm_id, async_channel_id, kafka_cluster_id, name, frn,
			 topic_configuration, materialized_configuration, partitions,
			 replication_factor, state, consumption, traffic_share_value,
			 traffic_share_unit, generation, misplaced, misplaced_reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		RETURNING `+topicCols,
		t.ID, t.RealmID, t.AsyncChannelID, t.KafkaClusterID, t.Name, t.FRN.Path(),
		topicCfg, matCfg, t.Partitions, t.ReplicationFactor, string(t.State),
		string(t.Consumption), t.TrafficShare.Value, t.TrafficShare.Unit, t.Generation,
		t.Misplaced, t.MisplacedReason), false)
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("kafka topic %q already exists", t.Name)
		}
		return err
	}
	// The INSERT does not join, so carry the read-path projections the caller
	// already knows across the refresh.
	stored.ChannelName, stored.ClusterName = channelName, clusterName
	*t = *stored
	return nil
}

// Get returns the shard by (realm, name), soft-deleted rows included.
func (r *TopicRepo) Get(ctx context.Context, realmID uuid.UUID, name string) (*topic.KafkaTopic, error) {
	return scanTopic(r.db.Pool().QueryRow(ctx,
		topicSelectJoined+` WHERE t.realm_id=$1 AND t.name=$2`, realmID, name), true)
}

// List returns one page ordered by name. DELETED rows are excluded. Filters are
// pushed to SQL, so pagination is exact.
func (r *TopicRepo) List(ctx context.Context, q out.TopicQuery) (out.TopicPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	sql := topicSelectJoined + `
		WHERE t.realm_id=$1 AND t.name > $2 AND t.state <> 'DELETED'`
	args := []any{q.RealmID, q.AfterName}
	if q.AsyncChannelID != nil {
		args = append(args, *q.AsyncChannelID)
		sql += ` AND t.async_channel_id = $` + strconv.Itoa(len(args))
	}
	if q.KafkaClusterID != nil {
		args = append(args, *q.KafkaClusterID)
		sql += ` AND t.kafka_cluster_id = $` + strconv.Itoa(len(args))
	}
	args = append(args, limit+1)
	sql += ` ORDER BY t.name ASC LIMIT $` + strconv.Itoa(len(args))

	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return out.TopicPage{}, errs.Internalf("list kafka topics").Wrap(err)
	}
	defer rows.Close()

	var page out.TopicPage
	for rows.Next() {
		t, err := scanTopic(rows, true)
		if err != nil {
			return out.TopicPage{}, err
		}
		page.Topics = append(page.Topics, t)
	}
	if err := rows.Err(); err != nil {
		return out.TopicPage{}, errs.Internalf("iterate kafka topics").Wrap(err)
	}
	if len(page.Topics) > limit {
		page.Topics = page.Topics[:limit]
		page.LastName = page.Topics[limit-1].Name
	}
	return page, nil
}

// MutateChannelShards loads every non-deleted shard of a channel FOR UPDATE,
// runs mutate on the set, and persists the result — one transaction.
func (r *TopicRepo) MutateChannelShards(
	ctx context.Context, realmID, channelID uuid.UUID,
	mutate func([]*topic.KafkaTopic) error,
) ([]*topic.KafkaTopic, error) {
	var result []*topic.KafkaTopic
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			topicSelectJoined+`
			 WHERE t.realm_id=$1 AND t.async_channel_id=$2 AND t.state <> 'DELETED'
			 ORDER BY t.name ASC FOR UPDATE OF t`, realmID, channelID)
		if err != nil {
			return errs.Internalf("load channel shards").Wrap(err)
		}
		var shards []*topic.KafkaTopic
		for rows.Next() {
			t, err := scanTopic(rows, true)
			if err != nil {
				rows.Close()
				return err
			}
			shards = append(shards, t)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return errs.Internalf("iterate channel shards").Wrap(err)
		}
		if len(shards) == 0 {
			return errs.NotFoundf("async channel has no shards")
		}

		if err := mutate(shards); err != nil {
			return err
		}

		for _, t := range shards {
			if err := persistTopicTx(ctx, tx, t); err != nil {
				return err
			}
		}
		result = shards
		return nil
	})
	return result, err
}

// PlaceChannelShards runs one placement pass for a channel in a single
// transaction (ADR-API-005): it locks the async_channel row and its live shard
// rows FOR UPDATE — so two concurrent passes cannot both materialise the same
// async-channel shard index — hands the loaded rows to plan, then inserts the
// Create set and persists the Update set.
func (r *TopicRepo) PlaceChannelShards(
	ctx context.Context, realmID, channelID uuid.UUID,
	plan func(existing []*topic.KafkaTopic) (out.ShardPlan, error),
) ([]*topic.KafkaTopic, error) {
	var written []*topic.KafkaTopic
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		var lockedID uuid.UUID
		if err := tx.QueryRow(ctx,
			`SELECT id FROM async_channel WHERE id=$1 AND realm_id=$2 FOR UPDATE`,
			channelID, realmID).Scan(&lockedID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return errs.NotFoundf("async channel not found")
			}
			return errs.Internalf("lock async channel for placement").Wrap(err)
		}

		// Soft-deleted rows are loaded too: they still own their (realm, name),
		// so placement must see them to know an async-channel shard index is
		// taken rather than trying to insert over it.
		rows, err := tx.Query(ctx,
			topicSelectJoined+`
			 WHERE t.realm_id=$1 AND t.async_channel_id=$2
			 ORDER BY t.name ASC FOR UPDATE OF t`, realmID, channelID)
		if err != nil {
			return errs.Internalf("load channel shards for placement").Wrap(err)
		}
		var existing []*topic.KafkaTopic
		for rows.Next() {
			shard, err := scanTopic(rows, true)
			if err != nil {
				rows.Close()
				return err
			}
			existing = append(existing, shard)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return errs.Internalf("iterate channel shards for placement").Wrap(err)
		}

		shardPlan, err := plan(existing)
		if err != nil {
			return err
		}

		written = nil
		for _, shard := range shardPlan.Create {
			if err := insertTopic(ctx, tx, shard); err != nil {
				return err
			}
			written = append(written, shard)
		}
		for _, shard := range shardPlan.Update {
			if err := persistTopicTx(ctx, tx, shard); err != nil {
				return err
			}
			written = append(written, shard)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return written, nil
}

// persistTopicTx writes a mutated shard's columns and refreshes it in place,
// keeping the joined ChannelName / ClusterName (immutable within a mutate).
func persistTopicTx(ctx context.Context, tx pgx.Tx, t *topic.KafkaTopic) error {
	channelName, clusterName := t.ChannelName, t.ClusterName
	topicCfg, _ := json.Marshal(nonNilMap(t.TopicConfiguration))
	matCfg, _ := json.Marshal(nonNilMap(t.MaterializedConfiguration))
	updated, err := scanTopic(tx.QueryRow(ctx, `
		UPDATE kafka_topic SET
			kafka_cluster_id=$1, topic_configuration=$2,
			materialized_configuration=$3, partitions=$4, replication_factor=$5,
			state=$6, consumption=$7, traffic_share_value=$8,
			traffic_share_unit=$9, generation=$10, reconciled_generation=$11,
			last_reconcile_message=$12, misplaced=$13, misplaced_reason=$14,
			updated_at=now()
		WHERE id=$15
		RETURNING `+topicCols,
		t.KafkaClusterID, topicCfg, matCfg, t.Partitions, t.ReplicationFactor,
		string(t.State), string(t.Consumption), t.TrafficShare.Value,
		t.TrafficShare.Unit, t.Generation, t.ReconciledGeneration,
		t.LastReconcileMessage, t.Misplaced, t.MisplacedReason, t.ID), false)
	if err != nil {
		return err
	}
	updated.ChannelName = channelName
	if updated.KafkaClusterID != nil {
		updated.ClusterName = clusterName
	}
	*t = *updated
	return nil
}

// ListByClusters returns every shard placed on any of clusterIDs, DELETED rows
// included, ordered by name (005 ADR §1.3 — the agent needs the REMOVED
// assignment for a soft-deleted partition).
func (r *TopicRepo) ListByClusters(
	ctx context.Context, realmID uuid.UUID, clusterIDs []uuid.UUID,
) ([]*topic.KafkaTopic, error) {
	if len(clusterIDs) == 0 {
		return nil, nil
	}
	rows, err := r.db.Pool().Query(ctx,
		topicSelectJoined+`
		 WHERE t.realm_id=$1 AND t.kafka_cluster_id = ANY($2)
		 ORDER BY t.name ASC`, realmID, clusterIDs)
	if err != nil {
		return nil, errs.Internalf("list kafka topics by cluster").Wrap(err)
	}
	defer rows.Close()

	var out []*topic.KafkaTopic
	for rows.Next() {
		t, err := scanTopic(rows, true)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internalf("iterate kafka topics by cluster").Wrap(err)
	}
	return out, nil
}

// MutateByFRN loads the shard by its prefix-less FRN path FOR UPDATE, runs
// mutate, and persists the result — one transaction, so a generation-gated
// reconciliation report cannot race a desired-state change.
func (r *TopicRepo) MutateByFRN(
	ctx context.Context, realmID uuid.UUID, frnPath string,
	mutate func(*topic.KafkaTopic) error,
) (*topic.KafkaTopic, error) {
	var result *topic.KafkaTopic
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		t, err := scanTopic(tx.QueryRow(ctx,
			topicSelectJoined+`
			 WHERE t.realm_id=$1 AND t.frn=$2 FOR UPDATE OF t`, realmID, frnPath), true)
		if err != nil {
			return err
		}
		if err := mutate(t); err != nil {
			return err
		}
		if err := persistTopicTx(ctx, tx, t); err != nil {
			return err
		}
		result = t
		return nil
	})
	return result, err
}

// ResolveChannelID maps an Async Channel name to its id within the realm.
func (r *TopicRepo) ResolveChannelID(ctx context.Context, realmID uuid.UUID, name string) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.db.Pool().QueryRow(ctx,
		`SELECT id FROM async_channel WHERE realm_id=$1 AND name=$2`, realmID, name).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, errs.NotFoundf("async channel %q not found", name)
		}
		return uuid.Nil, errs.Internalf("resolve async channel").Wrap(err)
	}
	return id, nil
}

// CountLiveTopics is the ClusterTopicGuard query (003.3 delete guard).
func (r *TopicRepo) CountLiveTopics(ctx context.Context, clusterID uuid.UUID) (int, error) {
	var n int
	err := r.db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM kafka_topic WHERE kafka_cluster_id=$1 AND state <> 'DELETED'`,
		clusterID).Scan(&n)
	if err != nil {
		return 0, errs.Internalf("count live topics").Wrap(err)
	}
	return n, nil
}
