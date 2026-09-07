package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// ChannelRepo implements out.AsyncChannelRepository with hand-written pgx.
type ChannelRepo struct {
	db *DB
}

// NewChannelRepo wires the repository to the pool.
func NewChannelRepo(db *DB) *ChannelRepo { return &ChannelRepo{db: db} }

var _ out.AsyncChannelRepository = (*ChannelRepo)(nil)

const channelColumns = `id, realm_id, name, frn, type, channel_partitions, labels,
	access_policy, state, created_at, updated_at`

// --- access_policy on-disk shape (domain stays tag-free) ---

type principalRow struct {
	ClientFRN     string `json:"client_frn,omitempty"`
	LabelSelector string `json:"labels,omitempty"`
}

type statementRow struct {
	Effect      string       `json:"effect"`
	Principal   principalRow `json:"principal"`
	Permissions []string     `json:"permissions"`
}

type policyRow struct {
	Statements []statementRow `json:"statements"`
}

func marshalPolicy(p accesspolicy.Policy) []byte {
	rows := policyRow{Statements: make([]statementRow, 0, len(p.Statements))}
	for _, s := range p.Statements {
		perms := make([]string, len(s.Permissions))
		for i, perm := range s.Permissions {
			perms[i] = string(perm)
		}
		rows.Statements = append(rows.Statements, statementRow{
			Effect:      string(s.Effect),
			Principal:   principalRow{ClientFRN: s.Principal.ClientFRN, LabelSelector: s.Principal.LabelSelector},
			Permissions: perms,
		})
	}
	b, _ := json.Marshal(rows)
	return b
}

func unmarshalPolicy(raw []byte) (accesspolicy.Policy, error) {
	if len(raw) == 0 {
		return accesspolicy.Policy{}, nil
	}
	var rows policyRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return accesspolicy.Policy{}, errs.Internalf("decode access_policy").Wrap(err)
	}
	p := accesspolicy.Policy{Statements: make([]accesspolicy.Statement, 0, len(rows.Statements))}
	for _, s := range rows.Statements {
		perms := make([]accesspolicy.Permission, len(s.Permissions))
		for i, perm := range s.Permissions {
			perms[i] = accesspolicy.Permission(perm)
		}
		p.Statements = append(p.Statements, accesspolicy.Statement{
			Effect: accesspolicy.Effect(s.Effect),
			Principal: accesspolicy.Principal{
				ClientFRN: s.Principal.ClientFRN, LabelSelector: s.Principal.LabelSelector,
			},
			Permissions: perms,
		})
	}
	return p, nil
}

func scanChannel(sc rowScanner) (*channel.AsyncChannel, error) {
	var (
		c                    channel.AsyncChannel
		frnPath, typ, state  string
		labelsRaw, policyRaw []byte
	)
	err := sc.Scan(&c.ID, &c.RealmID, &c.Name, &frnPath, &typ, &c.ChannelPartitions,
		&labelsRaw, &policyRaw, &state, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("async channel not found")
		}
		return nil, errs.Internalf("scan async channel").Wrap(err)
	}
	f, err := frn.ParsePath(frnPath)
	if err != nil {
		return nil, errs.Internalf("stored frn %q is malformed", frnPath).Wrap(err)
	}
	c.FRN = f
	c.Type = channel.Type(typ)
	c.State = channel.State(state)
	c.Labels = map[string]string{}
	if err := json.Unmarshal(labelsRaw, &c.Labels); err != nil {
		return nil, errs.Internalf("decode labels").Wrap(err)
	}
	if c.AccessPolicy, err = unmarshalPolicy(policyRaw); err != nil {
		return nil, err
	}
	return &c, nil
}

// Create inserts a new channel row. Shards are not created here (ADR-API-009).
func (r *ChannelRepo) Create(ctx context.Context, c *channel.AsyncChannel) error {
	if c.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		c.ID = id
	}
	labels, _ := json.Marshal(nonNilMap(c.Labels))
	policy := marshalPolicy(c.AccessPolicy)

	stored, err := scanChannel(r.db.Pool().QueryRow(ctx, `
		INSERT INTO async_channel
			(id, realm_id, name, frn, type, channel_partitions, labels, access_policy, state)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING `+channelColumns,
		c.ID, c.RealmID, c.Name, c.FRN.Path(), string(c.Type), c.ChannelPartitions,
		labels, policy, string(c.State)))
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("async channel %q already exists", c.Name)
		}
		return err
	}
	*c = *stored
	return nil
}

// Get returns the channel by (realm, name), soft-deleted rows included.
func (r *ChannelRepo) Get(ctx context.Context, realmID uuid.UUID, name string) (*channel.AsyncChannel, error) {
	return scanChannel(r.db.Pool().QueryRow(ctx,
		`SELECT `+channelColumns+` FROM async_channel WHERE realm_id=$1 AND name=$2`,
		realmID, name))
}

// List returns one page ordered by name, with the 003.1 selector applied in Go.
func (r *ChannelRepo) List(ctx context.Context, q out.ChannelQuery) (out.ChannelPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.db.Pool().Query(ctx,
		`SELECT `+channelColumns+` FROM async_channel
		 WHERE realm_id=$1 AND name > $2 AND state <> 'DELETED'
		 ORDER BY name ASC LIMIT 5000`, q.RealmID, q.AfterName)
	if err != nil {
		return out.ChannelPage{}, errs.Internalf("list async channels").Wrap(err)
	}
	defer rows.Close()

	var page out.ChannelPage
	for rows.Next() {
		c, err := scanChannel(rows)
		if err != nil {
			return out.ChannelPage{}, err
		}
		if !q.Selector.Match(c.Labels) {
			continue
		}
		page.Channels = append(page.Channels, c)
		if len(page.Channels) > limit {
			page.Channels = page.Channels[:limit]
			page.LastName = page.Channels[limit-1].Name
			break
		}
	}
	if err := rows.Err(); err != nil {
		return out.ChannelPage{}, errs.Internalf("iterate async channels").Wrap(err)
	}
	return page, nil
}

// ListActive returns every ACTIVE channel in the realm, ordered by name.
func (r *ChannelRepo) ListActive(
	ctx context.Context, realmID uuid.UUID,
) ([]*channel.AsyncChannel, error) {
	return r.collectChannels(ctx,
		`SELECT `+channelColumns+` FROM async_channel
		 WHERE realm_id=$1 AND state='ACTIVE' ORDER BY name ASC`, realmID)
}

// ListUnderplaced returns every ACTIVE channel, in any realm, that has fewer
// live kafka_topic rows than `channel_partitions` — the placement retry sweep's
// work list (003.7). Ordered by (realm, name) so the sweep is deterministic.
func (r *ChannelRepo) ListUnderplaced(ctx context.Context) ([]*channel.AsyncChannel, error) {
	return r.collectChannels(ctx, `
		SELECT `+prefixedChannelColumns+` FROM async_channel c
		WHERE c.state='ACTIVE'
		  AND (SELECT count(*) FROM kafka_topic t
		       WHERE t.async_channel_id = c.id AND t.state <> 'DELETED')
		      < c.channel_partitions
		ORDER BY c.realm_id ASC, c.name ASC`)
}

// prefixedChannelColumns is channelColumns qualified for a query that aliases
// async_channel as `c`.
const prefixedChannelColumns = `c.id, c.realm_id, c.name, c.frn, c.type,
	c.channel_partitions, c.labels, c.access_policy, c.state, c.created_at, c.updated_at`

func (r *ChannelRepo) collectChannels(
	ctx context.Context, sql string, args ...any,
) ([]*channel.AsyncChannel, error) {
	rows, err := r.db.Pool().Query(ctx, sql, args...)
	if err != nil {
		return nil, errs.Internalf("list async channels").Wrap(err)
	}
	defer rows.Close()

	var channels []*channel.AsyncChannel
	for rows.Next() {
		c, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		channels = append(channels, c)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internalf("iterate async channels").Wrap(err)
	}
	return channels, nil
}

// Mutate loads the channel FOR UPDATE, runs mutate, persists — one transaction.
func (r *ChannelRepo) Mutate(
	ctx context.Context, realmID uuid.UUID, name string,
	mutate func(*channel.AsyncChannel) error,
) (*channel.AsyncChannel, error) {
	var result *channel.AsyncChannel
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		c, err := scanChannel(tx.QueryRow(ctx,
			`SELECT `+channelColumns+` FROM async_channel
			 WHERE realm_id=$1 AND name=$2 FOR UPDATE`, realmID, name))
		if err != nil {
			return err
		}
		if err := mutate(c); err != nil {
			return err
		}
		updated, err := persistChannel(ctx, tx, c)
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	return result, err
}

// MutateWithShards additionally locks + persists the channel's shards.
func (r *ChannelRepo) MutateWithShards(
	ctx context.Context, realmID uuid.UUID, name string,
	mutate func(*channel.AsyncChannel, []*topic.KafkaTopic) error,
) (*channel.AsyncChannel, error) {
	var result *channel.AsyncChannel
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		c, err := scanChannel(tx.QueryRow(ctx,
			`SELECT `+channelColumns+` FROM async_channel
			 WHERE realm_id=$1 AND name=$2 FOR UPDATE`, realmID, name))
		if err != nil {
			return err
		}
		shardRows, err := tx.Query(ctx,
			topicSelectJoined+`
			 WHERE t.realm_id=$1 AND t.async_channel_id=$2 AND t.state <> 'DELETED'
			 ORDER BY t.name ASC FOR UPDATE OF t`, realmID, c.ID)
		if err != nil {
			return errs.Internalf("load channel shards").Wrap(err)
		}
		var shards []*topic.KafkaTopic
		for shardRows.Next() {
			sh, err := scanTopic(shardRows, true)
			if err != nil {
				shardRows.Close()
				return err
			}
			shards = append(shards, sh)
		}
		shardRows.Close()
		if err := shardRows.Err(); err != nil {
			return errs.Internalf("iterate channel shards").Wrap(err)
		}

		if err := mutate(c, shards); err != nil {
			return err
		}

		updated, err := persistChannel(ctx, tx, c)
		if err != nil {
			return err
		}
		for _, sh := range shards {
			if err := persistTopicTx(ctx, tx, sh); err != nil {
				return err
			}
		}
		result = updated
		return nil
	})
	return result, err
}

func persistChannel(ctx context.Context, tx pgx.Tx, c *channel.AsyncChannel) (*channel.AsyncChannel, error) {
	labels, _ := json.Marshal(nonNilMap(c.Labels))
	policy := marshalPolicy(c.AccessPolicy)
	return scanChannel(tx.QueryRow(ctx, `
		UPDATE async_channel SET
			labels=$1, access_policy=$2, state=$3, updated_at=now()
		WHERE id=$4
		RETURNING `+channelColumns,
		labels, policy, string(c.State), c.ID))
}
