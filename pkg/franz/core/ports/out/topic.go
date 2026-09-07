package out

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
)

// TopicQuery parameterises TopicRepository.List. A nil filter pointer means "do
// not filter on that dimension". DELETED rows are excluded from List.
type TopicQuery struct {
	RealmID        uuid.UUID
	AsyncChannelID *uuid.UUID
	KafkaClusterID *uuid.UUID
	Limit          int    // page size (already clamped)
	AfterName      string // exclusive lower bound, "" ⇒ first page
}

// TopicPage is one page of a List result, ordered by name ascending.
type TopicPage struct {
	Topics    []*topic.KafkaTopic
	LastName  string
	TotalSize int
}

// TopicRepository persists Kafka Topic shards (003.12). Realm scoping is the
// caller's responsibility.
type TopicRepository interface {
	// Create inserts a new shard row. Used by the Async Channel (deliverable 10);
	// there is no create RPC. A name/FRN collision is errs.AlreadyExists.
	Create(ctx context.Context, t *topic.KafkaTopic) error

	// Get returns the shard by (realm, name), including a soft-deleted one.
	// errs.NotFound if absent.
	Get(ctx context.Context, realmID uuid.UUID, name string) (*topic.KafkaTopic, error)

	// List returns one page per TopicQuery.
	List(ctx context.Context, q TopicQuery) (TopicPage, error)

	// MutateChannelShards loads every non-deleted shard of the channel FOR
	// UPDATE, runs mutate on the set, and persists the result — one transaction,
	// so SetConsumption plus the sibling traffic-share re-normalisation are
	// atomic. Returns the persisted shards.
	MutateChannelShards(ctx context.Context, realmID, channelID uuid.UUID,
		mutate func([]*topic.KafkaTopic) error) ([]*topic.KafkaTopic, error)

	// ResolveChannelID maps an Async Channel name to its id within the realm.
	// errs.NotFound if there is no such channel.
	ResolveChannelID(ctx context.Context, realmID uuid.UUID, name string) (uuid.UUID, error)

	// CountLiveTopics is the ClusterTopicGuard query — non-deleted shards on a
	// cluster (003.3 delete guard).
	CountLiveTopics(ctx context.Context, clusterID uuid.UUID) (int, error)

	// ListByClusters returns every shard placed on any of clusterIDs, DELETED
	// rows included so a Resource Provider agent gets the REMOVED assignment
	// that tells it to delete the real topic (005 ADR §1.3). Empty clusterIDs
	// returns no rows. Not paginated — an agent's in-scope set is bounded.
	ListByClusters(ctx context.Context, realmID uuid.UUID, clusterIDs []uuid.UUID) ([]*topic.KafkaTopic, error)

	// MutateByFRN loads the shard identified by its prefix-less FRN path FOR
	// UPDATE, runs mutate, and persists the result — one transaction, so a
	// generation-gated reconciliation report cannot race a desired-state change.
	// errs.NotFound if there is no such shard in the realm.
	MutateByFRN(ctx context.Context, realmID uuid.UUID, frnPath string,
		mutate func(*topic.KafkaTopic) error) (*topic.KafkaTopic, error)
}
