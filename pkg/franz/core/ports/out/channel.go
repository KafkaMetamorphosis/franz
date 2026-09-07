package out

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
)

// ChannelQuery parameterises AsyncChannelRepository.List.
type ChannelQuery struct {
	RealmID   uuid.UUID
	Selector  selector.Selector // empty ⇒ match all
	Limit     int
	AfterName string
}

// ChannelPage is one page of a List result, ordered by name ascending.
type ChannelPage struct {
	Channels  []*channel.AsyncChannel
	LastName  string
	TotalSize int
}

// AsyncChannelRepository persists Async Channels (003.12). CreateAsyncChannel
// writes only the channel row — shards are created by placement (ADR-API-009).
type AsyncChannelRepository interface {
	// Create inserts a new channel row. A name/FRN collision is
	// errs.AlreadyExists.
	Create(ctx context.Context, c *channel.AsyncChannel) error

	// Get returns the channel by (realm, name), soft-deleted rows included.
	Get(ctx context.Context, realmID uuid.UUID, name string) (*channel.AsyncChannel, error)

	// List returns one page per ChannelQuery.
	List(ctx context.Context, q ChannelQuery) (ChannelPage, error)

	// Mutate loads the channel row FOR UPDATE, runs mutate, persists — one
	// transaction. For label / access-policy changes that do not touch shards.
	Mutate(ctx context.Context, realmID uuid.UUID, name string,
		mutate func(*channel.AsyncChannel) error) (*channel.AsyncChannel, error)

	// ListActive returns every ACTIVE channel in the realm, ordered by name. A
	// cluster-triggered placement pass has to re-evaluate every channel at once,
	// and a realm's channel count is bounded, so this is not paginated.
	ListActive(ctx context.Context, realmID uuid.UUID) ([]*channel.AsyncChannel, error)

	// ListUnderplaced returns every ACTIVE channel — across all realms — with
	// fewer live kafka_topic rows than `channel_partitions`. It is the placement
	// retry sweep's work list (003.7); the sweep runs on a timer and has no realm
	// context of its own.
	ListUnderplaced(ctx context.Context) ([]*channel.AsyncChannel, error)

	// MutateWithShards additionally loads every non-deleted shard of the channel
	// FOR UPDATE and passes them to mutate, persisting the channel and every
	// shard in one transaction. For Pause / Resume / Delete cascades.
	MutateWithShards(ctx context.Context, realmID uuid.UUID, name string,
		mutate func(*channel.AsyncChannel, []*topic.KafkaTopic) error,
	) (*channel.AsyncChannel, error)
}
