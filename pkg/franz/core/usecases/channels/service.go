// Package channels is the Async Channel application service (003.4). It
// orchestrates the domain entity and the out ports; it holds no SQL and no
// transport types. The caller's realm is read from context.
package channels

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
)

// Service implements in.AsyncChannelService.
type Service struct {
	repo out.AsyncChannelRepository
}

var _ in.AsyncChannelService = (*Service)(nil)

// NewService wires the service to its repository.
func NewService(repo out.AsyncChannelRepository) *Service { return &Service{repo: repo} }

// Create registers a new channel (state ACTIVE, FRN assigned). It writes only
// the channel row — placement materialises the shards (ADR-API-009).
func (s *Service) Create(ctx context.Context, input in.CreateChannelInput) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	c, err := channel.New(r, input.Name, input.Type, input.ChannelPartitions, input.Labels, input.AccessPolicy)
	if err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, c); err != nil {
		return nil, err
	}
	return c, nil
}

// Get returns the channel by name, including a soft-deleted one.
func (s *Service) Get(ctx context.Context, name string) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Get(ctx, r.ID, name)
}

// List returns one page, ordered by name, with the 003.1 selector applied.
func (s *Service) List(ctx context.Context, input in.ListChannelsInput) (in.ChannelPage, error) {
	r := realm.MustFromContext(ctx)

	sel, err := selector.Parse(input.Selector)
	if err != nil {
		return in.ChannelPage{}, err
	}
	queryKey := pagetoken.QueryKey("async-channel", input.Selector)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ChannelPage{}, err
	}
	page, err := s.repo.List(ctx, out.ChannelQuery{
		RealmID:   r.ID,
		Selector:  sel,
		Limit:     pagetoken.ClampSize(input.PageSize),
		AfterName: after,
	})
	if err != nil {
		return in.ChannelPage{}, err
	}
	return in.ChannelPage{
		Channels:      page.Channels,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}

// Update applies the masked fields (labels only) under a row lock.
func (s *Service) Update(ctx context.Context, input in.UpdateChannelInput) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Mutate(ctx, r.ID, input.Name, func(c *channel.AsyncChannel) error {
		if input.Labels != nil {
			return c.SetLabels(*input.Labels)
		}
		return c.EnsureMutable()
	})
}

// SetAccessPolicy replaces the embedded document wholesale.
func (s *Service) SetAccessPolicy(
	ctx context.Context, name string, p accesspolicy.Policy,
) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Mutate(ctx, r.ID, name, func(c *channel.AsyncChannel) error {
		return c.SetAccessPolicy(p)
	})
}

// Delete soft-deletes the channel and cascades DELETED to every shard.
func (s *Service) Delete(ctx context.Context, name string) error {
	r := realm.MustFromContext(ctx)
	_, err := s.repo.MutateWithShards(ctx, r.ID, name,
		func(c *channel.AsyncChannel, shards []*topic.KafkaTopic) error {
			if err := c.Delete(); err != nil {
				return err
			}
			for _, sh := range shards {
				if sh.State != topic.StateDeleted {
					_ = sh.SetState(topic.StateDeleted)
				}
			}
			return nil
		})
	return err
}

// Pause moves the channel to PAUSED and pauses every non-deleted shard.
func (s *Service) Pause(ctx context.Context, name string) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.MutateWithShards(ctx, r.ID, name,
		func(c *channel.AsyncChannel, shards []*topic.KafkaTopic) error {
			if err := c.Pause(); err != nil {
				return err
			}
			for _, sh := range shards {
				if sh.State.CanTransition(topic.StatePaused) {
					_ = sh.SetState(topic.StatePaused)
				}
			}
			return nil
		})
}

// Resume moves the channel to ACTIVE and returns every paused shard to PENDING.
func (s *Service) Resume(ctx context.Context, name string) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.MutateWithShards(ctx, r.ID, name,
		func(c *channel.AsyncChannel, shards []*topic.KafkaTopic) error {
			if err := c.Resume(); err != nil {
				return err
			}
			for _, sh := range shards {
				if sh.State == topic.StatePaused {
					_ = sh.SetState(topic.StatePending)
				}
			}
			return nil
		})
}
