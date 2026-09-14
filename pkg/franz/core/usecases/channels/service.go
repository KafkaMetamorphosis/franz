// Package channels is the Async Channel application service (003.4). It
// orchestrates the domain entity and the out ports; it holds no SQL and no
// transport types. The caller's realm is read from context.
package channels

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
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
	// notifier pushes partition-assignment deltas to the in-scope Resource
	// Provider agents when a channel operation changes its shards' desired state
	// (005 ADR §1.3). Optional — nil in tests that do not exercise the agent wire.
	notifier out.PartitionNotifier
	// placer materialises the channel's async-channel shards (003.7). Optional —
	// nil in tests that do not exercise placement.
	placer out.ShardPlacer
	// clients backs ListChannelClients (003.5, 17.5) — the forward access view
	// needs every Client in the realm to evaluate the policy against. Optional —
	// nil in tests that do not exercise it.
	clients out.ClientRepository
}

var _ in.AsyncChannelService = (*Service)(nil)

// NewService wires the service to its repository, the partition notifier, the
// shard placer, and the Client repository the access-policy views read.
func NewService(
	repo out.AsyncChannelRepository, notifier out.PartitionNotifier, placer out.ShardPlacer,
	clients out.ClientRepository,
) *Service {
	return &Service{repo: repo, notifier: notifier, placer: placer, clients: clients}
}

// place runs a placement pass for one channel, after the caller's transaction
// has committed. Best-effort — the channel write already succeeded (003.7
// "channel create always succeeds") and the retry sweep is the safety net.
func (s *Service) place(ctx context.Context, realmID uuid.UUID, name string) {
	if s.placer == nil {
		return
	}
	s.placer.PlaceChannel(ctx, realmID, name)
}

// notifyShards forwards a shard change to the Resource Provider agents that
// hold the shards' clusters in scope. Best-effort: an agent that is not
// connected picks the change up on its next reconnect resync.
func (s *Service) notifyShards(ctx context.Context, realmID uuid.UUID, shards []*topic.KafkaTopic) {
	if s.notifier == nil || len(shards) == 0 {
		return
	}
	s.notifier.ShardsChanged(ctx, realmID, shards)
}

// Create registers a new channel (state ACTIVE, FRN assigned). It writes only
// the channel row — placement materialises the shards (ADR-API-009).
func (s *Service) Create(ctx context.Context, input in.CreateChannelInput) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	if err := placement.ValidateChannelLabels(input.Labels); err != nil {
		return nil, err
	}
	c, err := channel.New(r, input.Name, input.Type, input.ChannelPartitions, input.Labels, input.AccessPolicy)
	if err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, c); err != nil {
		return nil, err
	}
	s.place(ctx, r.ID, c.Name)
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

// Update applies the masked fields (labels only) under a row lock. A change to
// the reserved `franz.*` placement labels re-runs placement afterwards (003.7).
func (s *Service) Update(ctx context.Context, input in.UpdateChannelInput) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	if input.Labels != nil {
		if err := placement.ValidateChannelLabels(*input.Labels); err != nil {
			return nil, err
		}
	}
	var labelsBefore map[string]string
	updated, err := s.repo.Mutate(ctx, r.ID, input.Name, func(c *channel.AsyncChannel) error {
		labelsBefore = c.Labels
		if input.Labels != nil {
			return c.SetLabels(*input.Labels)
		}
		return c.EnsureMutable()
	})
	if err != nil {
		return nil, err
	}
	if placement.RulesChanged(labelsBefore, updated.Labels) {
		s.place(ctx, r.ID, updated.Name)
	}
	return updated, nil
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

// Delete soft-deletes the channel and cascades DELETED to every shard. The
// shards go out as REMOVED assignments so the agents delete the real topics.
func (s *Service) Delete(ctx context.Context, name string) error {
	r := realm.MustFromContext(ctx)
	var deleted []*topic.KafkaTopic
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
			deleted = shards
			return nil
		})
	if err != nil {
		return err
	}
	s.notifyShards(ctx, r.ID, deleted)
	return nil
}

// Pause moves the channel to PAUSED and pauses every non-deleted shard. The
// shards go out as PAUSED assignments so the agents stop managing them.
func (s *Service) Pause(ctx context.Context, name string) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	var paused []*topic.KafkaTopic
	c, err := s.repo.MutateWithShards(ctx, r.ID, name,
		func(c *channel.AsyncChannel, shards []*topic.KafkaTopic) error {
			if err := c.Pause(); err != nil {
				return err
			}
			for _, sh := range shards {
				if sh.State.CanTransition(topic.StatePaused) {
					_ = sh.SetState(topic.StatePaused)
				}
			}
			paused = shards
			return nil
		})
	if err != nil {
		return nil, err
	}
	s.notifyShards(ctx, r.ID, paused)
	return c, nil
}

// Resume moves the channel to ACTIVE and returns every paused shard to PENDING,
// re-offering them to the agents as SET.
func (s *Service) Resume(ctx context.Context, name string) (*channel.AsyncChannel, error) {
	r := realm.MustFromContext(ctx)
	var resumed []*topic.KafkaTopic
	c, err := s.repo.MutateWithShards(ctx, r.ID, name,
		func(c *channel.AsyncChannel, shards []*topic.KafkaTopic) error {
			if err := c.Resume(); err != nil {
				return err
			}
			for _, sh := range shards {
				if sh.State == topic.StatePaused {
					_ = sh.SetState(topic.StatePending)
				}
			}
			resumed = shards
			return nil
		})
	if err != nil {
		return nil, err
	}
	s.notifyShards(ctx, r.ID, resumed)
	return c, nil
}

// ListChannelClients evaluates this channel's access policy against one page
// of Clients (17.1–17.3) and returns only the ones granted anything — 003.5
// frames the view as "every client whose access policy matches", not an audit
// of every client regardless of outcome.
//
// The Client page is fetched once, unfiltered, and evaluated in Go — the same
// pattern every other List uses for its 003.1 selector (17's own "Selector-match
// cost" note: no special bound). A sparse-match page can come back with fewer
// rows than requested, or none; the caller pages forward with NextPageToken
// the same way it would past a selector that matched little.
func (s *Service) ListChannelClients(
	ctx context.Context, input in.ListChannelClientsInput,
) (in.ChannelClientAccessPage, error) {
	r := realm.MustFromContext(ctx)
	c, err := s.repo.Get(ctx, r.ID, input.Name)
	if err != nil {
		return in.ChannelClientAccessPage{}, err
	}

	queryKey := pagetoken.QueryKey("channel-clients", input.Name)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ChannelClientAccessPage{}, err
	}
	page, err := s.clients.List(ctx, out.ClientQuery{
		RealmID:   r.ID,
		Limit:     pagetoken.ClampSize(input.PageSize),
		AfterName: after,
	})
	if err != nil {
		return in.ChannelClientAccessPage{}, err
	}

	evaluator := accesspolicy.NewEvaluator(c.AccessPolicy)
	access := make([]in.ChannelClientAccess, 0, len(page.Clients))
	for _, cl := range page.Clients {
		eval := evaluator.Evaluate(cl.FRN.Path(), cl.Labels)
		if !eval.Granted() {
			continue
		}
		access = append(access, in.ChannelClientAccess{
			ClientFRN: cl.FRN, ClientLabels: cl.Labels,
			Effective: eval.Effective, MatchedBy: eval.MatchedBy,
		})
	}
	return in.ChannelClientAccessPage{
		Access:        access,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}
