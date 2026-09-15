// Package clients is the Client application service (003.10). It orchestrates
// the domain entity and the out ports; it holds no SQL and no transport types.
// The caller's realm is read from context.
package clients

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
)

// Service implements in.ClientService.
type Service struct {
	repo   out.ClientRepository
	groups out.ObservedConsumerGroupRepository
	// channels backs ListClientChannelAccess (003.5, 003.10, 17.6) — the reverse
	// access view needs every Async Channel in the realm to evaluate against
	// this client. Optional — nil in tests that do not exercise it.
	channels out.AsyncChannelRepository
}

var _ in.ClientService = (*Service)(nil)

// NewService wires the service to its ports, including the Async Channel
// repository the reverse access-policy view reads.
func NewService(
	repo out.ClientRepository, groups out.ObservedConsumerGroupRepository,
	channels out.AsyncChannelRepository,
) *Service {
	return &Service{repo: repo, groups: groups, channels: channels}
}

// Create registers a new Client. Name uniqueness — including against a
// previously deleted Client — is enforced by the repository.
func (s *Service) Create(ctx context.Context, input in.CreateClientInput) (*client.Client, error) {
	r := realm.MustFromContext(ctx)
	c, err := client.New(r, input.Name, input.Labels)
	if err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, c); err != nil {
		return nil, err
	}
	return c, nil
}

// Get returns the client by name.
func (s *Service) Get(ctx context.Context, name string) (*client.Client, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Get(ctx, r.ID, name)
}

// List returns one page, ordered by name, filtered by the selector.
func (s *Service) List(ctx context.Context, input in.ListClientsInput) (in.ClientPage, error) {
	r := realm.MustFromContext(ctx)

	sel, err := selector.Parse(input.Selector)
	if err != nil {
		return in.ClientPage{}, err
	}
	queryKey := pagetoken.QueryKey("client", input.Selector)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ClientPage{}, err
	}

	page, err := s.repo.List(ctx, out.ClientQuery{
		RealmID:   r.ID,
		Selector:  sel,
		Limit:     pagetoken.ClampSize(input.PageSize),
		AfterName: after,
	})
	if err != nil {
		return in.ClientPage{}, err
	}
	return in.ClientPage{
		Clients:       page.Clients,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}

// Update applies the masked fields under a row lock (003.12). `name` is not
// itself maskable — it is immutable (003.10).
func (s *Service) Update(ctx context.Context, input in.UpdateClientInput) (*client.Client, error) {
	r := realm.MustFromContext(ctx)
	if input.Labels == nil {
		return nil, errs.InvalidField("update_mask", "must name at least one field")
	}
	return s.repo.Update(ctx, r.ID, input.Name, func(c *client.Client) error {
		c.SetLabels(*input.Labels)
		return nil
	})
}

// Delete removes the client and reserves its name/FRN.
func (s *Service) Delete(ctx context.Context, name string) error {
	r := realm.MustFromContext(ctx)
	return s.repo.Delete(ctx, r.ID, name)
}

// ListObservedConsumerGroups returns the current view — the newest sighting
// per (group, topic) — scoped to this client's FRN.
func (s *Service) ListObservedConsumerGroups(
	ctx context.Context, input in.ListObservedConsumerGroupsInput,
) (in.ObservedGroupPage, error) {
	r := realm.MustFromContext(ctx)
	c, err := s.repo.Get(ctx, r.ID, input.Name)
	if err != nil {
		return in.ObservedGroupPage{}, err
	}

	queryKey := pagetoken.QueryKey("observed-consumer-group", input.Name)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ObservedGroupPage{}, err
	}
	page, err := s.groups.ListCurrent(ctx, out.ObservedGroupQuery{
		RealmID:     r.ID,
		ClientFRN:   c.FRN.Path(),
		Limit:       pagetoken.ClampSize(input.PageSize),
		AfterCursor: after,
	})
	if err != nil {
		return in.ObservedGroupPage{}, err
	}
	return in.ObservedGroupPage{
		Groups:        page.Observations,
		NextPageToken: pagetoken.Encode(page.LastCursor, queryKey),
	}, nil
}

// ListConsumerGroupObservations returns this client's raw sightings, newest
// first, optionally bounded by [From, To).
func (s *Service) ListConsumerGroupObservations(
	ctx context.Context, input in.ListConsumerGroupObservationsInput,
) (in.ObservedGroupPage, error) {
	r := realm.MustFromContext(ctx)
	c, err := s.repo.Get(ctx, r.ID, input.Name)
	if err != nil {
		return in.ObservedGroupPage{}, err
	}

	queryKey := pagetoken.QueryKey("consumer-group-observation", input.Name)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ObservedGroupPage{}, err
	}
	page, err := s.groups.ListObservations(ctx, out.ObservationQuery{
		RealmID:     r.ID,
		ClientFRN:   c.FRN.Path(),
		From:        input.From,
		To:          input.To,
		Limit:       pagetoken.ClampSize(input.PageSize),
		AfterCursor: after,
	})
	if err != nil {
		return in.ObservedGroupPage{}, err
	}
	return in.ObservedGroupPage{
		Groups:        page.Observations,
		NextPageToken: pagetoken.Encode(page.LastCursor, queryKey),
	}, nil
}

// ListClientChannelAccess evaluates one page of Async Channels' access
// policies against this client (17.1–17.3, the reverse of
// AsyncChannelService.ListChannelClients) and returns only the ones granting
// it anything.
//
// Unlike the forward view, each channel carries its own policy, so a fresh
// accesspolicy.Evaluator is compiled per channel rather than once per call —
// 17's own "Selector-match cost" note accepts this (no special bound needed).
func (s *Service) ListClientChannelAccess(
	ctx context.Context, input in.ListClientChannelAccessInput,
) (in.ClientChannelAccessPage, error) {
	r := realm.MustFromContext(ctx)
	c, err := s.repo.Get(ctx, r.ID, input.Name)
	if err != nil {
		return in.ClientChannelAccessPage{}, err
	}

	queryKey := pagetoken.QueryKey("client-channel-access", input.Name)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ClientChannelAccessPage{}, err
	}
	page, err := s.channels.List(ctx, out.ChannelQuery{
		RealmID:   r.ID,
		Limit:     pagetoken.ClampSize(input.PageSize),
		AfterName: after,
	})
	if err != nil {
		return in.ClientChannelAccessPage{}, err
	}

	clientFRN := c.FRN.Path()
	access := make([]in.ClientChannelAccess, 0, len(page.Channels))
	for _, ch := range page.Channels {
		eval := accesspolicy.NewEvaluator(ch.AccessPolicy).Evaluate(clientFRN, c.Labels)
		if !eval.Granted() {
			continue
		}
		access = append(access, in.ClientChannelAccess{
			AsyncChannel: ch.Name, Effective: eval.Effective, MatchedBy: eval.MatchedBy,
		})
	}
	return in.ClientChannelAccessPage{
		Access:        access,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}
