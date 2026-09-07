// Package agents is the Agent registry application service (003.9). Registration
// mints a one-time bearer token; Franz keeps only its hash.
package agents

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/token"
)

// Service implements in.AgentService.
type Service struct {
	repo out.AgentRepository
}

var _ in.AgentService = (*Service)(nil)

// NewService wires the service to its repository.
func NewService(repo out.AgentRepository) *Service { return &Service{repo: repo} }

// Create registers an agent and returns its one-time bearer token.
func (s *Service) Create(ctx context.Context, input in.CreateAgentInput) (in.CreatedAgent, error) {
	r := realm.MustFromContext(ctx)

	plaintext, hash, err := token.Generate()
	if err != nil {
		return in.CreatedAgent{}, errs.Internalf("mint agent token").Wrap(err)
	}
	a, err := agent.New(r, input.Name, input.Type, input.Labels, hash)
	if err != nil {
		return in.CreatedAgent{}, err
	}
	if err := s.repo.Create(ctx, a); err != nil {
		return in.CreatedAgent{}, err
	}
	return in.CreatedAgent{Agent: a, Token: plaintext}, nil
}

// Get returns the agent by name, including a soft-deleted one.
func (s *Service) Get(ctx context.Context, name string) (*agent.Agent, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Get(ctx, r.ID, name)
}

// List returns one page, ordered by name, optionally filtered by type.
func (s *Service) List(ctx context.Context, input in.ListAgentsInput) (in.AgentPage, error) {
	r := realm.MustFromContext(ctx)

	queryKey := pagetoken.QueryKey("agent", string(input.TypeFilter))
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.AgentPage{}, err
	}
	page, err := s.repo.List(ctx, out.AgentQuery{
		RealmID:    r.ID,
		TypeFilter: input.TypeFilter,
		Limit:      pagetoken.ClampSize(input.PageSize),
		AfterName:  after,
	})
	if err != nil {
		return in.AgentPage{}, err
	}
	return in.AgentPage{
		Agents:        page.Agents,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}

// Update applies the masked fields under a row lock.
func (s *Service) Update(ctx context.Context, input in.UpdateAgentInput) (*agent.Agent, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Mutate(ctx, r.ID, input.Name, func(a *agent.Agent) error {
		if err := a.EnsureMutable(); err != nil {
			return err
		}
		if input.Type != nil {
			if err := a.SetType(*input.Type); err != nil {
				return err
			}
		}
		if input.Labels != nil {
			a.Labels = *input.Labels
		}
		return nil
	})
}

// Delete soft-deletes the agent. A cluster that still names this agent in
// cluster_provider_agent keeps its dangling string (003.3 / 003.9).
func (s *Service) Delete(ctx context.Context, name string) error {
	r := realm.MustFromContext(ctx)
	_, err := s.repo.Mutate(ctx, r.ID, name, func(a *agent.Agent) error { return a.Delete() })
	return err
}

// Pause moves the agent to PAUSED (idempotent).
func (s *Service) Pause(ctx context.Context, name string) (*agent.Agent, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Mutate(ctx, r.ID, name, func(a *agent.Agent) error { return a.Pause() })
}

// Resume moves the agent to ACTIVE (idempotent).
func (s *Service) Resume(ctx context.Context, name string) (*agent.Agent, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Mutate(ctx, r.ID, name, func(a *agent.Agent) error { return a.Resume() })
}

// RotateToken mints a new bearer token, stores its hash, and returns the
// plaintext once.
func (s *Service) RotateToken(ctx context.Context, name string) (string, error) {
	r := realm.MustFromContext(ctx)
	plaintext, hash, err := token.Generate()
	if err != nil {
		return "", errs.Internalf("mint agent token").Wrap(err)
	}
	_, err = s.repo.Mutate(ctx, r.ID, name, func(a *agent.Agent) error {
		if err := a.EnsureMutable(); err != nil {
			return err
		}
		a.RotateToken(hash)
		return nil
	})
	if err != nil {
		return "", err
	}
	return plaintext, nil
}
