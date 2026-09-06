package out

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
)

// AgentQuery parameterises AgentRepository.List. TypeFilter is empty for "any".
type AgentQuery struct {
	RealmID    uuid.UUID
	TypeFilter agent.Type
	Limit      int
	AfterName  string
}

// AgentPage is one page of a List result, ordered by name ascending.
type AgentPage struct {
	Agents   []*agent.Agent
	LastName string
}

// AgentRepository persists Agent registrations (003.12).
type AgentRepository interface {
	Create(ctx context.Context, a *agent.Agent) error
	Get(ctx context.Context, realmID uuid.UUID, name string) (*agent.Agent, error)
	List(ctx context.Context, q AgentQuery) (AgentPage, error)
	Mutate(ctx context.Context, realmID uuid.UUID, name string,
		mutate func(*agent.Agent) error) (*agent.Agent, error)

	// GetByTokenHash resolves a bearer-token hash to its agent across all realms
	// (the token itself carries no realm). errs.NotFound if no agent has that
	// hash. Used by the agent-auth interceptor (005.2).
	GetByTokenHash(ctx context.Context, tokenHash string) (*agent.Agent, error)
}
