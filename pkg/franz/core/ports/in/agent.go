package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
)

// CreateAgentInput is the client-settable state for a new agent.
type CreateAgentInput struct {
	Name               string
	Type               agent.Type
	Labels             map[string]string
	ProvisioningLabels []agent.ProvisioningLabelSpec
}

// UpdateAgentInput carries only the masked fields; a nil pointer means
// "leave unchanged". `name` selects the agent.
type UpdateAgentInput struct {
	Name               string
	Type               *agent.Type
	Labels             *map[string]string
	ProvisioningLabels *[]agent.ProvisioningLabelSpec
}

// ListAgentsInput parameterises List. TypeFilter is empty for "any".
type ListAgentsInput struct {
	TypeFilter agent.Type
	PageSize   int32
	PageToken  string
}

// AgentPage is a page of List results.
type AgentPage struct {
	Agents        []*agent.Agent
	NextPageToken string
	TotalSize     int32
}

// CreatedAgent bundles the new agent with its one-time bearer token.
type CreatedAgent struct {
	Agent *agent.Agent
	Token string
}

// AgentService is the driving port for the Agent registry (003.9). The realm is
// taken from context.
type AgentService interface {
	Create(ctx context.Context, in CreateAgentInput) (CreatedAgent, error)
	Get(ctx context.Context, name string) (*agent.Agent, error)
	List(ctx context.Context, in ListAgentsInput) (AgentPage, error)
	Update(ctx context.Context, in UpdateAgentInput) (*agent.Agent, error)
	Delete(ctx context.Context, name string) error
	Pause(ctx context.Context, name string) (*agent.Agent, error)
	Resume(ctx context.Context, name string) (*agent.Agent, error)
	// RotateToken issues a new bearer token and invalidates the old one.
	RotateToken(ctx context.Context, name string) (string, error)
}
