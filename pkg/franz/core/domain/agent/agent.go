// Package agent is the Agent registry domain entity (003.9): Franz's record of
// an external program that connects to Franz to do fleet work. Registration is
// inert — it never starts a connection or schedules work.
package agent

import (
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// Type is the organisational filter on an agent (003.9). It changes nothing
// about how the agent connects or what it may do.
type Type string

const (
	TypeClusterProvider  Type = "CLUSTER_PROVIDER"
	TypeResourceProvider Type = "RESOURCE_PROVIDER"
	TypeTelemetryAgent   Type = "TELEMETRY_AGENT"
	TypeCustom           Type = "CUSTOM"
)

// Valid reports whether t is a known, non-empty type.
func (t Type) Valid() bool {
	switch t {
	case TypeClusterProvider, TypeResourceProvider, TypeTelemetryAgent, TypeCustom:
		return true
	default:
		return false
	}
}

// Status is the lifecycle state (003.9), mirroring Kafka Cluster.
type Status string

const (
	StatusActive  Status = "ACTIVE"
	StatusPaused  Status = "PAUSED"
	StatusDeleted Status = "DELETED"
)

// Agent is a registered agent.
type Agent struct {
	ID      uuid.UUID
	FRN     frn.FRN
	RealmID uuid.UUID
	Name    string
	Type    Type
	Labels  map[string]string
	// ProvisioningLabels is the agent's advisory franz.provisioning/* schema
	// (003.9, ADR-API-008). Nil/empty when the agent advertises none.
	ProvisioningLabels []ProvisioningLabelSpec
	Status             Status
	TokenHash          string // sha256 of the bearer token; never rendered to the API
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// New builds an Agent in status ACTIVE. tokenHash is the digest of the one-time
// bearer token minted by the caller.
func New(r realm.Realm, name string, typ Type, labels map[string]string, provisioningLabels []ProvisioningLabelSpec, tokenHash string) (*Agent, error) {
	id, err := frn.New(r.Slug, frn.TypeAgent, name)
	if err != nil {
		return nil, err
	}
	if !typ.Valid() {
		return nil, errs.InvalidField("type", "must be one of CLUSTER_PROVIDER, RESOURCE_PROVIDER, TELEMETRY_AGENT, CUSTOM")
	}
	if err := ValidateProvisioningLabels(provisioningLabels); err != nil {
		return nil, err
	}
	return &Agent{
		FRN:                id,
		RealmID:            r.ID,
		Name:               name,
		Type:               typ,
		Labels:             nonNil(labels),
		ProvisioningLabels: provisioningLabels,
		Status:             StatusActive,
		TokenHash:          tokenHash,
	}, nil
}

// SetProvisioningLabels replaces the advisory provisioning-label schema
// wholesale (003.9). Validates well-formedness only.
func (a *Agent) SetProvisioningLabels(specs []ProvisioningLabelSpec) error {
	if err := ValidateProvisioningLabels(specs); err != nil {
		return err
	}
	a.ProvisioningLabels = specs
	return nil
}

// SetType changes the organisational type. 003.9 OQ2 leaves type mutability
// open; deliverable 04 treats it as mutable.
func (a *Agent) SetType(typ Type) error {
	if !typ.Valid() {
		return errs.InvalidField("type", "unknown agent type "+string(typ))
	}
	a.Type = typ
	return nil
}

// RotateToken swaps in a new token hash.
func (a *Agent) RotateToken(tokenHash string) { a.TokenHash = tokenHash }

// Pause moves the agent to PAUSED (idempotent; rejected on DELETED).
func (a *Agent) Pause() error {
	if a.Status == StatusDeleted {
		return deletedErr(a.Name)
	}
	a.Status = StatusPaused
	return nil
}

// Resume moves the agent to ACTIVE (idempotent; rejected on DELETED).
func (a *Agent) Resume() error {
	if a.Status == StatusDeleted {
		return deletedErr(a.Name)
	}
	a.Status = StatusActive
	return nil
}

// Delete soft-deletes the agent. Rejected if already DELETED.
func (a *Agent) Delete() error {
	if a.Status == StatusDeleted {
		return deletedErr(a.Name)
	}
	a.Status = StatusDeleted
	return nil
}

// EnsureMutable returns FAILED_PRECONDITION if the agent is soft-deleted.
func (a *Agent) EnsureMutable() error {
	if a.Status == StatusDeleted {
		return deletedErr(a.Name)
	}
	return nil
}

func deletedErr(name string) error {
	return errs.Preconditionf("agent %q is deleted", name)
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
