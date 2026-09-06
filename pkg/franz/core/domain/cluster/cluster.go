// Package cluster is the Kafka Cluster domain entity (003.3): a registration
// recording where a real Kafka cluster lives and the fleet context it sits in.
// Franz never connects to it.
package cluster

import (
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/naming"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// State is the persisted lifecycle state (003.3 "State transitions").
type State string

const (
	StateActive  State = "ACTIVE"
	StatePaused  State = "PAUSED"
	StateDeleted State = "DELETED"
)

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	switch s {
	case StateActive, StatePaused, StateDeleted:
		return true
	default:
		return false
	}
}

// ConnectionType is the auth flavour of a ConnectionString. Only PLAINTEXT
// exists today (003.3).
type ConnectionType string

const ConnectionPlaintext ConnectionType = "PLAINTEXT"

// Valid reports whether t is a known connection type.
func (t ConnectionType) Valid() bool { return t == ConnectionPlaintext }

// ConnectionString is one way to reach a cluster's brokers.
type ConnectionString struct {
	BootstrapURLs []string
	Type          ConnectionType
}

// Cluster is a Kafka Cluster registration.
type Cluster struct {
	ID                uuid.UUID // surrogate key; assigned by the repository on Create
	FRN               frn.FRN
	RealmID           uuid.UUID
	Name              string
	ConnectionStrings []ConnectionString
	Labels            map[string]string
	Configuration     map[string]string // cluster_configuration
	ProviderAgent     string            // unvalidated free string
	State             State
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// New builds a Cluster in state ACTIVE, validating the name and connection
// strings. realm supplies the FRN realm segment and the realm_id.
func New(
	r realm.Realm,
	name string,
	conns []ConnectionString,
	labels, configuration map[string]string,
	providerAgent string,
) (*Cluster, error) {
	id, err := frn.New(r.Slug, frn.TypeKafkaCluster, name)
	if err != nil {
		return nil, err
	}
	if err := validateConns(conns); err != nil {
		return nil, err
	}
	return &Cluster{
		FRN:               id,
		RealmID:           r.ID,
		Name:              name,
		ConnectionStrings: conns,
		Labels:            nonNil(labels),
		Configuration:     nonNil(configuration),
		ProviderAgent:     providerAgent,
		State:             StateActive,
	}, nil
}

// SetConnectionStrings replaces the connection strings (Update path).
func (c *Cluster) SetConnectionStrings(conns []ConnectionString) error {
	if err := validateConns(conns); err != nil {
		return err
	}
	c.ConnectionStrings = conns
	return nil
}

// Pause moves ACTIVE → PAUSED. It is idempotent on PAUSED and rejected on
// DELETED (003.3: any operation on a deleted cluster fails).
func (c *Cluster) Pause() error {
	if c.State == StateDeleted {
		return deletedErr(c.Name)
	}
	c.State = StatePaused
	return nil
}

// Resume moves PAUSED → ACTIVE. Idempotent on ACTIVE, rejected on DELETED.
func (c *Cluster) Resume() error {
	if c.State == StateDeleted {
		return deletedErr(c.Name)
	}
	c.State = StateActive
	return nil
}

// Delete soft-deletes the cluster. Rejected if already DELETED.
func (c *Cluster) Delete() error {
	if c.State == StateDeleted {
		return deletedErr(c.Name)
	}
	c.State = StateDeleted
	return nil
}

// EnsureMutable returns FAILED_PRECONDITION if the cluster is soft-deleted;
// callers guard Update/Get-then-mutate paths with it.
func (c *Cluster) EnsureMutable() error {
	if c.State == StateDeleted {
		return deletedErr(c.Name)
	}
	return nil
}

func deletedErr(name string) error {
	return errs.Preconditionf("kafka cluster %q is deleted", name)
}

func validateConns(conns []ConnectionString) error {
	if len(conns) == 0 {
		return errs.InvalidField("connection_strings", "must not be empty")
	}
	for i, cs := range conns {
		if len(cs.BootstrapURLs) == 0 {
			return errs.InvalidField("connection_strings",
				"entry has no bootstrap_urls")
		}
		for _, u := range cs.BootstrapURLs {
			if u == "" {
				return errs.InvalidField("connection_strings", "empty bootstrap url")
			}
		}
		if cs.Type == "" {
			conns[i].Type = ConnectionPlaintext
			continue
		}
		if !cs.Type.Valid() {
			return errs.InvalidField("connection_strings",
				"unsupported connection type "+string(cs.Type))
		}
	}
	return nil
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// ValidateName is exposed so the handler can reject a bad name before touching
// the store.
func ValidateName(name string) error { return naming.Validate(name) }
