// Package channel is the Async Channel domain entity (003.4): the customer-facing
// boundary of one asynchronous communication path. It is abstract — it carries
// no Kafka configuration of its own. Franz shards it into `channel_partitions`
// Kafka Topics, which placement (deliverable 11) materialises (ADR-API-009).
package channel

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// Type is the channel substrate. Only KAFKA_TOPIC exists today (003.4); the enum
// is a forward-compat seam. Immutable once set.
type Type string

const TypeKafkaTopic Type = "KAFKA_TOPIC"

// Valid reports whether t is a known type.
func (t Type) Valid() bool { return t == TypeKafkaTopic }

// State is the channel lifecycle (003.4). No PENDING / ERROR — reconciliation
// progress lives on the shards (003.6).
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

// AsyncChannel is a registered channel.
type AsyncChannel struct {
	ID                uuid.UUID // surrogate key; assigned by the repository on Create
	FRN               frn.FRN
	RealmID           uuid.UUID
	Name              string
	Type              Type
	ChannelPartitions int32 // declared shard count (≥ 1); shards are created by placement
	Labels            map[string]string
	State             State
	AccessPolicy      accesspolicy.Policy
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// New builds a channel in state ACTIVE with an FRN assigned, validating the
// name, type, partition count, and the embedded access policy.
func New(
	r realm.Realm,
	name string,
	typ Type,
	channelPartitions int32,
	labels map[string]string,
	policy accesspolicy.Policy,
) (*AsyncChannel, error) {
	id, err := frn.New(r.Slug, frn.TypeAsyncChannel, name)
	if err != nil {
		return nil, err
	}
	if !typ.Valid() {
		return nil, errs.InvalidField("type", "must be KAFKA_TOPIC")
	}
	if channelPartitions < 1 {
		return nil, errs.InvalidField("channel_partitions", "must be >= 1")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &AsyncChannel{
		FRN:               id,
		RealmID:           r.ID,
		Name:              name,
		Type:              typ,
		ChannelPartitions: channelPartitions,
		Labels:            nonNil(labels),
		State:             StateActive,
		AccessPolicy:      policy,
	}, nil
}

// ShardName is the Kafka topic name of shard `index` (003.4): "<name>-<index>".
func (c *AsyncChannel) ShardName(index int) string {
	return fmt.Sprintf("%s-%d", c.Name, index)
}

// EnsureMutable rejects any operation on a soft-deleted channel (003.4).
func (c *AsyncChannel) EnsureMutable() error {
	if c.State == StateDeleted {
		return errs.Preconditionf("async channel %q is deleted", c.Name)
	}
	return nil
}

// SetLabels replaces the label map (the only field UpdateAsyncChannel may mask).
func (c *AsyncChannel) SetLabels(labels map[string]string) error {
	if err := c.EnsureMutable(); err != nil {
		return err
	}
	c.Labels = nonNil(labels)
	return nil
}

// SetAccessPolicy replaces the document wholesale (SetAccessPolicy RPC only).
func (c *AsyncChannel) SetAccessPolicy(policy accesspolicy.Policy) error {
	if err := c.EnsureMutable(); err != nil {
		return err
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	c.AccessPolicy = policy
	return nil
}

// Pause / Resume / Delete move the state (003.4 machine). Idempotent where the
// spec allows; rejected on DELETED.
func (c *AsyncChannel) Pause() error {
	if err := c.EnsureMutable(); err != nil {
		return err
	}
	c.State = StatePaused
	return nil
}

func (c *AsyncChannel) Resume() error {
	if err := c.EnsureMutable(); err != nil {
		return err
	}
	c.State = StateActive
	return nil
}

func (c *AsyncChannel) Delete() error {
	if err := c.EnsureMutable(); err != nil {
		return err
	}
	c.State = StateDeleted
	return nil
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
