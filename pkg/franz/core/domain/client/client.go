// Package client is the Client domain entity (003.10): the fleet-wide SDK
// identity a service authenticates as when it reads from or writes to Async
// Channels. A Client carries no permission of its own — the channel access
// policy (003.5) is the sole authority for Read/Write — so this package has no
// state machine, unlike every other Franz entity.
package client

import (
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// Client is a registered SDK identity. There is deliberately no Type, Role, or
// Status field (003.10 "Key fields"): a Client's only mutable state is Labels.
type Client struct {
	ID      uuid.UUID
	FRN     frn.FRN
	RealmID uuid.UUID
	// Name is realm-wide unique and immutable once set (003.10). It also doubles
	// as the default consumer-group prefix (`<name>.<topic>`).
	Name string
	// Labels is free-form metadata; channel access policies (003.5) match a
	// Principal against it. It *should* carry org.com/owner, but 003.10 OQ1
	// leaves enforcement open — not required here.
	Labels    map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// New builds a Client. Name uniqueness (including against a previously deleted
// Client — 003.10 "DeleteClient does not free the name") is enforced by the
// repository, not here.
func New(r realm.Realm, name string, labels map[string]string) (*Client, error) {
	id, err := frn.New(r.Slug, frn.TypeClient, name)
	if err != nil {
		return nil, err
	}
	return &Client{
		FRN:     id,
		RealmID: r.ID,
		Name:    name,
		Labels:  nonNil(labels),
	}, nil
}

// SetLabels replaces Labels wholesale — 003.10 "CreateClient / UpdateClient
// carry only name and labels", and name is immutable, so labels is the only
// mutable field there is.
func (c *Client) SetLabels(labels map[string]string) {
	c.Labels = nonNil(labels)
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
