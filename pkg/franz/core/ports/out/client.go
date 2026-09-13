package out

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
)

// ClientQuery parameterises ClientRepository.List.
type ClientQuery struct {
	RealmID   uuid.UUID
	Selector  selector.Selector // empty ⇒ match all
	Limit     int               // page size (already clamped)
	AfterName string            // exclusive lower bound, "" ⇒ first page
}

// ClientPage is one page of a List result, ordered by name ascending.
type ClientPage struct {
	Clients  []*client.Client
	LastName string // name of the last row, "" ⇒ no more
}

// ClientRepository persists Client registrations (003.12). Unlike every other
// Franz entity, deletion is a real row removal, not a state flip — Client has
// no state column (003.10) — so the repository is also what makes
// "name/FRN never reused" durable.
type ClientRepository interface {
	// Create inserts a new row. A name/FRN collision — active or previously
	// deleted — is errs.AlreadyExists.
	Create(ctx context.Context, c *client.Client) error

	// Get returns the client by (realm, name). errs.NotFound if absent
	// (including a previously deleted one — the row is gone, not soft-deleted).
	Get(ctx context.Context, realmID uuid.UUID, name string) (*client.Client, error)

	// List returns one page per ClientQuery.
	List(ctx context.Context, q ClientQuery) (ClientPage, error)

	// Update loads the row FOR UPDATE, runs mutate, and persists the result in
	// one transaction.
	Update(ctx context.Context, realmID uuid.UUID, name string,
		mutate func(*client.Client) error) (*client.Client, error)

	// Delete removes the row and reserves its (realm, name) and frn so
	// Create can never reuse either, in one transaction. errs.NotFound if the
	// client does not exist.
	Delete(ctx context.Context, realmID uuid.UUID, name string) error
}
