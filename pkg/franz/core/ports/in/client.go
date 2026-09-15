package in

import (
	"context"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
)

// CreateClientInput is the client-settable state for a new Client (003.10:
// "CreateClient / UpdateClient carry only name and labels").
type CreateClientInput struct {
	Name   string
	Labels map[string]string
}

// UpdateClientInput carries only the masked fields; `name` selects the client
// and is never itself mutable (003.10).
type UpdateClientInput struct {
	Name   string
	Labels *map[string]string
}

// ListClientsInput parameterises List. Selector is the raw 003.1 selector
// string; PageToken is opaque.
type ListClientsInput struct {
	Selector  string
	PageSize  int32
	PageToken string
}

// ClientPage is a page of List results.
type ClientPage struct {
	Clients       []*client.Client
	NextPageToken string
	TotalSize     int32
}

// ListObservedConsumerGroupsInput parameterises the current-view read — one
// client's groups, newest sighting per (group, topic).
type ListObservedConsumerGroupsInput struct {
	Name      string
	PageSize  int32
	PageToken string
}

// ListConsumerGroupObservationsInput parameterises the raw-sightings read —
// one client's history within an optional time window.
type ListConsumerGroupObservationsInput struct {
	Name      string
	From      time.Time
	To        time.Time
	PageSize  int32
	PageToken string
}

// ObservedGroupPage is a page of either observed-consumer-group read.
type ObservedGroupPage struct {
	Groups        []*consumergroup.Observation
	NextPageToken string
}

// ListClientChannelAccessInput parameterises the reverse access view (003.5,
// 003.10, 17.6): every Async Channel this client's identity is granted
// something on.
type ListClientChannelAccessInput struct {
	Name      string
	PageSize  int32
	PageToken string
}

// ClientChannelAccess is one resolved row of the reverse view — an Async
// Channel and what its access policy grants this client. Only rows with at
// least one effective permission are ever returned (see ChannelClientAccess's
// doc — the same "matches" framing applies in both directions).
type ClientChannelAccess struct {
	AsyncChannel string
	Effective    []accesspolicy.Permission
	MatchedBy    string
}

// ClientChannelAccessPage is a page of ListClientChannelAccess results.
type ClientChannelAccessPage struct {
	Access        []ClientChannelAccess
	NextPageToken string
}

// ClientService is the driving port for the Client registry (003.10). The
// realm is taken from context, never from the input.
type ClientService interface {
	Create(ctx context.Context, in CreateClientInput) (*client.Client, error)
	Get(ctx context.Context, name string) (*client.Client, error)
	List(ctx context.Context, in ListClientsInput) (ClientPage, error)
	Update(ctx context.Context, in UpdateClientInput) (*client.Client, error)
	// Delete hard-deletes the row and reserves its name/FRN so neither is ever
	// reused (003.10 "DeleteClient does not free the name / FRN").
	Delete(ctx context.Context, name string) error

	// ListObservedConsumerGroups returns the current view for one client — the
	// newest sighting per (group, topic). errs.NotFound if the client does not
	// exist.
	ListObservedConsumerGroups(ctx context.Context, in ListObservedConsumerGroupsInput) (ObservedGroupPage, error)
	// ListConsumerGroupObservations returns one client's raw sightings, newest
	// first, optionally bounded by [From, To). errs.NotFound if the client does
	// not exist.
	ListConsumerGroupObservations(ctx context.Context, in ListConsumerGroupObservationsInput) (ObservedGroupPage, error)

	// ListClientChannelAccess evaluates every Async Channel's access policy
	// against this client (17.1–17.3, the reverse of ListChannelClients) and
	// returns the ones granting it anything. errs.NotFound if the client does
	// not exist.
	ListClientChannelAccess(ctx context.Context, in ListClientChannelAccessInput) (ClientChannelAccessPage, error)
}
