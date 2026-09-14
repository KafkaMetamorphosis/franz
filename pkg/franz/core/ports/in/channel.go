package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
)

// CreateChannelInput is the client-settable state for a new Async Channel.
type CreateChannelInput struct {
	Name              string
	Type              channel.Type
	ChannelPartitions int32
	Labels            map[string]string
	AccessPolicy      accesspolicy.Policy
}

// UpdateChannelInput carries only the masked fields; a nil pointer means "leave
// unchanged". Only `labels` is maskable (003.4) — `channel_partitions`, `type`
// and `access_policy` are not.
type UpdateChannelInput struct {
	Name   string
	Labels *map[string]string
}

// ListChannelsInput parameterises List. Selector is the raw 003.1 selector
// string; PageToken is opaque.
type ListChannelsInput struct {
	Selector  string
	PageSize  int32
	PageToken string
}

// ChannelPage is a page of List results.
type ChannelPage struct {
	Channels      []*channel.AsyncChannel
	NextPageToken string
	TotalSize     int32
}

// ListChannelClientsInput parameterises the forward access view (003.5, 17.5):
// every Client this channel's access policy grants something to.
type ListChannelClientsInput struct {
	Name      string
	PageSize  int32
	PageToken string
}

// ChannelClientAccess is one resolved row of the forward view — a Client and
// what this channel's access policy grants it. Only rows with at least one
// effective permission are ever returned: 003.5 frames both views as "every
// client/channel whose policy matches", not an audit of every client in the
// realm regardless of outcome.
type ChannelClientAccess struct {
	ClientFRN    frn.FRN
	ClientLabels map[string]string
	Effective    []accesspolicy.Permission
	MatchedBy    string
}

// ChannelClientAccessPage is a page of ListChannelClients results.
type ChannelClientAccessPage struct {
	Access        []ChannelClientAccess
	NextPageToken string
}

// AsyncChannelService is the driving port for Async Channel management
// (003.4). The realm is taken from the request context.
type AsyncChannelService interface {
	Create(ctx context.Context, in CreateChannelInput) (*channel.AsyncChannel, error)
	Get(ctx context.Context, name string) (*channel.AsyncChannel, error)
	List(ctx context.Context, in ListChannelsInput) (ChannelPage, error)
	Update(ctx context.Context, in UpdateChannelInput) (*channel.AsyncChannel, error)
	Delete(ctx context.Context, name string) error
	Pause(ctx context.Context, name string) (*channel.AsyncChannel, error)
	Resume(ctx context.Context, name string) (*channel.AsyncChannel, error)
	// SetAccessPolicy replaces the embedded document wholesale.
	SetAccessPolicy(ctx context.Context, name string, p accesspolicy.Policy) (*channel.AsyncChannel, error)
	// ListChannelClients evaluates this channel's access policy against every
	// Client in the realm (17.1–17.3) and returns the ones with any grant.
	// errs.NotFound if the channel does not exist.
	ListChannelClients(ctx context.Context, in ListChannelClientsInput) (ChannelClientAccessPage, error)
}
