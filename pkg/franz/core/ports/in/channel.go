package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
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
}
