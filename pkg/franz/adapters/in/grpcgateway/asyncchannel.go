package grpcgateway

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/fieldmask"
)

// asyncChannelHandler adapts AsyncChannelService onto the generated gRPC server.
type asyncChannelHandler struct {
	franzv1.UnimplementedAsyncChannelServiceServer
	svc   in.AsyncChannelService
	codec frn.Codec
}

// RegisterAsyncChannelService mounts the AsyncChannelService on the gRPC server
// and the in-process REST gateway.
func RegisterAsyncChannelService(s *Server, svc in.AsyncChannelService, codec frn.Codec) error {
	h := &asyncChannelHandler{svc: svc, codec: codec}
	franzv1.RegisterAsyncChannelServiceServer(s.grpc, h)
	return franzv1.RegisterAsyncChannelServiceHandlerServer(context.Background(), s.gw, h)
}

func (h *asyncChannelHandler) CreateAsyncChannel(
	ctx context.Context, req *franzv1.CreateAsyncChannelRequest,
) (*franzv1.CreateAsyncChannelResponse, error) {
	c, err := h.svc.Create(ctx, in.CreateChannelInput{
		Name:              req.GetName(),
		Type:              channelTypeFromProto(req.GetType()),
		ChannelPartitions: req.GetChannelPartitions(),
		Labels:            req.GetLabels(),
		AccessPolicy:      policyFromProto(req.GetAccessPolicy()),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.CreateAsyncChannelResponse_builder{AsyncChannel: h.toProto(c)}.Build(), nil
}

func (h *asyncChannelHandler) GetAsyncChannel(
	ctx context.Context, req *franzv1.GetAsyncChannelRequest,
) (*franzv1.GetAsyncChannelResponse, error) {
	c, err := h.svc.Get(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetAsyncChannelResponse_builder{AsyncChannel: h.toProto(c)}.Build(), nil
}

func (h *asyncChannelHandler) ListAsyncChannels(
	ctx context.Context, req *franzv1.ListAsyncChannelsRequest,
) (*franzv1.ListAsyncChannelsResponse, error) {
	page, err := h.svc.List(ctx, in.ListChannelsInput{
		Selector:  req.GetSelector(),
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.AsyncChannel, len(page.Channels))
	for i, c := range page.Channels {
		out[i] = h.toProto(c)
	}
	return franzv1.ListAsyncChannelsResponse_builder{
		AsyncChannels: out,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

func (h *asyncChannelHandler) UpdateAsyncChannel(
	ctx context.Context, req *franzv1.UpdateAsyncChannelRequest,
) (*franzv1.UpdateAsyncChannelResponse, error) {
	paths, err := fieldmask.CanonicalPaths(req.GetUpdateMask(), req)
	if err != nil {
		return nil, ToError(err)
	}
	input := in.UpdateChannelInput{Name: req.GetName()}
	for _, p := range paths {
		switch p {
		case "labels":
			v := req.GetLabels()
			input.Labels = &v
		default:
			return nil, ToError(errs.InvalidField("update_mask",
				"field "+p+" is not updatable (channel_partitions is a re-shard, access_policy uses SetAccessPolicy)"))
		}
	}
	c, err := h.svc.Update(ctx, input)
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.UpdateAsyncChannelResponse_builder{AsyncChannel: h.toProto(c)}.Build(), nil
}

func (h *asyncChannelHandler) DeleteAsyncChannel(
	ctx context.Context, req *franzv1.DeleteAsyncChannelRequest,
) (*franzv1.DeleteAsyncChannelResponse, error) {
	if err := h.svc.Delete(ctx, req.GetName()); err != nil {
		return nil, ToError(err)
	}
	return franzv1.DeleteAsyncChannelResponse_builder{}.Build(), nil
}

func (h *asyncChannelHandler) PauseAsyncChannel(
	ctx context.Context, req *franzv1.PauseAsyncChannelRequest,
) (*franzv1.PauseAsyncChannelResponse, error) {
	c, err := h.svc.Pause(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.PauseAsyncChannelResponse_builder{AsyncChannel: h.toProto(c)}.Build(), nil
}

func (h *asyncChannelHandler) ResumeAsyncChannel(
	ctx context.Context, req *franzv1.ResumeAsyncChannelRequest,
) (*franzv1.ResumeAsyncChannelResponse, error) {
	c, err := h.svc.Resume(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.ResumeAsyncChannelResponse_builder{AsyncChannel: h.toProto(c)}.Build(), nil
}

func (h *asyncChannelHandler) SetAccessPolicy(
	ctx context.Context, req *franzv1.SetAccessPolicyRequest,
) (*franzv1.SetAccessPolicyResponse, error) {
	c, err := h.svc.SetAccessPolicy(ctx, req.GetName(), policyFromProto(req.GetAccessPolicy()))
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.SetAccessPolicyResponse_builder{AsyncChannel: h.toProto(c)}.Build(), nil
}

// ListChannelClients is the forward access view — implemented in deliverable 15
// (Access-policy engine & channel-access views), which needs Client to exist.
func (h *asyncChannelHandler) ListChannelClients(
	context.Context, *franzv1.ListChannelClientsRequest,
) (*franzv1.ListChannelClientsResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"ListChannelClients ships with deliverable 15 (needs Client)")
}

// --- mapping helpers -----------------------------------------------------

func (h *asyncChannelHandler) toProto(c *channel.AsyncChannel) *franzv1.AsyncChannel {
	return franzv1.AsyncChannel_builder{
		Name:              proto.String(c.Name),
		Frn:               proto.String(h.codec.Render(c.FRN)),
		Type:              channelTypeToProto(c.Type),
		ChannelPartitions: proto.Int32(c.ChannelPartitions),
		Labels:            c.Labels,
		State:             channelStateToProto(c.State),
		AccessPolicy:      policyToProto(c.AccessPolicy),
		CreatedAt:         timestamppb.New(c.CreatedAt),
		UpdatedAt:         timestamppb.New(c.UpdatedAt),
	}.Build()
}

func channelTypeFromProto(t franzv1.ChannelType) channel.Type {
	if t == franzv1.ChannelType_CHANNEL_TYPE_KAFKA_TOPIC {
		return channel.TypeKafkaTopic
	}
	return "" // domain rejects UNSPECIFIED
}

func channelTypeToProto(t channel.Type) *franzv1.ChannelType {
	v := franzv1.ChannelType_CHANNEL_TYPE_UNSPECIFIED
	if t == channel.TypeKafkaTopic {
		v = franzv1.ChannelType_CHANNEL_TYPE_KAFKA_TOPIC
	}
	return &v
}

func channelStateToProto(s channel.State) *franzv1.ChannelState {
	v := franzv1.ChannelState_CHANNEL_STATE_UNSPECIFIED
	switch s {
	case channel.StateActive:
		v = franzv1.ChannelState_CHANNEL_STATE_ACTIVE
	case channel.StatePaused:
		v = franzv1.ChannelState_CHANNEL_STATE_PAUSED
	case channel.StateDeleted:
		v = franzv1.ChannelState_CHANNEL_STATE_DELETED
	}
	return &v
}

func policyFromProto(p *franzv1.AccessPolicy) accesspolicy.Policy {
	if p == nil {
		return accesspolicy.Policy{}
	}
	out := accesspolicy.Policy{Statements: make([]accesspolicy.Statement, 0, len(p.GetStatements()))}
	for _, s := range p.GetStatements() {
		perms := make([]accesspolicy.Permission, 0, len(s.GetPermissions()))
		for _, perm := range s.GetPermissions() {
			perms = append(perms, permissionFromProto(perm))
		}
		out.Statements = append(out.Statements, accesspolicy.Statement{
			Effect: effectFromProto(s.GetEffect()),
			Principal: accesspolicy.Principal{
				ClientFRN:     s.GetPrincipal().GetClientFrn(),
				LabelSelector: s.GetPrincipal().GetLabels(),
			},
			Permissions: perms,
		})
	}
	return out
}

func policyToProto(p accesspolicy.Policy) *franzv1.AccessPolicy {
	statements := make([]*franzv1.AccessPolicyStatement, 0, len(p.Statements))
	for _, s := range p.Statements {
		perms := make([]franzv1.Permission, 0, len(s.Permissions))
		for _, perm := range s.Permissions {
			perms = append(perms, permissionToProto(perm))
		}
		statements = append(statements, franzv1.AccessPolicyStatement_builder{
			Effect: effectToProto(s.Effect),
			Principal: franzv1.Principal_builder{
				ClientFrn: proto.String(s.Principal.ClientFRN),
				Labels:    proto.String(s.Principal.LabelSelector),
			}.Build(),
			Permissions: perms,
		}.Build())
	}
	return franzv1.AccessPolicy_builder{Statements: statements}.Build()
}

func effectFromProto(e franzv1.Effect) accesspolicy.Effect {
	switch e {
	case franzv1.Effect_EFFECT_ALLOW:
		return accesspolicy.Allow
	case franzv1.Effect_EFFECT_DENY:
		return accesspolicy.Deny
	default:
		return "" // domain rejects
	}
}

func effectToProto(e accesspolicy.Effect) *franzv1.Effect {
	v := franzv1.Effect_EFFECT_UNSPECIFIED
	switch e {
	case accesspolicy.Allow:
		v = franzv1.Effect_EFFECT_ALLOW
	case accesspolicy.Deny:
		v = franzv1.Effect_EFFECT_DENY
	}
	return &v
}

func permissionFromProto(p franzv1.Permission) accesspolicy.Permission {
	switch p {
	case franzv1.Permission_PERMISSION_READ:
		return accesspolicy.Read
	case franzv1.Permission_PERMISSION_WRITE:
		return accesspolicy.Write
	default:
		return "" // domain rejects
	}
}

func permissionToProto(p accesspolicy.Permission) franzv1.Permission {
	switch p {
	case accesspolicy.Read:
		return franzv1.Permission_PERMISSION_READ
	case accesspolicy.Write:
		return franzv1.Permission_PERMISSION_WRITE
	default:
		return franzv1.Permission_PERMISSION_UNSPECIFIED
	}
}
