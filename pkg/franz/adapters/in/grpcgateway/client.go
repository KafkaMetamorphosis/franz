package grpcgateway

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/fieldmask"
)

// clientHandler adapts ClientService onto the generated gRPC server interface
// (003.10).
type clientHandler struct {
	franzv1.UnimplementedClientServiceServer
	svc   in.ClientService
	codec frn.Codec
}

// RegisterClientService mounts ClientService on the gRPC server and the
// in-process REST gateway.
func RegisterClientService(s *Server, svc in.ClientService, codec frn.Codec) error {
	h := &clientHandler{svc: svc, codec: codec}
	franzv1.RegisterClientServiceServer(s.grpc, h)
	return franzv1.RegisterClientServiceHandlerServer(context.Background(), s.gw, h)
}

func (h *clientHandler) CreateClient(
	ctx context.Context, req *franzv1.CreateClientRequest,
) (*franzv1.CreateClientResponse, error) {
	c, err := h.svc.Create(ctx, in.CreateClientInput{
		Name:   req.GetName(),
		Labels: req.GetLabels(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.CreateClientResponse_builder{Client: h.toProto(c)}.Build(), nil
}

func (h *clientHandler) GetClient(
	ctx context.Context, req *franzv1.GetClientRequest,
) (*franzv1.GetClientResponse, error) {
	c, err := h.svc.Get(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetClientResponse_builder{Client: h.toProto(c)}.Build(), nil
}

func (h *clientHandler) ListClients(
	ctx context.Context, req *franzv1.ListClientsRequest,
) (*franzv1.ListClientsResponse, error) {
	page, err := h.svc.List(ctx, in.ListClientsInput{
		Selector:  req.GetSelector(),
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.Client, len(page.Clients))
	for i, c := range page.Clients {
		out[i] = h.toProto(c)
	}
	return franzv1.ListClientsResponse_builder{
		Clients: out,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

func (h *clientHandler) UpdateClient(
	ctx context.Context, req *franzv1.UpdateClientRequest,
) (*franzv1.UpdateClientResponse, error) {
	paths, err := fieldmask.CanonicalPaths(req.GetUpdateMask(), req)
	if err != nil {
		return nil, ToError(err)
	}
	input := in.UpdateClientInput{Name: req.GetName()}
	for _, p := range paths {
		switch p {
		case "labels":
			v := req.GetLabels()
			input.Labels = &v
		case "name":
			return nil, ToError(errs.InvalidField("update_mask", "name is immutable"))
		default:
			return nil, ToError(errs.InvalidField("update_mask", "field "+p+" is not updatable"))
		}
	}
	c, err := h.svc.Update(ctx, input)
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.UpdateClientResponse_builder{Client: h.toProto(c)}.Build(), nil
}

func (h *clientHandler) DeleteClient(
	ctx context.Context, req *franzv1.DeleteClientRequest,
) (*franzv1.DeleteClientResponse, error) {
	if err := h.svc.Delete(ctx, req.GetName()); err != nil {
		return nil, ToError(err)
	}
	return franzv1.DeleteClientResponse_builder{}.Build(), nil
}

func (h *clientHandler) ListObservedConsumerGroups(
	ctx context.Context, req *franzv1.ListObservedConsumerGroupsRequest,
) (*franzv1.ListObservedConsumerGroupsResponse, error) {
	page, err := h.svc.ListObservedConsumerGroups(ctx, in.ListObservedConsumerGroupsInput{
		Name:      req.GetName(),
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.ListObservedConsumerGroupsResponse_builder{
		Groups: observationsToProto(page.Groups),
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

func (h *clientHandler) ListConsumerGroupObservations(
	ctx context.Context, req *franzv1.ListConsumerGroupObservationsRequest,
) (*franzv1.ListConsumerGroupObservationsResponse, error) {
	// An unset Timestamp stays the zero time.Time, which is exactly "no bound" to
	// the query layer (out.ObservationQuery checks From/To.IsZero()); AsTime() on
	// a nil Timestamp would instead yield the Unix epoch.
	var from, to time.Time
	if ts := req.GetFrom(); ts != nil {
		from = ts.AsTime()
	}
	if ts := req.GetTo(); ts != nil {
		to = ts.AsTime()
	}
	page, err := h.svc.ListConsumerGroupObservations(ctx, in.ListConsumerGroupObservationsInput{
		Name:      req.GetName(),
		From:      from,
		To:        to,
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.ListConsumerGroupObservationsResponse_builder{
		Observations: observationsToProto(page.Groups),
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

// ListClientChannelAccess is the reverse access view (003.5, 003.10,
// deliverable 17): every Async Channel this client is granted something on.
func (h *clientHandler) ListClientChannelAccess(
	ctx context.Context, req *franzv1.ListClientChannelAccessRequest,
) (*franzv1.ListClientChannelAccessResponse, error) {
	page, err := h.svc.ListClientChannelAccess(ctx, in.ListClientChannelAccessInput{
		Name:      req.GetName(),
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	access := make([]*franzv1.ClientChannelAccess, len(page.Access))
	for i, a := range page.Access {
		access[i] = franzv1.ClientChannelAccess_builder{
			AsyncChannel: proto.String(a.AsyncChannel),
			Effective:    permissionsToProto(a.Effective),
			MatchedBy:    proto.String(a.MatchedBy),
		}.Build()
	}
	return franzv1.ListClientChannelAccessResponse_builder{
		Access: access,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

// --- mapping helpers -----------------------------------------------------

func (h *clientHandler) toProto(c *client.Client) *franzv1.Client {
	return franzv1.Client_builder{
		Name:      proto.String(c.Name),
		Frn:       proto.String(h.codec.Render(c.FRN)),
		Labels:    c.Labels,
		CreatedAt: timestamppb.New(c.CreatedAt),
		UpdatedAt: timestamppb.New(c.UpdatedAt),
	}.Build()
}

// observationsToProto maps consumer-group observations onto the wire type
// shared by the current view and the history view (client.proto:
// "Current view: most recent sighting. History view: the sighting's time.") —
// both read straight from Observation.ObservedAt.
func observationsToProto(observations []*consumergroup.Observation) []*franzv1.ObservedConsumerGroup {
	out := make([]*franzv1.ObservedConsumerGroup, len(observations))
	for i, o := range observations {
		out[i] = franzv1.ObservedConsumerGroup_builder{
			Group:           proto.String(o.Group),
			AsyncChannel:    proto.String(o.AsyncChannel),
			KafkaTopic:      proto.String(o.KafkaTopic),
			Custom:          proto.Bool(o.Custom),
			ReportedByAgent: proto.String(o.ReportingAgent),
			LastSeenAt:      timestamppb.New(o.ObservedAt),
		}.Build()
	}
	return out
}
