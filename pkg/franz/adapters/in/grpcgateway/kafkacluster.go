package grpcgateway

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/fieldmask"
)

// kafkaClusterHandler adapts KafkaClusterService onto the generated gRPC server
// interface, mapping proto ⇄ domain and domain errors → gRPC status.
type kafkaClusterHandler struct {
	franzv1.UnimplementedKafkaClusterServiceServer
	svc   in.KafkaClusterService
	codec frn.Codec
}

// RegisterKafkaClusterService mounts the KafkaClusterService on both the gRPC
// server and the in-process REST gateway.
func RegisterKafkaClusterService(s *Server, svc in.KafkaClusterService, codec frn.Codec) error {
	h := &kafkaClusterHandler{svc: svc, codec: codec}
	franzv1.RegisterKafkaClusterServiceServer(s.grpc, h)
	return franzv1.RegisterKafkaClusterServiceHandlerServer(context.Background(), s.gw, h)
}

func (h *kafkaClusterHandler) CreateKafkaCluster(
	ctx context.Context, req *franzv1.CreateKafkaClusterRequest,
) (*franzv1.CreateKafkaClusterResponse, error) {
	c, err := h.svc.Create(ctx, in.CreateClusterInput{
		Name:              req.GetName(),
		ConnectionStrings: connsFromProto(req.GetConnectionStrings()),
		Labels:            req.GetLabels(),
		Configuration:     req.GetClusterConfiguration(),
		ProviderAgent:     req.GetClusterProviderAgent(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.CreateKafkaClusterResponse_builder{KafkaCluster: h.toProto(c)}.Build(), nil
}

func (h *kafkaClusterHandler) GetKafkaCluster(
	ctx context.Context, req *franzv1.GetKafkaClusterRequest,
) (*franzv1.GetKafkaClusterResponse, error) {
	c, err := h.svc.Get(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetKafkaClusterResponse_builder{KafkaCluster: h.toProto(c)}.Build(), nil
}

func (h *kafkaClusterHandler) ListKafkaClusters(
	ctx context.Context, req *franzv1.ListKafkaClustersRequest,
) (*franzv1.ListKafkaClustersResponse, error) {
	page, err := h.svc.List(ctx, in.ListClustersInput{
		Selector:  req.GetSelector(),
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.KafkaCluster, len(page.Clusters))
	for i, c := range page.Clusters {
		out[i] = h.toProto(c)
	}
	return franzv1.ListKafkaClustersResponse_builder{
		KafkaClusters: out,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(page.TotalSize),
		}.Build(),
	}.Build(), nil
}

func (h *kafkaClusterHandler) UpdateKafkaCluster(
	ctx context.Context, req *franzv1.UpdateKafkaClusterRequest,
) (*franzv1.UpdateKafkaClusterResponse, error) {
	paths, err := fieldmask.CanonicalPaths(req.GetUpdateMask(), req)
	if err != nil {
		return nil, ToError(err)
	}
	input := in.UpdateClusterInput{Name: req.GetName()}
	for _, p := range paths {
		switch p {
		case "connection_strings":
			v := connsFromProto(req.GetConnectionStrings())
			input.ConnectionStrings = &v
		case "labels":
			v := req.GetLabels()
			input.Labels = &v
		case "cluster_configuration":
			v := req.GetClusterConfiguration()
			input.Configuration = &v
		case "cluster_provider_agent":
			v := req.GetClusterProviderAgent()
			input.ProviderAgent = &v
		default:
			return nil, ToError(errs.InvalidField("update_mask", "field "+p+" is not updatable"))
		}
	}
	c, err := h.svc.Update(ctx, input)
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.UpdateKafkaClusterResponse_builder{KafkaCluster: h.toProto(c)}.Build(), nil
}

func (h *kafkaClusterHandler) DeleteKafkaCluster(
	ctx context.Context, req *franzv1.DeleteKafkaClusterRequest,
) (*franzv1.DeleteKafkaClusterResponse, error) {
	if err := h.svc.Delete(ctx, req.GetName()); err != nil {
		return nil, ToError(err)
	}
	return franzv1.DeleteKafkaClusterResponse_builder{}.Build(), nil
}

func (h *kafkaClusterHandler) PauseKafkaCluster(
	ctx context.Context, req *franzv1.PauseKafkaClusterRequest,
) (*franzv1.PauseKafkaClusterResponse, error) {
	c, err := h.svc.Pause(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.PauseKafkaClusterResponse_builder{KafkaCluster: h.toProto(c)}.Build(), nil
}

func (h *kafkaClusterHandler) ResumeKafkaCluster(
	ctx context.Context, req *franzv1.ResumeKafkaClusterRequest,
) (*franzv1.ResumeKafkaClusterResponse, error) {
	c, err := h.svc.Resume(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.ResumeKafkaClusterResponse_builder{KafkaCluster: h.toProto(c)}.Build(), nil
}

// --- mapping helpers -------------------------------------------------------

func (h *kafkaClusterHandler) toProto(c *cluster.Cluster) *franzv1.KafkaCluster {
	conns := make([]*franzv1.ConnectionString, len(c.ConnectionStrings))
	for i, cs := range c.ConnectionStrings {
		conns[i] = franzv1.ConnectionString_builder{
			BootstrapUrls: cs.BootstrapURLs,
			Type:          connTypeToProto(cs.Type),
		}.Build()
	}
	return franzv1.KafkaCluster_builder{
		Name:                 proto.String(c.Name),
		Frn:                  proto.String(h.codec.Render(c.FRN)),
		ConnectionStrings:    conns,
		Labels:               c.Labels,
		ClusterConfiguration: c.Configuration,
		ClusterProviderAgent: proto.String(c.ProviderAgent),
		State:                stateToProto(c.State),
		CreatedAt:            timestamppb.New(c.CreatedAt),
		UpdatedAt:            timestamppb.New(c.UpdatedAt),
	}.Build()
}

func connsFromProto(in []*franzv1.ConnectionString) []cluster.ConnectionString {
	out := make([]cluster.ConnectionString, len(in))
	for i, cs := range in {
		out[i] = cluster.ConnectionString{
			BootstrapURLs: cs.GetBootstrapUrls(),
			Type:          connTypeFromProto(cs.GetType()),
		}
	}
	return out
}

func connTypeFromProto(t franzv1.ConnectionType) cluster.ConnectionType {
	switch t {
	case franzv1.ConnectionType_CONNECTION_TYPE_PLAINTEXT:
		return cluster.ConnectionPlaintext
	case franzv1.ConnectionType_CONNECTION_TYPE_UNSPECIFIED:
		return "" // domain defaults this to PLAINTEXT
	default:
		return cluster.ConnectionType(t.String()) // domain rejects unknown types
	}
}

func connTypeToProto(t cluster.ConnectionType) *franzv1.ConnectionType {
	v := franzv1.ConnectionType_CONNECTION_TYPE_UNSPECIFIED
	if t == cluster.ConnectionPlaintext {
		v = franzv1.ConnectionType_CONNECTION_TYPE_PLAINTEXT
	}
	return &v
}

func stateToProto(s cluster.State) *franzv1.KafkaClusterState {
	v := franzv1.KafkaClusterState_KAFKA_CLUSTER_STATE_UNSPECIFIED
	switch s {
	case cluster.StateActive:
		v = franzv1.KafkaClusterState_KAFKA_CLUSTER_STATE_ACTIVE
	case cluster.StatePaused:
		v = franzv1.KafkaClusterState_KAFKA_CLUSTER_STATE_PAUSED
	case cluster.StateDeleted:
		v = franzv1.KafkaClusterState_KAFKA_CLUSTER_STATE_DELETED
	}
	return &v
}
