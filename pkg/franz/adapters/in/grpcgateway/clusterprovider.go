package grpcgateway

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// clusterProviderHandler implements the agent-facing ClusterProviderService
// (004 ADR). gRPC only — no REST gateway.
type clusterProviderHandler struct {
	franzv1.UnimplementedClusterProviderServiceServer
	svc   in.ClusterProviderService
	hub   *streamhub.Hub
	codec frn.Codec
}

// RegisterClusterProviderService mounts the service on the gRPC server only.
func RegisterClusterProviderService(s *Server, svc in.ClusterProviderService, hub *streamhub.Hub, codec frn.Codec) {
	franzv1.RegisterClusterProviderServiceServer(s.grpc, &clusterProviderHandler{svc: svc, hub: hub, codec: codec})
}

// WatchClusterAssignments streams the agent its assignments: the full current
// set on open (all CHANGE_SET / PAUSED / REMOVED), then a message per change.
func (h *clusterProviderHandler) WatchClusterAssignments(
	_ *franzv1.WatchClusterAssignmentsRequest,
	stream grpc.ServerStreamingServer[franzv1.WatchClusterAssignmentsResponse],
) error {
	ctx := stream.Context()
	ag := agent.MustFromContext(ctx)

	// Subscribe before reading the initial set so a change in between is buffered.
	deltas, unsubscribe := h.hub.Subscribe(ag.Name)
	defer unsubscribe()

	initial, err := h.svc.InitialAssignments(ctx)
	if err != nil {
		return ToError(err)
	}
	for _, a := range initial {
		if err := stream.Send(h.assignmentResponse(a)); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a, ok := <-deltas:
			if !ok {
				return status.Error(codes.Aborted, "assignment stream lagged; reconnect and re-sync")
			}
			if err := stream.Send(h.assignmentResponse(a)); err != nil {
				return err
			}
		}
	}
}

// ReportClusterStatus records one status report from the agent.
func (h *clusterProviderHandler) ReportClusterStatus(
	ctx context.Context, req *franzv1.ReportClusterStatusRequest,
) (*franzv1.ReportClusterStatusResponse, error) {
	err := h.svc.ReportStatus(ctx, in.ReportStatusInput{
		ClusterName: req.GetClusterName(),
		Phase:       phaseFromProto(req.GetPhase()),
		Reachable:   req.GetReachable(),
		Message:     req.GetMessage(),
		RecipeRef:   req.GetRecipeRef(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.ReportClusterStatusResponse_builder{}.Build(), nil
}

// --- mapping ------------------------------------------------------------

func (h *clusterProviderHandler) assignmentResponse(a provider.Assignment) *franzv1.WatchClusterAssignmentsResponse {
	conns := make([]*franzv1.ConnectionString, len(a.ConnectionStrings))
	for i, cs := range a.ConnectionStrings {
		ct := franzv1.ConnectionType_CONNECTION_TYPE_UNSPECIFIED
		if cs.Type == "PLAINTEXT" {
			ct = franzv1.ConnectionType_CONNECTION_TYPE_PLAINTEXT
		}
		conns[i] = franzv1.ConnectionString_builder{BootstrapUrls: cs.BootstrapURLs, Type: &ct}.Build()
	}
	return franzv1.WatchClusterAssignmentsResponse_builder{
		Assignment: franzv1.ClusterAssignment_builder{
			Change:               changeToProto(a.Change),
			ClusterName:          proto.String(a.ClusterName),
			ClusterFrn:           proto.String(h.codec.Render(a.ClusterFRN)),
			ConnectionStrings:    conns,
			ClusterConfiguration: a.Configuration,
			Provisioning:         a.Provisioning,
		}.Build(),
	}.Build()
}

func changeToProto(c provider.Change) *franzv1.ClusterAssignment_Change {
	v := franzv1.ClusterAssignment_CHANGE_UNSPECIFIED
	switch c {
	case provider.ChangeSet:
		v = franzv1.ClusterAssignment_CHANGE_SET
	case provider.ChangePaused:
		v = franzv1.ClusterAssignment_CHANGE_PAUSED
	case provider.ChangeRemoved:
		v = franzv1.ClusterAssignment_CHANGE_REMOVED
	}
	return &v
}

func phaseFromProto(p franzv1.ClusterProviderPhase) provider.Phase {
	switch p {
	case franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_PROVISIONING:
		return provider.PhaseProvisioning
	case franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_READY:
		return provider.PhaseReady
	case franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_DEGRADED:
		return provider.PhaseDegraded
	case franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_ERROR:
		return provider.PhaseError
	case franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_STOPPED:
		return provider.PhaseStopped
	case franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_REMOVED:
		return provider.PhaseRemoved
	default:
		return "" // domain rejects
	}
}

func phaseToProto(p provider.Phase) *franzv1.ClusterProviderPhase {
	v := franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_UNSPECIFIED
	switch p {
	case provider.PhaseProvisioning:
		v = franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_PROVISIONING
	case provider.PhaseReady:
		v = franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_READY
	case provider.PhaseDegraded:
		v = franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_DEGRADED
	case provider.PhaseError:
		v = franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_ERROR
	case provider.PhaseStopped:
		v = franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_STOPPED
	case provider.PhaseRemoved:
		v = franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_REMOVED
	}
	return &v
}
