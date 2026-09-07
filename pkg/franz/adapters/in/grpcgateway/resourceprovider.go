package grpcgateway

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// resourceProviderHandler implements the agent-facing ResourceProviderService
// (005 ADR). gRPC only — no REST gateway.
type resourceProviderHandler struct {
	franzv1.UnimplementedResourceProviderServiceServer
	svc   in.ResourceProviderService
	hub   *streamhub.Hub
	codec frn.Codec
}

// RegisterResourceProviderService mounts the service on the gRPC server only.
func RegisterResourceProviderService(
	s *Server, svc in.ResourceProviderService, hub *streamhub.Hub, codec frn.Codec,
) {
	franzv1.RegisterResourceProviderServiceServer(s.grpc,
		&resourceProviderHandler{svc: svc, hub: hub, codec: codec})
}

// WatchPartitionAssignments streams the agent its in-scope async channel
// partitions: the full current set on open, then a message per change.
func (h *resourceProviderHandler) WatchPartitionAssignments(
	_ *franzv1.WatchPartitionAssignmentsRequest,
	stream grpc.ServerStreamingServer[franzv1.WatchPartitionAssignmentsResponse],
) error {
	ctx := stream.Context()
	ag := agent.MustFromContext(ctx)

	// Subscribe before reading the initial set so a change in between is buffered
	// rather than lost between the snapshot and the first delta.
	deltas, unsubscribe := h.hub.SubscribePartitions(ag.Name)
	defer unsubscribe()

	initial, err := h.svc.InitialPartitionAssignments(ctx)
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

// ReportPartitionReconciliation records one reconcile outcome. A stale-generation
// report is not an error: it is acknowledged with applied=false (005 ADR §1.5).
func (h *resourceProviderHandler) ReportPartitionReconciliation(
	ctx context.Context, req *franzv1.ReportPartitionReconciliationRequest,
) (*franzv1.ReportPartitionReconciliationResponse, error) {
	report := req.GetReport()
	if report == nil {
		return nil, ToError(errs.InvalidField("report", "must be set"))
	}
	partitionFRN, err := h.codec.Parse(report.GetPartitionFrn())
	if err != nil {
		return nil, ToError(err)
	}
	if partitionFRN.Type() != frn.TypeKafkaTopic {
		return nil, ToError(errs.InvalidField("report.partition_frn",
			"must name a kafka-topic, got "+string(partitionFRN.Type())))
	}
	outcome, err := outcomeFromProto(report.GetOutcome())
	if err != nil {
		return nil, ToError(err)
	}

	applied, err := h.svc.ReportReconciliation(ctx, in.ReportReconciliationInput{
		PartitionFRNPath: partitionFRN.Path(),
		Generation:       report.GetGeneration(),
		Outcome:          outcome,
		Message:          report.GetMessage(),
		Applied:          appliedFromProto(report.GetAppliedConfig()),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.ReportPartitionReconciliationResponse_builder{
		Applied: proto.Bool(applied),
	}.Build(), nil
}

// --- mapping ------------------------------------------------------------

func (h *resourceProviderHandler) assignmentResponse(
	a resource.PartitionAssignment,
) *franzv1.WatchPartitionAssignmentsResponse {
	conns := make([]*franzv1.ConnectionString, len(a.ConnectionStrings))
	for i, cs := range a.ConnectionStrings {
		ct := franzv1.ConnectionType_CONNECTION_TYPE_UNSPECIFIED
		if cs.Type == "PLAINTEXT" {
			ct = franzv1.ConnectionType_CONNECTION_TYPE_PLAINTEXT
		}
		conns[i] = franzv1.ConnectionString_builder{BootstrapUrls: cs.BootstrapURLs, Type: &ct}.Build()
	}
	return franzv1.WatchPartitionAssignmentsResponse_builder{
		Assignment: franzv1.PartitionAssignment_builder{
			Change:            partitionChangeToProto(a.Change),
			Reason:            partitionReasonToProto(a.Reason),
			PartitionFrn:      proto.String(h.codec.Render(a.PartitionFRN)),
			Generation:        proto.Int64(a.Generation),
			AsyncChannel:      proto.String(a.AsyncChannel),
			TopicName:         proto.String(a.TopicName),
			KafkaCluster:      proto.String(a.ClusterName),
			KafkaClusterFrn:   proto.String(h.codec.Render(a.ClusterFRN)),
			ConnectionStrings: conns,
			DesiredConfig:     a.DesiredConfig,
			Partitions:        proto.Int32(a.Partitions),
			ReplicationFactor: proto.Int32(a.ReplicationFactor),
		}.Build(),
	}.Build()
}

func partitionChangeToProto(c resource.Change) *franzv1.PartitionAssignment_Change {
	v := franzv1.PartitionAssignment_CHANGE_UNSPECIFIED
	switch c {
	case resource.ChangeSet:
		v = franzv1.PartitionAssignment_CHANGE_SET
	case resource.ChangePaused:
		v = franzv1.PartitionAssignment_CHANGE_PAUSED
	case resource.ChangeRemoved:
		v = franzv1.PartitionAssignment_CHANGE_REMOVED
	}
	return &v
}

func partitionReasonToProto(r resource.Reason) *franzv1.PartitionAssignment_Reason {
	v := franzv1.PartitionAssignment_REASON_UNSPECIFIED
	if r == resource.ReasonScopeLoss {
		v = franzv1.PartitionAssignment_REASON_SCOPE_LOSS
	}
	return &v
}

func outcomeFromProto(o franzv1.ReconciliationOutcome) (topic.Outcome, error) {
	switch o {
	case franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_CREATED:
		return topic.OutcomeCreated, nil
	case franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_UPDATED:
		return topic.OutcomeUpdated, nil
	case franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_NOOP:
		return topic.OutcomeNoop, nil
	case franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_DELETED:
		return topic.OutcomeDeleted, nil
	case franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_ERROR:
		return topic.OutcomeError, nil
	default:
		return "", errs.InvalidField("report.outcome",
			"must be one of CREATED, UPDATED, NOOP, DELETED, ERROR")
	}
}

func appliedFromProto(a *franzv1.AppliedTopicState) *topic.AppliedState {
	if a == nil {
		return nil
	}
	return &topic.AppliedState{
		Partitions:        a.GetPartitions(),
		ReplicationFactor: a.GetReplicationFactor(),
		Config:            a.GetConfig(),
	}
}
