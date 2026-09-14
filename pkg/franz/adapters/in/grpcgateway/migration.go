package grpcgateway

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/migration"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// migrationHandler adapts MigrationService onto the generated gRPC server
// interface (003.13).
type migrationHandler struct {
	franzv1.UnimplementedMigrationServiceServer
	svc in.MigrationService
}

// RegisterMigrationService mounts MigrationService on the gRPC server and the
// in-process REST gateway.
func RegisterMigrationService(s *Server, svc in.MigrationService) error {
	h := &migrationHandler{svc: svc}
	franzv1.RegisterMigrationServiceServer(s.grpc, h)
	return franzv1.RegisterMigrationServiceHandlerServer(context.Background(), s.gw, h)
}

func (h *migrationHandler) MigrateKafkaTopic(
	ctx context.Context, req *franzv1.MigrateKafkaTopicRequest,
) (*franzv1.MigrateKafkaTopicResponse, error) {
	m, err := h.svc.MigrateKafkaTopic(ctx, req.GetKafkaTopic(), req.GetTargetCluster())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.MigrateKafkaTopicResponse_builder{Migration: migrationToProto(m)}.Build(), nil
}

func (h *migrationHandler) MigrateCluster(
	ctx context.Context, req *franzv1.MigrateClusterRequest,
) (*franzv1.MigrateClusterResponse, error) {
	migrations, err := h.svc.MigrateCluster(ctx, req.GetKafkaCluster(), req.GetReason())
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.ShardMigration, len(migrations))
	for i, m := range migrations {
		out[i] = migrationToProto(m)
	}
	return franzv1.MigrateClusterResponse_builder{Migrations: out}.Build(), nil
}

func (h *migrationHandler) GetShardMigration(
	ctx context.Context, req *franzv1.GetShardMigrationRequest,
) (*franzv1.GetShardMigrationResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, ToError(errs.InvalidField("id", "must be a valid uuid"))
	}
	m, err := h.svc.GetShardMigration(ctx, id)
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetShardMigrationResponse_builder{Migration: migrationToProto(m)}.Build(), nil
}

func (h *migrationHandler) ListShardMigrations(
	ctx context.Context, req *franzv1.ListShardMigrationsRequest,
) (*franzv1.ListShardMigrationsResponse, error) {
	page, err := h.svc.ListShardMigrations(ctx, in.ListShardMigrationsInput{
		AsyncChannel: req.GetAsyncChannel(),
		PageSize:     req.GetPage().GetPageSize(),
		PageToken:    req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.ShardMigration, len(page.Migrations))
	for i, m := range page.Migrations {
		out[i] = migrationToProto(m)
	}
	return franzv1.ListShardMigrationsResponse_builder{
		Migrations: out,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

// --- mapping helpers -------------------------------------------------------

func migrationToProto(m *migration.ShardMigration) *franzv1.ShardMigration {
	b := franzv1.ShardMigration_builder{
		Id:                 proto.String(m.ID.String()),
		AsyncChannel:       proto.String(m.AsyncChannelName),
		SourceKafkaTopic:   proto.String(m.SourceTopicName),
		TargetKafkaTopic:   proto.String(m.TargetTopicName),
		SourceKafkaCluster: proto.String(m.SourceClusterName),
		TargetKafkaCluster: proto.String(m.TargetClusterName),
		Phase:              migrationPhaseToProto(m.Phase),
		Reason:             proto.String(m.Reason),
		FailureReason:      proto.String(m.FailureReason),
		StartedAt:          timestamppb.New(m.StartedAt),
	}
	if !m.DrainDeadline.IsZero() {
		b.DrainDeadline = timestamppb.New(m.DrainDeadline)
	}
	if !m.CompletedAt.IsZero() {
		b.CompletedAt = timestamppb.New(m.CompletedAt)
	}
	return b.Build()
}

func migrationPhaseToProto(p migration.Phase) *franzv1.MigrationPhase {
	v := franzv1.MigrationPhase_MIGRATION_PHASE_UNSPECIFIED
	switch p {
	case migration.PhaseProvisioning:
		v = franzv1.MigrationPhase_MIGRATION_PHASE_PROVISIONING
	case migration.PhaseCutover:
		v = franzv1.MigrationPhase_MIGRATION_PHASE_CUTOVER
	case migration.PhaseDraining:
		v = franzv1.MigrationPhase_MIGRATION_PHASE_DRAINING
	case migration.PhaseRetiring:
		v = franzv1.MigrationPhase_MIGRATION_PHASE_RETIRING
	case migration.PhaseDone:
		v = franzv1.MigrationPhase_MIGRATION_PHASE_DONE
	case migration.PhaseFailed:
		v = franzv1.MigrationPhase_MIGRATION_PHASE_FAILED
	}
	return &v
}
