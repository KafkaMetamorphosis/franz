package gregorsamsa

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/reconcile"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/stream"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/telemetry"
)

// bearerCreds attaches `authorization: Bearer <token>` to every RPC.
type bearerCreds struct{ token string }

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}
func (bearerCreds) RequireTransportSecurity() bool { return false }

// Agent is the assembled Gregor Samsa instance.
type Agent struct {
	cfg  Config
	log  *slog.Logger
	conn *grpc.ClientConn

	resources franzv1.ResourceProviderServiceClient
	telemetry franzv1.TelemetryServiceClient

	reconciler *reconcile.Reconciler
	sweeper    *telemetry.Sweeper
}

// NewAgent dials Franz and assembles the reconciler and telemetry sweeper.
// adminFactory is the Kafka driver; pass kafkaadmin.NewKadm in production and an
// in-memory fake in tests.
func NewAgent(cfg Config, log *slog.Logger, adminFactory kafkaadmin.Factory) (*Agent, error) {
	conn, err := grpc.NewClient(
		cfg.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerCreds{token: cfg.Token}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial franz %s: %w", cfg.Endpoint, err)
	}

	a := &Agent{
		cfg:       cfg,
		log:       log,
		conn:      conn,
		resources: franzv1.NewResourceProviderServiceClient(conn),
		telemetry: franzv1.NewTelemetryServiceClient(conn),
	}

	// The sweeper reads the reconciler's world, and the reconciler calls the
	// sweeper after each partition — build the reconciler first with a hook that
	// resolves the sweeper lazily, so neither has to know about the other's
	// construction order.
	a.reconciler = reconcile.New(
		adminFactory,
		&grpcReporter{client: a.resources, log: log, timeout: cfg.ReportTimeout},
		log,
		a.observeAfterReconcile,
	)
	if cfg.TelemetryInterval > 0 {
		a.sweeper = telemetry.NewSweeper(
			a.reconciler,
			&grpcPublisher{client: a.telemetry, agentName: cfg.AgentName, log: log},
			cfg.TelemetryInterval, log)
	}
	return a, nil
}

// Close releases the gRPC connection and every cached AdminClient.
func (a *Agent) Close() {
	a.reconciler.Close()
	_ = a.conn.Close()
}

// Run watches partition assignments and reconciles until ctx is cancelled,
// sweeping telemetry alongside.
func (a *Agent) Run(ctx context.Context) error {
	watcher := &stream.Watcher{
		Open: func(ctx context.Context) (stream.AssignmentStream, error) {
			return a.resources.WatchPartitionAssignments(ctx,
				franzv1.WatchPartitionAssignmentsRequest_builder{}.Build())
		},
		Sync:       a.reconciler.Sync,
		Log:        a.log,
		BackoffMin: a.cfg.ReconnectBackoffMin,
		BackoffMax: a.cfg.ReconnectBackoffMax,
		Debounce:   a.cfg.Debounce,
	}

	a.log.Info("gregor samsa running",
		"endpoint", a.cfg.Endpoint, "agent", a.cfg.AgentName,
		"telemetry_interval", a.cfg.TelemetryInterval)

	var wg sync.WaitGroup
	if a.sweeper != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.sweeper.Run(ctx)
		}()
	}
	err := watcher.Run(ctx)
	wg.Wait()
	return err
}

// observeAfterReconcile publishes a fresh sample for a partition the moment it
// reconciles, so kafka.topic.state does not wait for the next sweep.
func (a *Agent) observeAfterReconcile(ctx context.Context, as assign.Assignment, admin kafkaadmin.Admin) {
	if a.sweeper == nil {
		return
	}
	a.sweeper.ObservePartition(ctx, as, admin)
}

// grpcReporter implements reconcile.Reporter over ReportPartitionReconciliation.
type grpcReporter struct {
	client  franzv1.ResourceProviderServiceClient
	log     *slog.Logger
	timeout time.Duration
}

func (r *grpcReporter) Report(ctx context.Context, report reconcile.Report) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	resp, err := r.client.ReportPartitionReconciliation(ctx,
		franzv1.ReportPartitionReconciliationRequest_builder{
			Report: franzv1.PartitionReconciliationReport_builder{
				PartitionFrn:  proto.String(report.PartitionFRN),
				Generation:    proto.Int64(report.Generation),
				Outcome:       outcomeToProto(report.Outcome).Enum(),
				Message:       proto.String(report.Message),
				AppliedConfig: appliedToProto(report.Applied),
			}.Build(),
		}.Build())
	if err != nil {
		return err
	}
	if !resp.GetApplied() {
		// Not a failure: the desired state moved while we were working. Franz has
		// already re-emitted the assignment; the next sync reconciles it.
		r.log.Info("report was stale; franz will re-offer the partition",
			"partition", report.PartitionFRN, "generation", report.Generation)
	}
	return nil
}

func outcomeToProto(o reconcile.Outcome) franzv1.ReconciliationOutcome {
	switch o {
	case reconcile.OutcomeCreated:
		return franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_CREATED
	case reconcile.OutcomeUpdated:
		return franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_UPDATED
	case reconcile.OutcomeNoop:
		return franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_NOOP
	case reconcile.OutcomeDeleted:
		return franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_DELETED
	case reconcile.OutcomeError:
		return franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_ERROR
	default:
		return franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_UNSPECIFIED
	}
}

func appliedToProto(t *kafkaadmin.Topic) *franzv1.AppliedTopicState {
	if t == nil {
		return nil
	}
	return franzv1.AppliedTopicState_builder{
		Partitions:        proto.Int32(t.Partitions),
		ReplicationFactor: proto.Int32(t.ReplicationFactor),
		Config:            t.Config,
	}.Build()
}

// grpcPublisher implements telemetry.Publisher over PublishIndicatorSamples.
//
// It uses the unary form per batch rather than holding the client stream open:
// a sweep is one batch a minute, and a unary call gets a per-batch ack and
// per-batch error instead of losing the whole stream on one bad sample. The
// client-streaming StreamIndicatorSamples exists for a future high-frequency
// producer (005 ADR §2.2, 003.14 OQ2).
type grpcPublisher struct {
	client    franzv1.TelemetryServiceClient
	agentName string
	log       *slog.Logger
}

func (p *grpcPublisher) Publish(ctx context.Context, samples []telemetry.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	wire := make([]*franzv1.IndicatorSample, 0, len(samples))
	for _, s := range samples {
		wire = append(wire, franzv1.IndicatorSample_builder{
			Indicator:      proto.String(s.Indicator),
			ResourceFrn:    proto.String(s.ResourceFRN),
			ResourceEntity: entityToProto(s.ResourceEntity).Enum(),
			Value:          proto.String(s.Value),
			SampleAt:       timestamppb.New(s.SampleAt),
		}.Build())
	}
	_, err := p.client.PublishIndicatorSamples(ctx, franzv1.PublishIndicatorSamplesRequest_builder{
		Agent:   proto.String(p.agentName),
		Samples: wire,
	}.Build())
	return err
}

func entityToProto(e telemetry.Entity) franzv1.Entity {
	switch e {
	case telemetry.EntityKafkaTopic:
		return franzv1.Entity_ENTITY_KAFKA_TOPIC
	case telemetry.EntityKafkaCluster:
		return franzv1.Entity_ENTITY_KAFKA_CLUSTER
	default:
		return franzv1.Entity_ENTITY_UNSPECIFIED
	}
}
