package localkafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/docker"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/probe"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/reconcile"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/stream"
)

// bearerCreds attaches `authorization: Bearer <token>` to every RPC.
type bearerCreds struct{ token string }

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}
func (bearerCreds) RequireTransportSecurity() bool { return false }

// Agent is the assembled local-kafka-docker-agent.
type Agent struct {
	cfg    Config
	log    *slog.Logger
	conn   *grpc.ClientConn
	client franzv1.ClusterProviderServiceClient
	driver docker.Driver
}

// NewAgent dials Franz and connects to Docker.
func NewAgent(cfg Config, log *slog.Logger) (*Agent, error) {
	conn, err := grpc.NewClient(
		cfg.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerCreds{token: cfg.Token}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial franz %s: %w", cfg.Endpoint, err)
	}

	drv, err := docker.NewEngineDriver(cfg.DockerHost)
	if err != nil {
		conn.Close()
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := drv.Ping(pingCtx); err != nil {
		conn.Close()
		_ = drv.Close()
		return nil, fmt.Errorf("docker not reachable (DOCKER_HOST=%q): %w", cfg.DockerHost, err)
	}

	return &Agent{
		cfg:    cfg,
		log:    log,
		conn:   conn,
		client: franzv1.NewClusterProviderServiceClient(conn),
		driver: drv,
	}, nil
}

// Close releases the gRPC and Docker connections.
func (a *Agent) Close() {
	_ = a.conn.Close()
	if c, ok := a.driver.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// Run watches assignments and reconciles until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	reporter := &grpcReporter{client: a.client, log: a.log}
	rec := reconcile.New(a.cfg.AgentName, a.cfg.KafkaVersionDefault, a.driver, reporter,
		func(ctx context.Context, url string) (bool, string) {
			r := probe.Broker(ctx, url)
			return r.Ready, r.Message
		})

	w := &stream.Watcher{
		Open: func(ctx context.Context) (stream.AssignmentStream, error) {
			return a.client.WatchClusterAssignments(ctx, &franzv1.WatchClusterAssignmentsRequest{})
		},
		Sync:       rec.Sync,
		Log:        a.log,
		BackoffMax: a.cfg.ReconnectBackoffMax,
		Debounce:   500 * time.Millisecond,
		Resync:     a.cfg.ResyncInterval,
	}
	a.log.Info("agent running", "endpoint", a.cfg.Endpoint, "agent", a.cfg.AgentName)
	return w.Run(ctx)
}

// grpcReporter implements reconcile.Reporter over ReportClusterStatus.
type grpcReporter struct {
	client franzv1.ClusterProviderServiceClient
	log    *slog.Logger
}

func (r *grpcReporter) Report(ctx context.Context, cluster string, phase reconcile.Phase, reachable bool, message, ref string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.client.ReportClusterStatus(ctx, franzv1.ReportClusterStatusRequest_builder{
		ClusterName: strptr(cluster),
		Phase:       phaseToProto(phase).Enum(),
		Reachable:   boolptr(reachable),
		Message:     strptr(message),
		RecipeRef:   strptr(ref),
	}.Build())
	if err != nil {
		r.log.Warn("ReportClusterStatus failed", "cluster", cluster, "phase", phase, "err", err)
		return err
	}
	lvl := slog.LevelInfo
	if phase == reconcile.PhaseError || phase == reconcile.PhaseDegraded {
		lvl = slog.LevelWarn
	}
	r.log.Log(ctx, lvl, "reported", "cluster", cluster, "phase", phase, "reachable", reachable, "detail", message)
	return nil
}

func phaseToProto(p reconcile.Phase) franzv1.ClusterProviderPhase {
	switch p {
	case reconcile.PhaseProvisioning:
		return franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_PROVISIONING
	case reconcile.PhaseReady:
		return franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_READY
	case reconcile.PhaseDegraded:
		return franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_DEGRADED
	case reconcile.PhaseError:
		return franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_ERROR
	case reconcile.PhaseStopped:
		return franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_STOPPED
	case reconcile.PhaseRemoved:
		return franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_REMOVED
	default:
		return franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_UNSPECIFIED
	}
}

func strptr(s string) *string { return &s }
func boolptr(b bool) *bool    { return &b }
