package grpcgateway_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/in/grpcgateway"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/stub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/agents"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	provideruc "github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/provider"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/internal/dbtest"
)

func TestClusterProviderE2E(t *testing.T) {
	dsn := os.Getenv("FRANZ_TEST_DB_DSN")
	if dsn == "" {
		t.Skip("set FRANZ_TEST_DB_DSN to run the cluster-provider e2e test")
	}
	ctx := context.Background()
	db, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(db.Close)
	dbtest.Lock(t, db.Pool()) // serialise with the postgres package's DB tests
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, tbl := range []string{"cluster_provider_event", "kafka_cluster", "agent"} {
		if _, err := db.Pool().Exec(ctx, "DELETE FROM "+tbl); err != nil {
			t.Fatalf("clean %s: %v", tbl, err)
		}
	}

	realmRepo := postgres.NewRealmRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)
	agentRepo := postgres.NewAgentRepo(db)
	eventRepo := postgres.NewProviderEventRepo(db)
	hub := streamhub.New()
	codec := frn.MustCodec("frn")

	clusterSvc := clusters.NewService(clusterRepo, stub.NoTopicGuard{}, eventRepo, hub)
	agentSvc := agents.NewService(agentRepo)
	providerSvc := provideruc.NewService(clusterRepo, eventRepo)

	srv := grpcgateway.New(0, 0, slog.New(slog.NewTextHandler(io.Discard, nil)),
		grpcgateway.WithAuthenticator(grpcgateway.NewAuthenticator(realmRepo)),
		grpcgateway.WithAgentAuth(grpcgateway.NewAgentAuthenticator(agentRepo)),
	)
	if err := grpcgateway.RegisterKafkaClusterService(srv, clusterSvc, providerSvc, codec); err != nil {
		t.Fatal(err)
	}
	if err := grpcgateway.RegisterAgentService(srv, agentSvc, codec); err != nil {
		t.Fatal(err)
	}
	grpcgateway.RegisterClusterProviderService(srv, providerSvc, hub, codec)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Grpc().Serve(lis) }()
	t.Cleanup(srv.Grpc().Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	agentCli := franzv1.NewAgentServiceClient(conn)
	clusterCli := franzv1.NewKafkaClusterServiceClient(conn)
	cpCli := franzv1.NewClusterProviderServiceClient(conn)

	// --- register agent + cluster it owns --------------------------------
	created, err := agentCli.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
		Name: proto.String("prov-1"),
		Type: franzv1.AgentType_AGENT_TYPE_CLUSTER_PROVIDER.Enum(),
	}.Build())
	if err != nil {
		t.Fatalf("CreateAgent: %v", err)
	}
	token := created.GetToken()

	if _, err := clusterCli.CreateKafkaCluster(ctx, franzv1.CreateKafkaClusterRequest_builder{
		Name:                 proto.String("east-1"),
		ConnectionStrings:    []*franzv1.ConnectionString{franzv1.ConnectionString_builder{BootstrapUrls: []string{"localhost:9092"}}.Build()},
		ClusterProviderAgent: proto.String("prov-1"),
		Labels:               map[string]string{"franz.provisioning/deployment-type": "local-docker"},
	}.Build()); err != nil {
		t.Fatalf("CreateKafkaCluster: %v", err)
	}

	// --- open the stream: expect the full set (one CHANGE_SET) -----------
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	streamCtx, cancelStream := context.WithCancel(authCtx)
	defer cancelStream()

	stream, err := cpCli.WatchClusterAssignments(streamCtx, franzv1.WatchClusterAssignmentsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("WatchClusterAssignments: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv (initial): %v", err)
	}
	if a := first.GetAssignment(); a.GetClusterName() != "east-1" || a.GetChange() != franzv1.ClusterAssignment_CHANGE_SET {
		t.Fatalf("initial assignment = %+v", first.GetAssignment())
	}
	if first.GetAssignment().GetProvisioning()["franz.provisioning/deployment-type"] != "local-docker" {
		t.Errorf("provisioning labels missing on initial assignment")
	}

	// --- edit a provisioning label → expect a CHANGE_SET delta -----------
	if _, err := clusterCli.UpdateKafkaCluster(ctx, franzv1.UpdateKafkaClusterRequest_builder{
		Name:       proto.String("east-1"),
		Labels:     map[string]string{"franz.provisioning/deployment-type": "local-docker", "franz.provisioning/kafka-version": "3.7.0"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}.Build()); err != nil {
		t.Fatalf("UpdateKafkaCluster: %v", err)
	}
	delta := recvWithTimeout(t, stream)
	if a := delta.GetAssignment(); a.GetChange() != franzv1.ClusterAssignment_CHANGE_SET ||
		a.GetProvisioning()["franz.provisioning/kafka-version"] != "3.7.0" {
		t.Fatalf("delta assignment = %+v", delta.GetAssignment())
	}

	// --- report status → surfaces on GetKafkaCluster --------------------
	if _, err := cpCli.ReportClusterStatus(authCtx, franzv1.ReportClusterStatusRequest_builder{
		ClusterName: proto.String("east-1"),
		Phase:       franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_READY.Enum(),
		Reachable:   proto.Bool(true),
		RecipeRef:   proto.String("local-docker@abcd1234"),
	}.Build()); err != nil {
		t.Fatalf("ReportClusterStatus: %v", err)
	}
	got, err := clusterCli.GetKafkaCluster(ctx, franzv1.GetKafkaClusterRequest_builder{Name: proto.String("east-1")}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if ps := got.GetKafkaCluster().GetProviderStatus(); ps == nil ||
		ps.GetPhase() != franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_READY || !ps.GetReachable() {
		t.Fatalf("provider_status = %+v", got.GetKafkaCluster().GetProviderStatus())
	}

	// --- history --------------------------------------------------------
	hist, err := clusterCli.ListClusterProviderEvents(ctx, franzv1.ListClusterProviderEventsRequest_builder{
		Name: proto.String("east-1"),
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if len(hist.GetEvents()) != 1 || hist.GetEvents()[0].GetReportingAgent() != "prov-1" {
		t.Fatalf("history = %+v", hist.GetEvents())
	}

	// --- ownership: a different agent cannot report --------------------
	other, _ := agentCli.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
		Name: proto.String("prov-2"), Type: franzv1.AgentType_AGENT_TYPE_CLUSTER_PROVIDER.Enum(),
	}.Build())
	otherCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+other.GetToken())
	_, err = cpCli.ReportClusterStatus(otherCtx, franzv1.ReportClusterStatusRequest_builder{
		ClusterName: proto.String("east-1"),
		Phase:       franzv1.ClusterProviderPhase_CLUSTER_PROVIDER_PHASE_READY.Enum(),
	}.Build())
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-owner report = %v, want PERMISSION_DENIED", err)
	}

	// --- token rotation invalidates the old token ---------------------
	if _, err := agentCli.RotateAgentToken(ctx, franzv1.RotateAgentTokenRequest_builder{Name: proto.String("prov-1")}.Build()); err != nil {
		t.Fatalf("RotateAgentToken: %v", err)
	}
	badStream, err := cpCli.WatchClusterAssignments(
		metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token),
		franzv1.WatchClusterAssignmentsRequest_builder{}.Build())
	if err == nil {
		_, err = badStream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("old token after rotation = %v, want UNAUTHENTICATED", err)
	}

	// --- unauthenticated: no token ----------------------------------
	noTokStream, err := cpCli.WatchClusterAssignments(ctx, franzv1.WatchClusterAssignmentsRequest_builder{}.Build())
	if err == nil {
		_, err = noTokStream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token = %v, want UNAUTHENTICATED", err)
	}
}

func recvWithTimeout(t *testing.T, s grpc.ServerStreamingClient[franzv1.WatchClusterAssignmentsResponse]) *franzv1.WatchClusterAssignmentsResponse {
	t.Helper()
	type result struct {
		msg *franzv1.WatchClusterAssignmentsResponse
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := s.Recv()
		ch <- result{m, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("stream.Recv (delta): %v", r.err)
		}
		return r.msg
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a stream delta")
		return nil
	}
}
