package grpcgateway_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/in/grpcgateway"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/stub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/agents"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/channels"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	provideruc "github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/resourceprovider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/telemetry"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/topics"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/internal/dbtest"
)

// resourceProviderFixture is the whole Resource Provider wire over bufconn:
// Postgres, the scope resolver, the stream hub, and the agent-auth interceptor.
type resourceProviderFixture struct {
	db       *postgres.DB
	realmID  uuid.UUID
	agents   franzv1.AgentServiceClient
	clusters franzv1.KafkaClusterServiceClient
	channels franzv1.AsyncChannelServiceClient
	topics   franzv1.KafkaTopicServiceClient
	resource franzv1.ResourceProviderServiceClient
	tele     franzv1.TelemetryServiceClient
}

func newResourceProviderFixture(t *testing.T) *resourceProviderFixture {
	t.Helper()
	dsn := os.Getenv("FRANZ_TEST_DB_DSN")
	if dsn == "" {
		t.Skip("set FRANZ_TEST_DB_DSN to run the resource-provider e2e test")
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
	for _, tbl := range []string{
		"indicator_sample", "kafka_topic", "async_channel",
		"cluster_provider_event", "kafka_cluster", "agent",
	} {
		if _, err := db.Pool().Exec(ctx, "DELETE FROM "+tbl); err != nil {
			t.Fatalf("clean %s: %v", tbl, err)
		}
	}

	var realmID uuid.UUID
	if err := db.Pool().QueryRow(ctx, `SELECT id FROM realm WHERE slug='default'`).Scan(&realmID); err != nil {
		t.Fatalf("resolve default realm: %v", err)
	}

	realmRepo := postgres.NewRealmRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)
	agentRepo := postgres.NewAgentRepo(db)
	topicRepo := postgres.NewTopicRepo(db)
	channelRepo := postgres.NewChannelRepo(db)
	eventRepo := postgres.NewProviderEventRepo(db)
	sampleRepo := postgres.NewIndicatorSampleRepo(db)
	hub := streamhub.New()
	codec := frn.MustCodec("frn")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	notifier := resourceprovider.NewNotifier(agentRepo, clusterRepo, topicRepo, hub, log)
	clusterSvc := clusters.NewService(clusterRepo, stub.NoTopicGuard{}, eventRepo, hub, notifier)
	agentSvc := agents.NewService(agentRepo, notifier)
	channelSvc := channels.NewService(channelRepo, notifier)
	topicSvc := topics.NewService(topicRepo, clusterRepo, notifier)
	providerSvc := provideruc.NewService(clusterRepo, eventRepo)
	resourceSvc := resourceprovider.NewService(clusterRepo, topicRepo)
	telemetrySvc := telemetry.NewService(sampleRepo)

	srv := grpcgateway.New(0, 0, log,
		grpcgateway.WithAuthenticator(grpcgateway.NewAuthenticator(realmRepo)),
		grpcgateway.WithAgentAuth(grpcgateway.NewAgentAuthenticator(agentRepo)),
	)
	if err := grpcgateway.RegisterKafkaClusterService(srv, clusterSvc, providerSvc, codec); err != nil {
		t.Fatal(err)
	}
	if err := grpcgateway.RegisterAgentService(srv, agentSvc, codec); err != nil {
		t.Fatal(err)
	}
	if err := grpcgateway.RegisterAsyncChannelService(srv, channelSvc, codec); err != nil {
		t.Fatal(err)
	}
	if err := grpcgateway.RegisterKafkaTopicService(srv, topicSvc, codec); err != nil {
		t.Fatal(err)
	}
	grpcgateway.RegisterResourceProviderService(srv, resourceSvc, hub, codec)
	grpcgateway.RegisterTelemetryService(srv, telemetrySvc)

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

	return &resourceProviderFixture{
		db:       db,
		realmID:  realmID,
		agents:   franzv1.NewAgentServiceClient(conn),
		clusters: franzv1.NewKafkaClusterServiceClient(conn),
		channels: franzv1.NewAsyncChannelServiceClient(conn),
		topics:   franzv1.NewKafkaTopicServiceClient(conn),
		resource: franzv1.NewResourceProviderServiceClient(conn),
		tele:     franzv1.NewTelemetryServiceClient(conn),
	}
}

// insertShard writes a kafka_topic row directly. Placement (deliverable 13) is
// the real producer of these rows (ADR-API-009); until it lands the tests seed
// them, the same pattern deliverables 09/10 use.
func (f *resourceProviderFixture) insertShard(
	t *testing.T, channelID uuid.UUID, clusterName, name string, partitions, rf int32,
	config map[string]string,
) {
	t.Helper()
	ctx := context.Background()
	var clusterID uuid.UUID
	if err := f.db.Pool().QueryRow(ctx,
		`SELECT id FROM kafka_cluster WHERE realm_id=$1 AND name=$2`, f.realmID, clusterName).
		Scan(&clusterID); err != nil {
		t.Fatalf("resolve cluster %q: %v", clusterName, err)
	}
	cfg := "{}"
	if len(config) > 0 {
		cfg = `{"retention.ms": "` + config["retention.ms"] + `"}`
	}
	if _, err := f.db.Pool().Exec(ctx, `
		INSERT INTO kafka_topic
			(id, realm_id, async_channel_id, kafka_cluster_id, name, frn,
			 topic_configuration, materialized_configuration, partitions,
			 replication_factor, state, consumption, generation)
		VALUES ($1,$2,$3,$4,$5,$6,'{}'::jsonb,$7::jsonb,$8,$9,'PENDING','ENABLED',1)`,
		uuid.New(), f.realmID, channelID, clusterID, name, "default:kafka-topic:"+name,
		cfg, partitions, rf); err != nil {
		t.Fatalf("insert shard %q: %v", name, err)
	}
}

func (f *resourceProviderFixture) channelID(t *testing.T, name string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := f.db.Pool().QueryRow(context.Background(),
		`SELECT id FROM async_channel WHERE realm_id=$1 AND name=$2`, f.realmID, name).Scan(&id); err != nil {
		t.Fatalf("resolve channel %q: %v", name, err)
	}
	return id
}

func (f *resourceProviderFixture) shardRow(t *testing.T, name string) (state string, generation int64, reconciled *int64, message string) {
	t.Helper()
	if err := f.db.Pool().QueryRow(context.Background(),
		`SELECT state, generation, reconciled_generation, last_reconcile_message
		 FROM kafka_topic WHERE realm_id=$1 AND name=$2`, f.realmID, name).
		Scan(&state, &generation, &reconciled, &message); err != nil {
		t.Fatalf("read shard %q: %v", name, err)
	}
	return state, generation, reconciled, message
}

func TestResourceProviderE2E(t *testing.T) {
	f := newResourceProviderFixture(t)
	ctx := context.Background()

	// --- an agent scoped to prod, and one cluster in each of two scopes ------
	created, err := f.agents.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
		Name:   proto.String("gregor-samsa-prod"),
		Type:   franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER.Enum(),
		Labels: map[string]string{"franz.placement-selector/env": "prod"},
	}.Build())
	if err != nil {
		t.Fatalf("CreateAgent: %v", err)
	}
	token := created.GetToken()

	for name, env := range map[string]string{"east-1": "prod", "west-1": "staging"} {
		if _, err := f.clusters.CreateKafkaCluster(ctx, franzv1.CreateKafkaClusterRequest_builder{
			Name: proto.String(name),
			ConnectionStrings: []*franzv1.ConnectionString{
				franzv1.ConnectionString_builder{BootstrapUrls: []string{name + ":9092"}}.Build(),
			},
			Labels: map[string]string{"franz.placement/env": env},
		}.Build()); err != nil {
			t.Fatalf("CreateKafkaCluster %q: %v", name, err)
		}
	}

	if _, err := f.channels.CreateAsyncChannel(ctx, franzv1.CreateAsyncChannelRequest_builder{
		Name:              proto.String("billing-events"),
		Type:              franzv1.ChannelType_CHANNEL_TYPE_KAFKA_TOPIC.Enum(),
		ChannelPartitions: proto.Int32(1),
	}.Build()); err != nil {
		t.Fatalf("CreateAsyncChannel: %v", err)
	}
	channelID := f.channelID(t, "billing-events")

	// Placement is deliverable 13; seed the rows directly.
	f.insertShard(t, channelID, "east-1", "billing-events-0", 3, 1,
		map[string]string{"retention.ms": "604800000"})
	f.insertShard(t, channelID, "west-1", "billing-events-1", 3, 1, nil)

	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	// --- the stream opens with the full in-scope set, and only that ---------
	streamCtx, cancelStream := context.WithCancel(authCtx)
	defer cancelStream()

	stream, err := f.resource.WatchPartitionAssignments(streamCtx,
		franzv1.WatchPartitionAssignmentsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("WatchPartitionAssignments: %v", err)
	}
	first := recvPartition(t, stream)
	if first.GetTopicName() != "billing-events-0" {
		t.Fatalf("initial assignment = %+v; the staging cluster's partition must not be streamed", first)
	}
	if first.GetChange() != franzv1.PartitionAssignment_CHANGE_SET {
		t.Errorf("change = %v, want CHANGE_SET", first.GetChange())
	}
	if first.GetKafkaCluster() != "east-1" || first.GetKafkaClusterFrn() != "frn:default:kafka-cluster:east-1" {
		t.Errorf("cluster = %q / %q", first.GetKafkaCluster(), first.GetKafkaClusterFrn())
	}
	if first.GetPartitionFrn() != "frn:default:kafka-topic:billing-events-0" {
		t.Errorf("partition_frn = %q", first.GetPartitionFrn())
	}
	if first.GetDesiredConfig()["retention.ms"] != "604800000" ||
		first.GetPartitions() != 3 || first.GetReplicationFactor() != 1 {
		t.Errorf("desired state = %+v", first)
	}
	if urls := first.GetConnectionStrings(); len(urls) != 1 || urls[0].GetBootstrapUrls()[0] != "east-1:9092" {
		t.Errorf("connection strings = %+v", first.GetConnectionStrings())
	}
	partitionFRN := first.GetPartitionFrn()
	generation := first.GetGeneration()

	// --- a stale report is acknowledged but moves nothing -------------------
	stale, err := f.resource.ReportPartitionReconciliation(authCtx, reportRequest(
		partitionFRN, generation+99, franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_CREATED, ""))
	if err != nil {
		t.Fatalf("a stale report must be acknowledged, not rejected: %v", err)
	}
	if stale.GetApplied() {
		t.Error("applied = true for a stale generation, want false")
	}
	if state, _, reconciled, _ := f.shardRow(t, "billing-events-0"); state != "PENDING" || reconciled != nil {
		t.Fatalf("stale report moved the row: state=%s reconciled=%v", state, reconciled)
	}

	// --- the current-generation report drives PENDING → READY ---------------
	applied, err := f.resource.ReportPartitionReconciliation(authCtx, reportRequest(
		partitionFRN, generation, franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_CREATED, ""))
	if err != nil {
		t.Fatalf("ReportPartitionReconciliation: %v", err)
	}
	if !applied.GetApplied() {
		t.Fatal("applied = false, want true")
	}
	state, gen, reconciled, _ := f.shardRow(t, "billing-events-0")
	if state != "READY" {
		t.Fatalf("state = %s, want READY", state)
	}
	if reconciled == nil || *reconciled != gen {
		t.Fatalf("reconciled_generation = %v, want %d", reconciled, gen)
	}

	// --- an ERROR report drives → ERROR and records why ---------------------
	const detail = "topic has unconsumed data (partition 0: earliest=0 latest=45201)"
	if _, err := f.resource.ReportPartitionReconciliation(authCtx, reportRequest(
		partitionFRN, generation, franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_ERROR, detail)); err != nil {
		t.Fatalf("ReportPartitionReconciliation(ERROR): %v", err)
	}
	if state, _, _, message := f.shardRow(t, "billing-events-0"); state != "ERROR" || message != detail {
		t.Fatalf("state = %s, message = %q", state, message)
	}

	// --- a delta reaches the open stream ------------------------------------
	if _, err := f.topics.SetConsumption(ctx, franzv1.SetConsumptionRequest_builder{
		Name:        proto.String("billing-events-0"),
		Consumption: franzv1.Consumption_CONSUMPTION_DISABLED.Enum(),
	}.Build()); err != nil {
		t.Fatalf("SetConsumption: %v", err)
	}
	delta := recvPartition(t, stream)
	if delta.GetChange() != franzv1.PartitionAssignment_CHANGE_SET ||
		delta.GetTopicName() != "billing-events-0" {
		t.Fatalf("delta = %+v", delta)
	}
	if delta.GetGeneration() <= generation {
		t.Errorf("generation = %d, want a bump over %d", delta.GetGeneration(), generation)
	}

	// --- scope loss: the cluster is relabelled out of the agent's scope ------
	if _, err := f.clusters.UpdateKafkaCluster(ctx, franzv1.UpdateKafkaClusterRequest_builder{
		Name:       proto.String("east-1"),
		Labels:     map[string]string{"franz.placement/env": "staging"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}.Build()); err != nil {
		t.Fatalf("UpdateKafkaCluster: %v", err)
	}
	lost := recvPartition(t, stream)
	if lost.GetChange() != franzv1.PartitionAssignment_CHANGE_REMOVED ||
		lost.GetReason() != franzv1.PartitionAssignment_REASON_SCOPE_LOSS {
		t.Fatalf("scope loss = %+v, want REMOVED/SCOPE_LOSS", lost)
	}
	if lost.GetPartitions() != 0 || len(lost.GetDesiredConfig()) != 0 {
		t.Errorf("a scope-loss REMOVED must carry no desired state: %+v", lost)
	}

	// --- and the partition is gone from a freshly-opened stream -------------
	resyncCtx, cancelResync := context.WithCancel(authCtx)
	defer cancelResync()
	resync, err := f.resource.WatchPartitionAssignments(resyncCtx,
		franzv1.WatchPartitionAssignmentsRequest_builder{}.Build())
	if err != nil {
		t.Fatal(err)
	}
	assertNoPartition(t, resync)

	// --- ownership: an agent out of scope cannot report ---------------------
	other, err := f.agents.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
		Name:   proto.String("gregor-samsa-other"),
		Type:   franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER.Enum(),
		Labels: map[string]string{"franz.placement-selector/env": "qa"},
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	otherCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+other.GetToken())
	_, err = f.resource.ReportPartitionReconciliation(otherCtx, reportRequest(
		partitionFRN, generation, franzv1.ReconciliationOutcome_RECONCILIATION_OUTCOME_CREATED, ""))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("out-of-scope report = %v, want PERMISSION_DENIED", err)
	}

	// --- auth: no token, and a rotated token -------------------------------
	noTokenStream, err := f.resource.WatchPartitionAssignments(ctx,
		franzv1.WatchPartitionAssignmentsRequest_builder{}.Build())
	if err == nil {
		_, err = noTokenStream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token = %v, want UNAUTHENTICATED", err)
	}
	if _, err := f.agents.RotateAgentToken(ctx, franzv1.RotateAgentTokenRequest_builder{
		Name: proto.String("gregor-samsa-prod"),
	}.Build()); err != nil {
		t.Fatalf("RotateAgentToken: %v", err)
	}
	staleTokenStream, err := f.resource.WatchPartitionAssignments(
		metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token),
		franzv1.WatchPartitionAssignmentsRequest_builder{}.Build())
	if err == nil {
		_, err = staleTokenStream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("rotated token = %v, want UNAUTHENTICATED", err)
	}
}

// The telemetry ingest path is agent-authenticated and appends to the 30-day
// series — the store the 005 Part 2 sweep feeds.
func TestTelemetryIngestE2E(t *testing.T) {
	f := newResourceProviderFixture(t)
	ctx := context.Background()

	created, err := f.agents.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
		Name: proto.String("gregor-samsa-prod"),
		Type: franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER.Enum(),
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+created.GetToken())

	samples := []*franzv1.IndicatorSample{
		franzv1.IndicatorSample_builder{
			Indicator:      proto.String("kafka.cluster.broker_count"),
			ResourceFrn:    proto.String("frn:default:kafka-cluster:east-1"),
			ResourceEntity: franzv1.Entity_ENTITY_KAFKA_CLUSTER.Enum(),
			Value:          proto.String("3"),
			SampleAt:       timestamppb.New(time.Now()),
		}.Build(),
		franzv1.IndicatorSample_builder{
			Indicator:      proto.String("kafka.topic.state"),
			ResourceFrn:    proto.String("frn:default:kafka-topic:billing-events-0"),
			ResourceEntity: franzv1.Entity_ENTITY_KAFKA_TOPIC.Enum(),
			Value:          proto.String("provisioned"),
			SampleAt:       timestamppb.New(time.Now()),
		}.Build(),
	}

	resp, err := f.tele.PublishIndicatorSamples(authCtx, franzv1.PublishIndicatorSamplesRequest_builder{
		Agent: proto.String("gregor-samsa-prod"), Samples: samples,
	}.Build())
	if err != nil {
		t.Fatalf("PublishIndicatorSamples: %v", err)
	}
	if resp.GetAccepted() != 2 {
		t.Fatalf("accepted = %d, want 2", resp.GetAccepted())
	}

	// The client-streaming form accepts batch after batch and totals on close.
	stream, err := f.tele.StreamIndicatorSamples(authCtx)
	if err != nil {
		t.Fatalf("StreamIndicatorSamples: %v", err)
	}
	for range 2 {
		if err := stream.Send(franzv1.StreamIndicatorSamplesRequest_builder{
			Agent: proto.String("gregor-samsa-prod"), Samples: samples,
		}.Build()); err != nil {
			t.Fatalf("stream.Send: %v", err)
		}
	}
	streamResp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("stream.CloseAndRecv: %v", err)
	}
	if streamResp.GetAccepted() != 4 {
		t.Fatalf("stream accepted = %d, want 4", streamResp.GetAccepted())
	}

	var total int
	var reportingAgent string
	if err := f.db.Pool().QueryRow(ctx,
		`SELECT count(*), max(reporting_agent) FROM indicator_sample
		 WHERE indicator='kafka.cluster.broker_count'`).Scan(&total, &reportingAgent); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("stored broker_count samples = %d, want 3", total)
	}
	// The authenticated identity wins over whatever the request claimed.
	if reportingAgent != "gregor-samsa-prod" {
		t.Fatalf("reporting_agent = %q", reportingAgent)
	}

	// TelemetryService is agent-only: an unauthenticated call is rejected.
	_, err = f.tele.PublishIndicatorSamples(ctx, franzv1.PublishIndicatorSamplesRequest_builder{
		Samples: samples,
	}.Build())
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token = %v, want UNAUTHENTICATED", err)
	}
}

// --- helpers --------------------------------------------------------------

func reportRequest(
	partitionFRN string, generation int64, outcome franzv1.ReconciliationOutcome, message string,
) *franzv1.ReportPartitionReconciliationRequest {
	return franzv1.ReportPartitionReconciliationRequest_builder{
		Report: franzv1.PartitionReconciliationReport_builder{
			PartitionFrn: proto.String(partitionFRN),
			Generation:   proto.Int64(generation),
			Outcome:      outcome.Enum(),
			Message:      proto.String(message),
			AppliedConfig: franzv1.AppliedTopicState_builder{
				Partitions:        proto.Int32(3),
				ReplicationFactor: proto.Int32(1),
				Config:            map[string]string{"retention.ms": "604800000"},
			}.Build(),
		}.Build(),
	}.Build()
}

func recvPartition(
	t *testing.T, s grpc.ServerStreamingClient[franzv1.WatchPartitionAssignmentsResponse],
) *franzv1.PartitionAssignment {
	t.Helper()
	type result struct {
		msg *franzv1.WatchPartitionAssignmentsResponse
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
			t.Fatalf("stream.Recv: %v", r.err)
		}
		return r.msg.GetAssignment()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a partition assignment")
		return nil
	}
}

// assertNoPartition fails if the stream delivers an assignment within a short
// window — used to prove a partition left the agent's scope entirely.
func assertNoPartition(
	t *testing.T, s grpc.ServerStreamingClient[franzv1.WatchPartitionAssignmentsResponse],
) {
	t.Helper()
	ch := make(chan *franzv1.PartitionAssignment, 1)
	go func() {
		if m, err := s.Recv(); err == nil {
			ch <- m.GetAssignment()
		}
	}()
	select {
	case a := <-ch:
		t.Fatalf("stream delivered %+v; the agent should have no in-scope partitions", a)
	case <-time.After(750 * time.Millisecond):
	}
}
