package grpcgateway_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/in/grpcgateway"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/agents"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/resourceprovider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/telemetry"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/internal/dbtest"
)

const (
	brokerCountIndicator = "kafka.cluster.broker_count"
	topicStateIndicator  = "kafka.topic.state"
	eastCluster          = "frn:default:kafka-cluster:east-1"
)

// evalCall is one ingest→eval hook firing.
type evalCall struct {
	indicator   string
	resourceFRN string
	value       string
}

// recordingEvaluator stands in for the real governance pass so a test can assert
// that the hook fired — and, for an out-of-order sample, that it did not. The
// pass itself is deliverable 14's; what deliverable 15 owns is *when* it runs.
type recordingEvaluator struct {
	mu    sync.Mutex
	calls []evalCall
}

func (e *recordingEvaluator) Evaluate(_ context.Context, name, resourceFRN, value string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls,
		evalCall{indicator: name, resourceFRN: resourceFRN, value: value})
	return nil
}

func (e *recordingEvaluator) snapshot() []evalCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]evalCall(nil), e.calls...)
}

// telemetryFixture is the whole ingest wire over bufconn: Postgres, the
// Indicator registry behind GovernanceService, the agent-auth interceptor, and
// the eval hook.
type telemetryFixture struct {
	db         *postgres.DB
	realmID    uuid.UUID
	agents     franzv1.AgentServiceClient
	governance franzv1.GovernanceServiceClient
	tele       franzv1.TelemetryServiceClient
	evaluator  *recordingEvaluator
}

func newTelemetryFixture(t *testing.T) *telemetryFixture {
	t.Helper()
	dsn := os.Getenv("FRANZ_TEST_DB_DSN")
	if dsn == "" {
		t.Skip("set FRANZ_TEST_DB_DSN to run the telemetry-ingest e2e test")
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
		"policy_action", "policy", "indicator_sample", "indicator",
		"observed_consumer_group", "kafka_topic", "async_channel",
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
	agentRepo := postgres.NewAgentRepo(db)
	clusterRepo := postgres.NewClusterRepo(db)
	topicRepo := postgres.NewTopicRepo(db)
	channelRepo := postgres.NewChannelRepo(db)
	sampleRepo := postgres.NewIndicatorSampleRepo(db)
	indicatorRepo := postgres.NewIndicatorRepo(db)
	policyRepo := postgres.NewPolicyRepo(db)
	actionRepo := postgres.NewPolicyActionRepo(db)
	groupRepo := postgres.NewObservedConsumerGroupRepo(db)
	codec := frn.MustCodec("frn")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	evaluator := &recordingEvaluator{}
	// The notifier is only here because AgentService takes one; no agent ever
	// opens a partition stream on this wire.
	agentSvc := agents.NewService(agentRepo, resourceprovider.NewNotifier(
		agentRepo, clusterRepo, topicRepo, streamhub.New(), log))
	governanceSvc := governance.NewService(policyRepo, indicatorRepo, actionRepo,
		sampleRepo, channelRepo, clusterRepo, topicRepo)
	telemetrySvc := telemetry.NewService(
		sampleRepo, indicatorRepo, groupRepo, evaluator, log)

	srv := grpcgateway.New(0, 0, log,
		grpcgateway.WithAuthenticator(grpcgateway.NewAuthenticator(realmRepo)),
		grpcgateway.WithAgentAuth(grpcgateway.NewAgentAuthenticator(agentRepo)),
	)
	if err := grpcgateway.RegisterAgentService(srv, agentSvc, codec); err != nil {
		t.Fatal(err)
	}
	if err := grpcgateway.RegisterGovernanceService(srv, governanceSvc, codec); err != nil {
		t.Fatal(err)
	}
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

	return &telemetryFixture{
		db:         db,
		realmID:    realmID,
		agents:     franzv1.NewAgentServiceClient(conn),
		governance: franzv1.NewGovernanceServiceClient(conn),
		tele:       franzv1.NewTelemetryServiceClient(conn),
		evaluator:  evaluator,
	}
}

// registerAgent creates an agent of the given type and returns a context
// carrying its bearer token.
func (f *telemetryFixture) registerAgent(
	t *testing.T, name string, typ franzv1.AgentType,
) context.Context {
	t.Helper()
	created, err := f.agents.CreateAgent(context.Background(), franzv1.CreateAgentRequest_builder{
		Name: proto.String(name), Type: typ.Enum(),
	}.Build())
	if err != nil {
		t.Fatalf("CreateAgent %q: %v", name, err)
	}
	return metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "Bearer "+created.GetToken())
}

func (f *telemetryFixture) registerIndicator(
	t *testing.T, name, unit string, appliesTo franzv1.Entity, staleness string,
) {
	t.Helper()
	if _, err := f.governance.CreateIndicator(context.Background(),
		franzv1.CreateIndicatorRequest_builder{
			Name:               proto.String(name),
			Unit:               proto.String(unit),
			AppliesTo:          appliesTo.Enum(),
			StalenessThreshold: proto.String(staleness),
		}.Build()); err != nil {
		t.Fatalf("CreateIndicator %q: %v", name, err)
	}
}

func clusterSample(name, value string, at time.Time) *franzv1.IndicatorSample {
	return franzv1.IndicatorSample_builder{
		Indicator:      proto.String(name),
		ResourceFrn:    proto.String(eastCluster),
		ResourceEntity: franzv1.Entity_ENTITY_KAFKA_CLUSTER.Enum(),
		Value:          proto.String(value),
		SampleAt:       timestamppb.New(at),
	}.Build()
}

// The "Done when" walk-through: an unregistered indicator is refused, and
// registering it lets the very same batch through.
func TestPublishIndicatorSamplesRequiresRegistration(t *testing.T) {
	f := newTelemetryFixture(t)
	authCtx := f.registerAgent(t, "gregor-samsa-prod",
		franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER)
	batch := franzv1.PublishIndicatorSamplesRequest_builder{
		Samples: []*franzv1.IndicatorSample{clusterSample(brokerCountIndicator, "3", time.Now())},
	}.Build()

	_, err := f.tele.PublishIndicatorSamples(authCtx, batch)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unregistered indicator = %v, want FAILED_PRECONDITION", err)
	}

	f.registerIndicator(t, brokerCountIndicator, "count",
		franzv1.Entity_ENTITY_KAFKA_CLUSTER, "1h")

	resp, err := f.tele.PublishIndicatorSamples(authCtx, batch)
	if err != nil {
		t.Fatalf("PublishIndicatorSamples after registration: %v", err)
	}
	if resp.GetAccepted() != 1 {
		t.Fatalf("accepted = %d, want 1", resp.GetAccepted())
	}
}

// The three per-sample rules of 003.14, over the wire, each rejecting the whole
// call.
func TestPublishIndicatorSamplesValidation(t *testing.T) {
	f := newTelemetryFixture(t)
	authCtx := f.registerAgent(t, "gregor-samsa-prod",
		franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER)
	f.registerIndicator(t, brokerCountIndicator, "count",
		franzv1.Entity_ENTITY_KAFKA_CLUSTER, "1h")

	entityMismatch := clusterSample(brokerCountIndicator, "3", time.Now())
	entityMismatch.SetResourceEntity(franzv1.Entity_ENTITY_KAFKA_TOPIC)

	tests := []struct {
		name   string
		sample *franzv1.IndicatorSample
		want   codes.Code
	}{
		{"unregistered indicator", clusterSample("kafka.cluster.ghost", "3", time.Now()),
			codes.FailedPrecondition},
		{"resource_entity disagrees with applies_to", entityMismatch, codes.InvalidArgument},
		{"value does not parse in the unit",
			clusterSample(brokerCountIndicator, "three", time.Now()), codes.InvalidArgument},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A good sample rides along to prove the whole call is rejected, not
			// just the offending row.
			_, err := f.tele.PublishIndicatorSamples(authCtx,
				franzv1.PublishIndicatorSamplesRequest_builder{
					Samples: []*franzv1.IndicatorSample{
						clusterSample(brokerCountIndicator, "3", time.Now()),
						tc.sample,
					},
				}.Build())
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v (err %v), want %v", status.Code(err), err, tc.want)
			}
		})
	}

	var stored int
	if err := f.db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM indicator_sample`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 0 {
		t.Fatalf("%d rows landed from rejected batches, want 0", stored)
	}
}

// 15.5: the client stream routes through the same service method, so it gets
// validation, current-value maintenance and the eval hook without a handler of
// its own.
func TestStreamIndicatorSamplesSharesTheIngestPath(t *testing.T) {
	f := newTelemetryFixture(t)
	authCtx := f.registerAgent(t, "gregor-samsa-prod",
		franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER)
	f.registerIndicator(t, brokerCountIndicator, "count",
		franzv1.Entity_ENTITY_KAFKA_CLUSTER, "1h")
	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	stream, err := f.tele.StreamIndicatorSamples(authCtx)
	if err != nil {
		t.Fatalf("StreamIndicatorSamples: %v", err)
	}
	for i := range 3 {
		if err := stream.Send(franzv1.StreamIndicatorSamplesRequest_builder{
			Samples: []*franzv1.IndicatorSample{
				clusterSample(brokerCountIndicator, "3", base.Add(time.Duration(i)*time.Second)),
			},
		}.Build()); err != nil {
			t.Fatalf("stream.Send: %v", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("stream.CloseAndRecv: %v", err)
	}
	if resp.GetAccepted() != 3 {
		t.Fatalf("accepted = %d, want 3", resp.GetAccepted())
	}
	if got := len(f.evaluator.snapshot()); got != 3 {
		t.Errorf("evaluations = %d, want one per advancing sample", got)
	}

	// A batch that fails validation ends the stream rather than being dropped.
	bad, err := f.tele.StreamIndicatorSamples(authCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.Send(franzv1.StreamIndicatorSamplesRequest_builder{
		Samples: []*franzv1.IndicatorSample{clusterSample("kafka.cluster.ghost", "1", time.Now())},
	}.Build()); err != nil {
		t.Fatalf("stream.Send: %v", err)
	}
	if _, err := bad.CloseAndRecv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stream error = %v, want FAILED_PRECONDITION", err)
	}
}

// 003.14's current-value, out-of-order and staleness rules end to end.
func TestIngestMaintainsCurrentValueAndHealth(t *testing.T) {
	f := newTelemetryFixture(t)
	ctx := context.Background()
	authCtx := f.registerAgent(t, "gregor-samsa-prod",
		franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER)
	f.registerIndicator(t, brokerCountIndicator, "count",
		franzv1.Entity_ENTITY_KAFKA_CLUSTER, "1h")

	// A registered indicator that has never been sampled is STALE, not HEALTHY:
	// there is no value for a policy to compare.
	got, err := f.governance.GetIndicator(ctx,
		franzv1.GetIndicatorRequest_builder{Name: proto.String(brokerCountIndicator)}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetIndicator().GetHealth() != franzv1.IndicatorHealth_INDICATOR_HEALTH_STALE {
		t.Fatalf("unsampled health = %v, want STALE", got.GetIndicator().GetHealth())
	}

	now := time.Now().Truncate(time.Millisecond)
	if _, err := f.tele.PublishIndicatorSamples(authCtx,
		franzv1.PublishIndicatorSamplesRequest_builder{
			Samples: []*franzv1.IndicatorSample{clusterSample(brokerCountIndicator, "3", now)},
		}.Build()); err != nil {
		t.Fatalf("PublishIndicatorSamples: %v", err)
	}

	got, err = f.governance.GetIndicator(ctx,
		franzv1.GetIndicatorRequest_builder{Name: proto.String(brokerCountIndicator)}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetIndicator().GetHealth() != franzv1.IndicatorHealth_INDICATOR_HEALTH_HEALTHY {
		t.Fatalf("health after a fresh sample = %v, want HEALTHY", got.GetIndicator().GetHealth())
	}
	if !got.GetIndicator().GetLastSampleAt().AsTime().Equal(now.UTC()) {
		t.Errorf("last_sample_at = %v, want %v",
			got.GetIndicator().GetLastSampleAt().AsTime(), now.UTC())
	}
	if calls := f.evaluator.snapshot(); len(calls) != 1 || calls[0].value != "3" {
		t.Fatalf("evaluations = %+v, want one carrying the new value", calls)
	}

	// Out-of-order: stored as history, not current, and no second evaluation.
	if _, err := f.tele.PublishIndicatorSamples(authCtx,
		franzv1.PublishIndicatorSamplesRequest_builder{
			Samples: []*franzv1.IndicatorSample{
				clusterSample(brokerCountIndicator, "1", now.Add(-time.Minute)),
			},
		}.Build()); err != nil {
		t.Fatalf("a late sample is history, not an error: %v", err)
	}
	var stored int
	var currentValue string
	if err := f.db.Pool().QueryRow(ctx,
		`SELECT (SELECT count(*) FROM indicator_sample WHERE indicator=$1),
		        (SELECT current_value FROM indicator WHERE name=$1)`,
		brokerCountIndicator).Scan(&stored, &currentValue); err != nil {
		t.Fatal(err)
	}
	if stored != 2 {
		t.Errorf("stored samples = %d, want the late one kept as history", stored)
	}
	if currentValue != "3" {
		t.Errorf("current_value = %q, want the newer sample's 3", currentValue)
	}
	if got := len(f.evaluator.snapshot()); got != 1 {
		t.Errorf("evaluations = %d, want no second one", got)
	}

	// Past the staleness threshold with no sample, health reads STALE. The clock
	// is fast-forwarded by ageing last_sample_at rather than by waiting an hour.
	if _, err := f.db.Pool().Exec(ctx,
		`UPDATE indicator SET last_sample_at = now() - interval '2 hours' WHERE name=$1`,
		brokerCountIndicator); err != nil {
		t.Fatal(err)
	}
	got, err = f.governance.GetIndicator(ctx,
		franzv1.GetIndicatorRequest_builder{Name: proto.String(brokerCountIndicator)}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetIndicator().GetHealth() != franzv1.IndicatorHealth_INDICATOR_HEALTH_STALE {
		t.Fatalf("health past the threshold = %v, want STALE", got.GetIndicator().GetHealth())
	}
}

// 005 ADR §2.1 registers `kafka.topic.state` as an enum; Gregor Samsa's
// structural sweep has to be publishable against the registry.
func TestPublishAcceptsCategoricalIndicator(t *testing.T) {
	f := newTelemetryFixture(t)
	authCtx := f.registerAgent(t, "gregor-samsa-prod",
		franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER)
	f.registerIndicator(t, topicStateIndicator, "enum",
		franzv1.Entity_ENTITY_KAFKA_TOPIC, "1h")

	resp, err := f.tele.PublishIndicatorSamples(authCtx,
		franzv1.PublishIndicatorSamplesRequest_builder{
			Samples: []*franzv1.IndicatorSample{
				franzv1.IndicatorSample_builder{
					Indicator:      proto.String(topicStateIndicator),
					ResourceFrn:    proto.String("frn:default:kafka-topic:billing-events-0"),
					ResourceEntity: franzv1.Entity_ENTITY_KAFKA_TOPIC.Enum(),
					Value:          proto.String("provisioned"),
					SampleAt:       timestamppb.New(time.Now()),
				}.Build(),
			},
		}.Build())
	if err != nil {
		t.Fatalf("PublishIndicatorSamples: %v", err)
	}
	if resp.GetAccepted() != 1 {
		t.Fatalf("accepted = %d, want 1", resp.GetAccepted())
	}
}

// ReportConsumerGroups end to end: the sightings land, `custom` is derived by
// Franz, and the row is attributed to the authenticated agent.
func TestReportConsumerGroupsE2E(t *testing.T) {
	f := newTelemetryFixture(t)
	ctx := context.Background()
	authCtx := f.registerAgent(t, "odradek-prod", franzv1.AgentType_AGENT_TYPE_TELEMETRY_AGENT)
	observed := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	resp, err := f.tele.ReportConsumerGroups(authCtx,
		franzv1.ReportConsumerGroupsRequest_builder{
			// The request's `agent` field is ignored: the bearer token is
			// authoritative, so an agent cannot attribute sightings to another.
			Agent: proto.String("someone-else"),
			Observations: []*franzv1.ConsumerGroupObservation{
				franzv1.ConsumerGroupObservation_builder{
					Group:        proto.String("billing.billing-events-0"),
					ClientFrn:    proto.String("frn:default:client:billing"),
					Owner:        proto.String("payments-team"),
					AsyncChannel: proto.String("billing-events"),
					KafkaTopic:   proto.String("billing-events-0"),
					ObservedAt:   timestamppb.New(observed),
				}.Build(),
				franzv1.ConsumerGroupObservation_builder{
					Group:      proto.String("legacy-batch-reader"),
					ClientFrn:  proto.String("frn:default:client:billing"),
					KafkaTopic: proto.String("billing-events-0"),
					ObservedAt: timestamppb.New(observed),
				}.Build(),
			},
		}.Build())
	if err != nil {
		t.Fatalf("ReportConsumerGroups: %v", err)
	}
	if resp.GetAccepted() != 2 {
		t.Fatalf("accepted = %d, want 2", resp.GetAccepted())
	}

	rows, err := f.db.Pool().Query(ctx,
		`SELECT group_name, custom, reported_by_agent, client_frn
		 FROM observed_consumer_group ORDER BY group_name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type row struct {
		group  string
		custom bool
		agent  string
		client string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.group, &r.custom, &r.agent, &r.client); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("stored %d rows, want 2", len(got))
	}
	if got[0].group != "billing.billing-events-0" || got[0].custom {
		t.Errorf("the default <client>.<topic> form must not be custom: %+v", got[0])
	}
	if got[1].group != "legacy-batch-reader" || !got[1].custom {
		t.Errorf("an operator-chosen name must be custom: %+v", got[1])
	}
	for _, r := range got {
		if r.agent != "odradek-prod" {
			t.Errorf("reported_by_agent = %q, want the authenticated identity", r.agent)
		}
	}

	// A blank group is rejected and nothing partial lands.
	if _, err := f.tele.ReportConsumerGroups(authCtx,
		franzv1.ReportConsumerGroupsRequest_builder{
			Observations: []*franzv1.ConsumerGroupObservation{
				franzv1.ConsumerGroupObservation_builder{
					KafkaTopic: proto.String("billing-events-0"),
					ObservedAt: timestamppb.New(observed),
				}.Build(),
			},
		}.Build()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("blank group = %v, want INVALID_ARGUMENT", err)
	}
}

// 15.4: TelemetryService is agent-only, and `Agent.type` is organisational
// (003.9) — a RESOURCE_PROVIDER and a TELEMETRY_AGENT both authenticate, while
// no token at all does not. The finer per-type policy is the agent-auth ADR's.
func TestTelemetryServiceAcceptsEveryAgentType(t *testing.T) {
	f := newTelemetryFixture(t)
	f.registerIndicator(t, brokerCountIndicator, "count",
		franzv1.Entity_ENTITY_KAFKA_CLUSTER, "1h")

	for _, tc := range []struct {
		name string
		typ  franzv1.AgentType
	}{
		{"gregor-samsa-prod", franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER},
		{"odradek-prod", franzv1.AgentType_AGENT_TYPE_TELEMETRY_AGENT},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			authCtx := f.registerAgent(t, tc.name, tc.typ)

			if _, err := f.tele.PublishIndicatorSamples(authCtx,
				franzv1.PublishIndicatorSamplesRequest_builder{
					Samples: []*franzv1.IndicatorSample{
						clusterSample(brokerCountIndicator, "3", time.Now()),
					},
				}.Build()); err != nil {
				t.Fatalf("PublishIndicatorSamples: %v", err)
			}

			// The stream interceptor covers the service too.
			stream, err := f.tele.StreamIndicatorSamples(authCtx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.CloseAndRecv(); err != nil {
				t.Fatalf("StreamIndicatorSamples: %v", err)
			}

			if _, err := f.tele.ReportConsumerGroups(authCtx,
				franzv1.ReportConsumerGroupsRequest_builder{
					Observations: []*franzv1.ConsumerGroupObservation{
						franzv1.ConsumerGroupObservation_builder{
							Group:      proto.String("billing.orders-0"),
							ClientFrn:  proto.String("frn:default:client:billing"),
							KafkaTopic: proto.String("orders-0"),
							ObservedAt: timestamppb.New(time.Now()),
						}.Build(),
					},
				}.Build()); err != nil {
				t.Fatalf("ReportConsumerGroups: %v", err)
			}
		})
	}

	anon := context.Background()
	if _, err := f.tele.PublishIndicatorSamples(anon,
		franzv1.PublishIndicatorSamplesRequest_builder{}.Build()); status.Code(err) != codes.Unauthenticated {
		t.Errorf("unauthenticated publish = %v, want UNAUTHENTICATED", err)
	}
	if _, err := f.tele.ReportConsumerGroups(anon,
		franzv1.ReportConsumerGroupsRequest_builder{}.Build()); status.Code(err) != codes.Unauthenticated {
		t.Errorf("unauthenticated report = %v, want UNAUTHENTICATED", err)
	}
	stream, err := f.tele.StreamIndicatorSamples(anon)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Unauthenticated {
		t.Errorf("unauthenticated stream = %v, want UNAUTHENTICATED", err)
	}
}
