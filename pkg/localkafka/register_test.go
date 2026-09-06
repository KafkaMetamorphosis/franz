package localkafka

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// stubAgentService is a minimal AgentService for exercising EnsureRegistered
// without a full Franz + Postgres stack.
type stubAgentService struct {
	franzv1.UnimplementedAgentServiceServer

	existing *franzv1.Agent // nil ⇒ GetAgent returns NotFound
	created  []string
	rotated  []string
}

func (s *stubAgentService) GetAgent(_ context.Context, req *franzv1.GetAgentRequest) (*franzv1.GetAgentResponse, error) {
	if s.existing == nil {
		return nil, status.Errorf(codes.NotFound, "agent %q not found", req.GetName())
	}
	return franzv1.GetAgentResponse_builder{Agent: s.existing}.Build(), nil
}

func (s *stubAgentService) CreateAgent(_ context.Context, req *franzv1.CreateAgentRequest) (*franzv1.CreateAgentResponse, error) {
	s.created = append(s.created, req.GetName())
	return franzv1.CreateAgentResponse_builder{Token: strptr("frnat_created")}.Build(), nil
}

func (s *stubAgentService) RotateAgentToken(_ context.Context, req *franzv1.RotateAgentTokenRequest) (*franzv1.RotateAgentTokenResponse, error) {
	s.rotated = append(s.rotated, req.GetName())
	return franzv1.RotateAgentTokenResponse_builder{Token: strptr("frnat_rotated")}.Build(), nil
}

func dialStub(t *testing.T, svc franzv1.AgentServiceServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	franzv1.RegisterAgentServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestEnsureRegisteredCreatesWhenMissing(t *testing.T) {
	svc := &stubAgentService{existing: nil}
	token, created, err := EnsureRegistered(context.Background(), dialStub(t, svc), "local-kafka-agent")
	if err != nil {
		t.Fatal(err)
	}
	if !created || token != "frnat_created" {
		t.Fatalf("want created token, got created=%v token=%q", created, token)
	}
	if len(svc.created) != 1 || svc.created[0] != "local-kafka-agent" {
		t.Fatalf("CreateAgent not called as expected: %v", svc.created)
	}
}

func TestEnsureRegisteredRotatesWhenPresent(t *testing.T) {
	svc := &stubAgentService{existing: franzv1.Agent_builder{
		Name:   strptr("local-kafka-agent"),
		Status: franzv1.AgentStatus_AGENT_STATUS_ACTIVE.Enum(),
	}.Build()}
	token, created, err := EnsureRegistered(context.Background(), dialStub(t, svc), "local-kafka-agent")
	if err != nil {
		t.Fatal(err)
	}
	if created || token != "frnat_rotated" {
		t.Fatalf("want rotated token, got created=%v token=%q", created, token)
	}
	if len(svc.rotated) != 1 {
		t.Fatalf("RotateAgentToken not called: %v", svc.rotated)
	}
}

func TestEnsureRegisteredRejectsDeleted(t *testing.T) {
	svc := &stubAgentService{existing: franzv1.Agent_builder{
		Name:   strptr("local-kafka-agent"),
		Status: franzv1.AgentStatus_AGENT_STATUS_DELETED.Enum(),
	}.Build()}
	if _, _, err := EnsureRegistered(context.Background(), dialStub(t, svc), "local-kafka-agent"); err == nil {
		t.Fatal("expected an error for a deleted agent")
	}
}
