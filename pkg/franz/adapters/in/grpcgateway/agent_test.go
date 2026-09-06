package grpcgateway

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

type fakeAgentSvc struct {
	created  in.CreateAgentInput
	updated  in.UpdateAgentInput
	ret      *agent.Agent
	tok      string
	err      error
	listResp in.AgentPage
}

func (f *fakeAgentSvc) Create(_ context.Context, i in.CreateAgentInput) (in.CreatedAgent, error) {
	f.created = i
	return in.CreatedAgent{Agent: f.ret, Token: f.tok}, f.err
}
func (f *fakeAgentSvc) Get(context.Context, string) (*agent.Agent, error) { return f.ret, f.err }
func (f *fakeAgentSvc) List(context.Context, in.ListAgentsInput) (in.AgentPage, error) {
	return f.listResp, f.err
}
func (f *fakeAgentSvc) Update(_ context.Context, i in.UpdateAgentInput) (*agent.Agent, error) {
	f.updated = i
	return f.ret, f.err
}
func (f *fakeAgentSvc) Delete(context.Context, string) error                 { return f.err }
func (f *fakeAgentSvc) Pause(context.Context, string) (*agent.Agent, error)  { return f.ret, f.err }
func (f *fakeAgentSvc) Resume(context.Context, string) (*agent.Agent, error) { return f.ret, f.err }
func (f *fakeAgentSvc) RotateToken(context.Context, string) (string, error)  { return f.tok, f.err }

func sampleAgent(t *testing.T) *agent.Agent {
	t.Helper()
	a, err := agent.New(realm.Realm{Slug: "default"}, "prov-1", agent.TypeClusterProvider, map[string]string{"team": "infra"}, "hash")
	if err != nil {
		t.Fatal(err)
	}
	a.CreatedAt = time.Unix(1700000000, 0)
	a.UpdatedAt = time.Unix(1700000001, 0)
	return a
}

func newAgentHandler(svc in.AgentService, prefix string) *agentHandler {
	return &agentHandler{svc: svc, codec: frn.MustCodec(prefix)}
}

func TestCreateAgentReturnsTokenAndRendersFRN(t *testing.T) {
	fake := &fakeAgentSvc{ret: sampleAgent(t), tok: "frnat_secret"}
	h := newAgentHandler(fake, "acme")

	resp, err := h.CreateAgent(context.Background(), franzv1.CreateAgentRequest_builder{
		Name: proto.String("prov-1"),
		Type: franzv1.AgentType_AGENT_TYPE_CLUSTER_PROVIDER.Enum(),
	}.Build())
	if err != nil {
		t.Fatalf("CreateAgent: %v", err)
	}
	if resp.GetToken() != "frnat_secret" {
		t.Errorf("token = %q", resp.GetToken())
	}
	if resp.GetAgent().GetFrn() != "acme:default:agent:prov-1" {
		t.Errorf("frn = %q", resp.GetAgent().GetFrn())
	}
	if fake.created.Type != agent.TypeClusterProvider {
		t.Errorf("service saw type %q", fake.created.Type)
	}
}

func TestAgentErrorMapping(t *testing.T) {
	h := newAgentHandler(&fakeAgentSvc{err: errs.NotFoundf("agent %q not found", "x")}, "frn")
	_, err := h.GetAgent(context.Background(), franzv1.GetAgentRequest_builder{Name: proto.String("x")}.Build())
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v", status.Code(err))
	}
}

func TestUpdateAgentMaskForwarding(t *testing.T) {
	fake := &fakeAgentSvc{ret: sampleAgent(t)}
	h := newAgentHandler(fake, "frn")

	_, err := h.UpdateAgent(context.Background(), franzv1.UpdateAgentRequest_builder{
		Name:       proto.String("prov-1"),
		Type:       franzv1.AgentType_AGENT_TYPE_TELEMETRY_AGENT.Enum(),
		Labels:     map[string]string{"team": "obs"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"type"}},
	}.Build())
	if err != nil {
		t.Fatalf("UpdateAgent: %v", err)
	}
	if fake.updated.Type == nil || *fake.updated.Type != agent.TypeTelemetryAgent {
		t.Errorf("type not forwarded: %+v", fake.updated)
	}
	if fake.updated.Labels != nil {
		t.Error("labels forwarded despite not being masked")
	}
}

func TestRotateAgentToken(t *testing.T) {
	h := newAgentHandler(&fakeAgentSvc{tok: "frnat_new"}, "frn")
	resp, err := h.RotateAgentToken(context.Background(), franzv1.RotateAgentTokenRequest_builder{
		Name: proto.String("prov-1"),
	}.Build())
	if err != nil {
		t.Fatalf("RotateAgentToken: %v", err)
	}
	if resp.GetToken() != "frnat_new" {
		t.Errorf("token = %q", resp.GetToken())
	}
}
