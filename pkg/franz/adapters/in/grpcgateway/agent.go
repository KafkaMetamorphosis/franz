package grpcgateway

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/fieldmask"
)

// agentHandler adapts AgentService onto the generated gRPC server interface.
type agentHandler struct {
	franzv1.UnimplementedAgentServiceServer
	svc   in.AgentService
	codec frn.Codec
}

// RegisterAgentService mounts the AgentService on the gRPC server and the
// in-process REST gateway.
func RegisterAgentService(s *Server, svc in.AgentService, codec frn.Codec) error {
	h := &agentHandler{svc: svc, codec: codec}
	franzv1.RegisterAgentServiceServer(s.grpc, h)
	return franzv1.RegisterAgentServiceHandlerServer(context.Background(), s.gw, h)
}

func (h *agentHandler) CreateAgent(
	ctx context.Context, req *franzv1.CreateAgentRequest,
) (*franzv1.CreateAgentResponse, error) {
	created, err := h.svc.Create(ctx, in.CreateAgentInput{
		Name:               req.GetName(),
		Type:               agentTypeFromProto(req.GetType()),
		Labels:             req.GetLabels(),
		ProvisioningLabels: provisioningLabelsFromProto(req.GetProvisioningLabels()),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.CreateAgentResponse_builder{
		Agent: h.toProto(created.Agent),
		Token: proto.String(created.Token),
	}.Build(), nil
}

func (h *agentHandler) GetAgent(
	ctx context.Context, req *franzv1.GetAgentRequest,
) (*franzv1.GetAgentResponse, error) {
	a, err := h.svc.Get(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetAgentResponse_builder{Agent: h.toProto(a)}.Build(), nil
}

func (h *agentHandler) ListAgents(
	ctx context.Context, req *franzv1.ListAgentsRequest,
) (*franzv1.ListAgentsResponse, error) {
	page, err := h.svc.List(ctx, in.ListAgentsInput{
		TypeFilter: agentTypeFromProto(req.GetType()),
		PageSize:   req.GetPage().GetPageSize(),
		PageToken:  req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.Agent, len(page.Agents))
	for i, a := range page.Agents {
		out[i] = h.toProto(a)
	}
	return franzv1.ListAgentsResponse_builder{
		Agents: out,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

func (h *agentHandler) UpdateAgent(
	ctx context.Context, req *franzv1.UpdateAgentRequest,
) (*franzv1.UpdateAgentResponse, error) {
	paths, err := fieldmask.CanonicalPaths(req.GetUpdateMask(), req)
	if err != nil {
		return nil, ToError(err)
	}
	input := in.UpdateAgentInput{Name: req.GetName()}
	for _, p := range paths {
		switch p {
		case "type":
			v := agentTypeFromProto(req.GetType())
			input.Type = &v
		case "labels":
			v := req.GetLabels()
			input.Labels = &v
		case "provisioning_labels":
			v := provisioningLabelsFromProto(req.GetProvisioningLabels())
			input.ProvisioningLabels = &v
		default:
			return nil, ToError(errs.InvalidField("update_mask", "field "+p+" is not updatable"))
		}
	}
	a, err := h.svc.Update(ctx, input)
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.UpdateAgentResponse_builder{Agent: h.toProto(a)}.Build(), nil
}

func (h *agentHandler) DeleteAgent(
	ctx context.Context, req *franzv1.DeleteAgentRequest,
) (*franzv1.DeleteAgentResponse, error) {
	if err := h.svc.Delete(ctx, req.GetName()); err != nil {
		return nil, ToError(err)
	}
	return franzv1.DeleteAgentResponse_builder{}.Build(), nil
}

func (h *agentHandler) PauseAgent(
	ctx context.Context, req *franzv1.PauseAgentRequest,
) (*franzv1.PauseAgentResponse, error) {
	a, err := h.svc.Pause(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.PauseAgentResponse_builder{Agent: h.toProto(a)}.Build(), nil
}

func (h *agentHandler) ResumeAgent(
	ctx context.Context, req *franzv1.ResumeAgentRequest,
) (*franzv1.ResumeAgentResponse, error) {
	a, err := h.svc.Resume(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.ResumeAgentResponse_builder{Agent: h.toProto(a)}.Build(), nil
}

func (h *agentHandler) RotateAgentToken(
	ctx context.Context, req *franzv1.RotateAgentTokenRequest,
) (*franzv1.RotateAgentTokenResponse, error) {
	tok, err := h.svc.RotateToken(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.RotateAgentTokenResponse_builder{Token: proto.String(tok)}.Build(), nil
}

// --- mapping helpers -----------------------------------------------------

func (h *agentHandler) toProto(a *agent.Agent) *franzv1.Agent {
	return franzv1.Agent_builder{
		Name:               proto.String(a.Name),
		Frn:                proto.String(h.codec.Render(a.FRN)),
		Type:               agentTypeToProto(a.Type),
		Labels:             a.Labels,
		ProvisioningLabels: provisioningLabelsToProto(a.ProvisioningLabels),
		Status:             agentStatusToProto(a.Status),
		CreatedAt:          timestamppb.New(a.CreatedAt),
		UpdatedAt:          timestamppb.New(a.UpdatedAt),
	}.Build()
}

func provisioningLabelsFromProto(in []*franzv1.ProvisioningLabelSpec) []agent.ProvisioningLabelSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]agent.ProvisioningLabelSpec, len(in))
	for i, s := range in {
		out[i] = agent.ProvisioningLabelSpec{
			Key:           s.GetKey(),
			Description:   s.GetDescription(),
			AllowedValues: s.GetAllowedValues(),
			DefaultValue:  s.GetDefaultValue(),
			Required:      s.GetRequired(),
		}
	}
	return out
}

func provisioningLabelsToProto(specs []agent.ProvisioningLabelSpec) []*franzv1.ProvisioningLabelSpec {
	if len(specs) == 0 {
		return nil
	}
	out := make([]*franzv1.ProvisioningLabelSpec, len(specs))
	for i, s := range specs {
		out[i] = franzv1.ProvisioningLabelSpec_builder{
			Key:           proto.String(s.Key),
			Description:   proto.String(s.Description),
			AllowedValues: s.AllowedValues,
			DefaultValue:  proto.String(s.DefaultValue),
			Required:      proto.Bool(s.Required),
		}.Build()
	}
	return out
}

func agentTypeFromProto(t franzv1.AgentType) agent.Type {
	switch t {
	case franzv1.AgentType_AGENT_TYPE_CLUSTER_PROVIDER:
		return agent.TypeClusterProvider
	case franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER:
		return agent.TypeResourceProvider
	case franzv1.AgentType_AGENT_TYPE_TELEMETRY_AGENT:
		return agent.TypeTelemetryAgent
	case franzv1.AgentType_AGENT_TYPE_CUSTOM:
		return agent.TypeCustom
	default:
		return "" // unspecified ⇒ no filter / domain rejects at create
	}
}

func agentTypeToProto(t agent.Type) *franzv1.AgentType {
	v := franzv1.AgentType_AGENT_TYPE_UNSPECIFIED
	switch t {
	case agent.TypeClusterProvider:
		v = franzv1.AgentType_AGENT_TYPE_CLUSTER_PROVIDER
	case agent.TypeResourceProvider:
		v = franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER
	case agent.TypeTelemetryAgent:
		v = franzv1.AgentType_AGENT_TYPE_TELEMETRY_AGENT
	case agent.TypeCustom:
		v = franzv1.AgentType_AGENT_TYPE_CUSTOM
	}
	return &v
}

func agentStatusToProto(s agent.Status) *franzv1.AgentStatus {
	v := franzv1.AgentStatus_AGENT_STATUS_UNSPECIFIED
	switch s {
	case agent.StatusActive:
		v = franzv1.AgentStatus_AGENT_STATUS_ACTIVE
	case agent.StatusPaused:
		v = franzv1.AgentStatus_AGENT_STATUS_PAUSED
	case agent.StatusDeleted:
		v = franzv1.AgentStatus_AGENT_STATUS_DELETED
	}
	return &v
}
