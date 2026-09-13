package grpcgateway

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/fieldmask"
)

// governanceHandler adapts GovernanceService onto the generated gRPC server
// interface, mapping proto ⇄ domain and domain errors → gRPC status.
type governanceHandler struct {
	franzv1.UnimplementedGovernanceServiceServer
	svc   in.GovernanceService
	codec frn.Codec
	// clock supplies the instant an Indicator's derived `health` is evaluated at.
	// Injectable so a test can render a stale indicator deterministically.
	now func() time.Time
}

// RegisterGovernanceService mounts the GovernanceService on both the gRPC server
// and the in-process REST gateway.
func RegisterGovernanceService(s *Server, svc in.GovernanceService, codec frn.Codec) error {
	h := &governanceHandler{svc: svc, codec: codec, now: time.Now}
	franzv1.RegisterGovernanceServiceServer(s.grpc, h)
	return franzv1.RegisterGovernanceServiceHandlerServer(context.Background(), s.gw, h)
}

// --- Policy CRUD ---------------------------------------------------------

func (h *governanceHandler) CreatePolicy(
	ctx context.Context, req *franzv1.CreatePolicyRequest,
) (*franzv1.CreatePolicyResponse, error) {
	p, err := h.svc.CreatePolicy(ctx, in.CreatePolicyInput{
		Name: req.GetName(),
		Definition: gov.Definition{
			Indicator: req.GetIndicator(),
			Matcher:   matcherFromProto(req.GetMatcher()),
			Limit:     limitFromProto(req.GetLimit()),
			Actions:   actionsFromProto(req.GetActions()),
		},
		Weight:  req.GetWeight(),
		Enabled: req.GetEnabled(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.CreatePolicyResponse_builder{Policy: h.policyToProto(p)}.Build(), nil
}

func (h *governanceHandler) GetPolicy(
	ctx context.Context, req *franzv1.GetPolicyRequest,
) (*franzv1.GetPolicyResponse, error) {
	p, err := h.svc.GetPolicy(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetPolicyResponse_builder{Policy: h.policyToProto(p)}.Build(), nil
}

func (h *governanceHandler) ListPolicies(
	ctx context.Context, req *franzv1.ListPoliciesRequest,
) (*franzv1.ListPoliciesResponse, error) {
	page, err := h.svc.ListPolicies(ctx, in.ListPoliciesInput{
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	policies := make([]*franzv1.Policy, len(page.Policies))
	for i, p := range page.Policies {
		policies[i] = h.policyToProto(p)
	}
	return franzv1.ListPoliciesResponse_builder{
		Policies: policies,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

func (h *governanceHandler) UpdatePolicy(
	ctx context.Context, req *franzv1.UpdatePolicyRequest,
) (*franzv1.UpdatePolicyResponse, error) {
	paths, err := fieldmask.CanonicalPaths(req.GetUpdateMask(), req)
	if err != nil {
		return nil, ToError(err)
	}
	input := in.UpdatePolicyInput{Name: req.GetName()}
	for _, p := range paths {
		switch p {
		case "indicator":
			v := req.GetIndicator()
			input.Indicator = &v
		case "matcher":
			v := matcherFromProto(req.GetMatcher())
			input.Matcher = &v
		case "limit":
			v := limitFromProto(req.GetLimit())
			input.Limit = &v
		case "actions":
			v := actionsFromProto(req.GetActions())
			input.Actions = &v
		case "weight":
			v := req.GetWeight()
			input.Weight = &v
		case "enabled":
			v := req.GetEnabled()
			input.Enabled = &v
		default:
			return nil, ToError(errs.InvalidField("update_mask", "field "+p+" is not updatable"))
		}
	}
	policy, err := h.svc.UpdatePolicy(ctx, input)
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.UpdatePolicyResponse_builder{Policy: h.policyToProto(policy)}.Build(), nil
}

func (h *governanceHandler) DeletePolicy(
	ctx context.Context, req *franzv1.DeletePolicyRequest,
) (*franzv1.DeletePolicyResponse, error) {
	if err := h.svc.DeletePolicy(ctx, req.GetName()); err != nil {
		return nil, ToError(err)
	}
	return franzv1.DeletePolicyResponse_builder{}.Build(), nil
}

// DryRunPolicy evaluates an inline definition. `weight` and `enabled` are not on
// the request: they do not affect whether a policy would trigger (003.8).
func (h *governanceHandler) DryRunPolicy(
	ctx context.Context, req *franzv1.DryRunPolicyRequest,
) (*franzv1.DryRunPolicyResponse, error) {
	matches, err := h.svc.DryRunPolicy(ctx, gov.Definition{
		Indicator: req.GetIndicator(),
		Matcher:   matcherFromProto(req.GetMatcher()),
		Limit:     limitFromProto(req.GetLimit()),
		Actions:   actionsFromProto(req.GetActions()),
	})
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.DryRunPolicyResponse_Match, len(matches))
	for i, m := range matches {
		out[i] = franzv1.DryRunPolicyResponse_Match_builder{
			ResourceFrn:    proto.String(m.ResourceFRN),
			IndicatorValue: proto.String(m.IndicatorValue),
			WouldTrigger:   proto.Bool(m.WouldTrigger),
		}.Build()
	}
	return franzv1.DryRunPolicyResponse_builder{Matches: out}.Build(), nil
}

func (h *governanceHandler) ListPolicyActions(
	ctx context.Context, req *franzv1.ListPolicyActionsRequest,
) (*franzv1.ListPolicyActionsResponse, error) {
	page, err := h.svc.ListPolicyActions(ctx, in.ListPolicyActionsInput{
		PolicyName: req.GetName(),
		PageSize:   req.GetPage().GetPageSize(),
		PageToken:  req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	actions := make([]*franzv1.PolicyAction, len(page.Actions))
	for i, a := range page.Actions {
		actions[i] = franzv1.PolicyAction_builder{
			OccurredAt:     timestamppb.New(a.OccurredAt),
			ResourceFrn:    proto.String(h.renderStoredFRN(a.ResourceFRN)),
			IndicatorValue: proto.String(a.IndicatorValue),
			Action:         actionToProto(a.Action),
			Result:         proto.String(a.Result),
		}.Build()
	}
	return franzv1.ListPolicyActionsResponse_builder{
		Actions: actions,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0),
		}.Build(),
	}.Build(), nil
}

// --- Indicator registry --------------------------------------------------

func (h *governanceHandler) CreateIndicator(
	ctx context.Context, req *franzv1.CreateIndicatorRequest,
) (*franzv1.CreateIndicatorResponse, error) {
	i, err := h.svc.CreateIndicator(ctx, in.CreateIndicatorInput{
		Name:               req.GetName(),
		Unit:               indicator.Unit(req.GetUnit()),
		AppliesTo:          entityFromProto(req.GetAppliesTo()),
		StalenessThreshold: req.GetStalenessThreshold(),
		SourceAgents:       req.GetSourceAgents(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.CreateIndicatorResponse_builder{Indicator: h.indicatorToProto(i)}.Build(), nil
}

func (h *governanceHandler) GetIndicator(
	ctx context.Context, req *franzv1.GetIndicatorRequest,
) (*franzv1.GetIndicatorResponse, error) {
	i, err := h.svc.GetIndicator(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetIndicatorResponse_builder{Indicator: h.indicatorToProto(i)}.Build(), nil
}

func (h *governanceHandler) ListIndicators(
	ctx context.Context, req *franzv1.ListIndicatorsRequest,
) (*franzv1.ListIndicatorsResponse, error) {
	page, err := h.svc.ListIndicators(ctx, in.ListIndicatorsInput{
		PageSize:  req.GetPage().GetPageSize(),
		PageToken: req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	indicators := make([]*franzv1.Indicator, len(page.Indicators))
	for i, ind := range page.Indicators {
		indicators[i] = h.indicatorToProto(ind)
	}
	return franzv1.ListIndicatorsResponse_builder{
		Indicators: indicators,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0),
		}.Build(),
	}.Build(), nil
}

// UpdateIndicator applies the masked fields. `applies_to` is not a case here and
// is not on the request message: it is immutable once registered (003.14).
func (h *governanceHandler) UpdateIndicator(
	ctx context.Context, req *franzv1.UpdateIndicatorRequest,
) (*franzv1.UpdateIndicatorResponse, error) {
	paths, err := fieldmask.CanonicalPaths(req.GetUpdateMask(), req)
	if err != nil {
		return nil, ToError(err)
	}
	input := in.UpdateIndicatorInput{Name: req.GetName()}
	for _, p := range paths {
		switch p {
		case "unit":
			v := indicator.Unit(req.GetUnit())
			input.Unit = &v
		case "staleness_threshold":
			v := req.GetStalenessThreshold()
			input.StalenessThreshold = &v
		case "source_agents":
			v := req.GetSourceAgents()
			input.SourceAgents = &v
		default:
			return nil, ToError(errs.InvalidField("update_mask",
				"field "+p+" is not updatable (applies_to is immutable)"))
		}
	}
	i, err := h.svc.UpdateIndicator(ctx, input)
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.UpdateIndicatorResponse_builder{Indicator: h.indicatorToProto(i)}.Build(), nil
}

func (h *governanceHandler) DeleteIndicator(
	ctx context.Context, req *franzv1.DeleteIndicatorRequest,
) (*franzv1.DeleteIndicatorResponse, error) {
	if err := h.svc.DeleteIndicator(ctx, req.GetName()); err != nil {
		return nil, ToError(err)
	}
	return franzv1.DeleteIndicatorResponse_builder{}.Build(), nil
}

func (h *governanceHandler) ListIndicatorSamples(
	ctx context.Context, req *franzv1.ListIndicatorSamplesRequest,
) (*franzv1.ListIndicatorSamplesResponse, error) {
	page, err := h.svc.ListIndicatorSamples(ctx, in.ListIndicatorSamplesInput{
		Indicator:   req.GetIndicator(),
		ResourceFRN: req.GetResourceFrn(),
		From:        req.GetFrom().AsTime(),
		To:          req.GetTo().AsTime(),
		PageSize:    req.GetPage().GetPageSize(),
		PageToken:   req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	samples := make([]*franzv1.IndicatorSampleView, len(page.Samples))
	for i, s := range page.Samples {
		samples[i] = franzv1.IndicatorSampleView_builder{
			ResourceFrn:    proto.String(h.renderStoredFRN(s.ResourceFRN)),
			ResourceEntity: entityToProto(s.ResourceEntity),
			Value:          proto.String(s.Value),
			SampleAt:       timestamppb.New(s.SampleAt),
		}.Build()
	}
	return franzv1.ListIndicatorSamplesResponse_builder{
		Samples: samples,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0),
		}.Build(),
	}.Build(), nil
}

// --- mapping helpers -----------------------------------------------------

func (h *governanceHandler) policyToProto(p *gov.Policy) *franzv1.Policy {
	b := franzv1.Policy_builder{
		Name:      proto.String(p.Name),
		Frn:       proto.String(h.codec.Render(p.FRN)),
		Indicator: proto.String(p.Indicator),
		Matcher: franzv1.Matcher_builder{
			Entity:   entityToProto(p.Matcher.Entity),
			Selector: proto.String(p.Matcher.Selector),
		}.Build(),
		Limit: franzv1.Limit_builder{
			Operator: operatorToProto(p.Limit.Operator),
			Value:    proto.String(p.Limit.Value),
		}.Build(),
		Actions:   actionsToProto(p.Actions),
		Weight:    proto.Int32(p.Weight),
		Enabled:   proto.Bool(p.Enabled),
		CreatedAt: timestamppb.New(p.CreatedAt),
		UpdatedAt: timestamppb.New(p.UpdatedAt),
	}
	if p.LastFiredAt != nil {
		b.LastFiredAt = timestamppb.New(*p.LastFiredAt)
	}
	return b.Build()
}

// indicatorToProto renders an Indicator. `health` is derived here, at read time,
// from last_sample_at and staleness_threshold — it is never a stored column
// (003.14), so it cannot itself go stale.
func (h *governanceHandler) indicatorToProto(i *indicator.Indicator) *franzv1.Indicator {
	b := franzv1.Indicator_builder{
		Name:               proto.String(i.Name),
		Unit:               proto.String(string(i.Unit)),
		AppliesTo:          entityToProto(i.AppliesTo),
		SourceAgents:       i.SourceAgents,
		Health:             healthToProto(i.Health(h.now())),
		StalenessThreshold: proto.String(i.StalenessSpec),
	}
	if i.LastSampleAt != nil {
		b.LastSampleAt = timestamppb.New(*i.LastSampleAt)
	}
	return b.Build()
}

// renderStoredFRN applies the deployment's prefix to a stored, prefix-less FRN
// path. A sample may name a cluster sub-resource ("<cluster-frn>/broker/3"),
// which is not a parseable FRN, so an unparseable value is passed through as-is
// rather than dropped — the operator needs to see what was reported.
func (h *governanceHandler) renderStoredFRN(path string) string {
	f, err := frn.ParsePath(path)
	if err != nil {
		return path
	}
	return h.codec.Render(f)
}

func matcherFromProto(m *franzv1.Matcher) gov.Matcher {
	if m == nil {
		return gov.Matcher{}
	}
	return gov.Matcher{Entity: entityFromProto(m.GetEntity()), Selector: m.GetSelector()}
}

func limitFromProto(l *franzv1.Limit) gov.Limit {
	if l == nil {
		return gov.Limit{}
	}
	return gov.Limit{Operator: operatorFromProto(l.GetOperator()), Value: l.GetValue()}
}

func actionsFromProto(actions []*franzv1.Action) []gov.Action {
	out := make([]gov.Action, 0, len(actions))
	for _, a := range actions {
		out = append(out, gov.Action{
			Kind: actionKindFromProto(a.GetKind()),
			Args: a.GetArgs(),
		})
	}
	return out
}

func actionsToProto(actions []gov.Action) []*franzv1.Action {
	out := make([]*franzv1.Action, len(actions))
	for i, a := range actions {
		out[i] = actionToProto(a)
	}
	return out
}

func actionToProto(a gov.Action) *franzv1.Action {
	return franzv1.Action_builder{
		Kind: actionKindToProto(a.Kind),
		Args: a.Args,
	}.Build()
}

func entityFromProto(e franzv1.Entity) indicator.Entity {
	switch e {
	case franzv1.Entity_ENTITY_ASYNC_CHANNEL:
		return indicator.EntityAsyncChannel
	case franzv1.Entity_ENTITY_KAFKA_TOPIC:
		return indicator.EntityKafkaTopic
	case franzv1.Entity_ENTITY_KAFKA_CLUSTER:
		return indicator.EntityKafkaCluster
	default:
		return "" // the domain rejects UNSPECIFIED
	}
}

func entityToProto(e indicator.Entity) *franzv1.Entity {
	v := franzv1.Entity_ENTITY_UNSPECIFIED
	switch e {
	case indicator.EntityAsyncChannel:
		v = franzv1.Entity_ENTITY_ASYNC_CHANNEL
	case indicator.EntityKafkaTopic:
		v = franzv1.Entity_ENTITY_KAFKA_TOPIC
	case indicator.EntityKafkaCluster:
		v = franzv1.Entity_ENTITY_KAFKA_CLUSTER
	}
	return &v
}

func operatorFromProto(o franzv1.Operator) gov.Operator {
	switch o {
	case franzv1.Operator_OPERATOR_LESS_THAN:
		return gov.OpLessThan
	case franzv1.Operator_OPERATOR_LESS_THAN_OR_EQUAL:
		return gov.OpLessThanOrEqual
	case franzv1.Operator_OPERATOR_EQUAL:
		return gov.OpEqual
	case franzv1.Operator_OPERATOR_NOT_EQUAL:
		return gov.OpNotEqual
	case franzv1.Operator_OPERATOR_GREATER_THAN_OR_EQUAL:
		return gov.OpGreaterThanOrEqual
	case franzv1.Operator_OPERATOR_GREATER_THAN:
		return gov.OpGreaterThan
	default:
		return "" // the domain rejects UNSPECIFIED
	}
}

func operatorToProto(o gov.Operator) *franzv1.Operator {
	v := franzv1.Operator_OPERATOR_UNSPECIFIED
	switch o {
	case gov.OpLessThan:
		v = franzv1.Operator_OPERATOR_LESS_THAN
	case gov.OpLessThanOrEqual:
		v = franzv1.Operator_OPERATOR_LESS_THAN_OR_EQUAL
	case gov.OpEqual:
		v = franzv1.Operator_OPERATOR_EQUAL
	case gov.OpNotEqual:
		v = franzv1.Operator_OPERATOR_NOT_EQUAL
	case gov.OpGreaterThanOrEqual:
		v = franzv1.Operator_OPERATOR_GREATER_THAN_OR_EQUAL
	case gov.OpGreaterThan:
		v = franzv1.Operator_OPERATOR_GREATER_THAN
	}
	return &v
}

func actionKindFromProto(k franzv1.ActionKind) gov.ActionKind {
	switch k {
	case franzv1.ActionKind_ACTION_KIND_ADD_LABEL:
		return gov.ActionAddLabel
	case franzv1.ActionKind_ACTION_KIND_REMOVE_LABEL:
		return gov.ActionRemoveLabel
	case franzv1.ActionKind_ACTION_KIND_SET_STATUS:
		return gov.ActionSetStatus
	case franzv1.ActionKind_ACTION_KIND_UPDATE_FIELD:
		return gov.ActionUpdateField
	case franzv1.ActionKind_ACTION_KIND_INCREASE_FIELD_BY:
		return gov.ActionIncreaseFieldBy
	case franzv1.ActionKind_ACTION_KIND_DECREASE_FIELD_BY:
		return gov.ActionDecreaseFieldBy
	default:
		return "" // the domain rejects UNSPECIFIED
	}
}

func actionKindToProto(k gov.ActionKind) *franzv1.ActionKind {
	v := franzv1.ActionKind_ACTION_KIND_UNSPECIFIED
	switch k {
	case gov.ActionAddLabel:
		v = franzv1.ActionKind_ACTION_KIND_ADD_LABEL
	case gov.ActionRemoveLabel:
		v = franzv1.ActionKind_ACTION_KIND_REMOVE_LABEL
	case gov.ActionSetStatus:
		v = franzv1.ActionKind_ACTION_KIND_SET_STATUS
	case gov.ActionUpdateField:
		v = franzv1.ActionKind_ACTION_KIND_UPDATE_FIELD
	case gov.ActionIncreaseFieldBy:
		v = franzv1.ActionKind_ACTION_KIND_INCREASE_FIELD_BY
	case gov.ActionDecreaseFieldBy:
		v = franzv1.ActionKind_ACTION_KIND_DECREASE_FIELD_BY
	}
	return &v
}

func healthToProto(h indicator.Health) *franzv1.IndicatorHealth {
	v := franzv1.IndicatorHealth_INDICATOR_HEALTH_UNSPECIFIED
	switch h {
	case indicator.HealthHealthy:
		v = franzv1.IndicatorHealth_INDICATOR_HEALTH_HEALTHY
	case indicator.HealthStale:
		v = franzv1.IndicatorHealth_INDICATOR_HEALTH_STALE
	}
	return &v
}
