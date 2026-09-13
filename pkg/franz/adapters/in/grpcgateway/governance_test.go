package grpcgateway

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// fakeGovernanceSvc records what the handler forwarded and returns canned rows.
type fakeGovernanceSvc struct {
	createdPolicy    in.CreatePolicyInput
	updatedPolicy    in.UpdatePolicyInput
	dryRunDefinition gov.Definition
	createdIndicator in.CreateIndicatorInput
	updatedIndicator in.UpdateIndicatorInput
	listedSamples    in.ListIndicatorSamplesInput

	policy    *gov.Policy
	indicator *indicator.Indicator
	actions   []*gov.ActionRecord
	samples   []*indicator.Sample
	matches   []in.DryRunMatch
	err       error
}

var _ in.GovernanceService = (*fakeGovernanceSvc)(nil)

func (f *fakeGovernanceSvc) CreatePolicy(_ context.Context, i in.CreatePolicyInput) (*gov.Policy, error) {
	f.createdPolicy = i
	return f.policy, f.err
}

func (f *fakeGovernanceSvc) GetPolicy(context.Context, string) (*gov.Policy, error) {
	return f.policy, f.err
}

func (f *fakeGovernanceSvc) ListPolicies(context.Context, in.ListPoliciesInput) (in.PolicyPage, error) {
	return in.PolicyPage{Policies: []*gov.Policy{f.policy}, NextPageToken: "next"}, f.err
}

func (f *fakeGovernanceSvc) UpdatePolicy(_ context.Context, i in.UpdatePolicyInput) (*gov.Policy, error) {
	f.updatedPolicy = i
	return f.policy, f.err
}

func (f *fakeGovernanceSvc) DeletePolicy(context.Context, string) error { return f.err }

func (f *fakeGovernanceSvc) DryRunPolicy(_ context.Context, d gov.Definition) ([]in.DryRunMatch, error) {
	f.dryRunDefinition = d
	return f.matches, f.err
}

func (f *fakeGovernanceSvc) ListPolicyActions(
	context.Context, in.ListPolicyActionsInput,
) (in.PolicyActionPage, error) {
	return in.PolicyActionPage{Actions: f.actions}, f.err
}

func (f *fakeGovernanceSvc) CreateIndicator(
	_ context.Context, i in.CreateIndicatorInput,
) (*indicator.Indicator, error) {
	f.createdIndicator = i
	return f.indicator, f.err
}

func (f *fakeGovernanceSvc) GetIndicator(context.Context, string) (*indicator.Indicator, error) {
	return f.indicator, f.err
}

func (f *fakeGovernanceSvc) ListIndicators(
	context.Context, in.ListIndicatorsInput,
) (in.IndicatorPage, error) {
	return in.IndicatorPage{Indicators: []*indicator.Indicator{f.indicator}}, f.err
}

func (f *fakeGovernanceSvc) UpdateIndicator(
	_ context.Context, i in.UpdateIndicatorInput,
) (*indicator.Indicator, error) {
	f.updatedIndicator = i
	return f.indicator, f.err
}

func (f *fakeGovernanceSvc) DeleteIndicator(context.Context, string) error { return f.err }

func (f *fakeGovernanceSvc) ListIndicatorSamples(
	_ context.Context, i in.ListIndicatorSamplesInput,
) (in.IndicatorSamplePage, error) {
	f.listedSamples = i
	return in.IndicatorSamplePage{Samples: f.samples}, f.err
}

// --- helpers --------------------------------------------------------------

func newGovernanceHandler(svc in.GovernanceService, now time.Time) *governanceHandler {
	return &governanceHandler{
		svc:   svc,
		codec: frn.MustCodec("frn"),
		now:   func() time.Time { return now },
	}
}

func samplePolicy(t *testing.T) *gov.Policy {
	t.Helper()
	p, err := gov.New(realm.Realm{Slug: "default"}, "pause-hot", gov.Definition{
		Indicator: "lag",
		Matcher:   gov.Matcher{Entity: indicator.EntityAsyncChannel, Selector: "tier=gold"},
		Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "100"},
		Actions:   []gov.Action{{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}}},
	}, 10, true)
	if err != nil {
		t.Fatal(err)
	}
	p.CreatedAt = time.Unix(1700000000, 0)
	return p
}

// --- Policy ---------------------------------------------------------------

func TestCreatePolicyForwardsAndRenders(t *testing.T) {
	svc := &fakeGovernanceSvc{policy: samplePolicy(t)}
	h := newGovernanceHandler(svc, time.Now())

	resp, err := h.CreatePolicy(context.Background(), franzv1.CreatePolicyRequest_builder{
		Name:      proto.String("pause-hot"),
		Indicator: proto.String("lag"),
		Matcher: franzv1.Matcher_builder{
			Entity:   franzv1.Entity_ENTITY_ASYNC_CHANNEL.Enum(),
			Selector: proto.String("tier=gold"),
		}.Build(),
		Limit: franzv1.Limit_builder{
			Operator: franzv1.Operator_OPERATOR_GREATER_THAN.Enum(),
			Value:    proto.String("100"),
		}.Build(),
		Actions: []*franzv1.Action{
			franzv1.Action_builder{
				Kind: franzv1.ActionKind_ACTION_KIND_SET_STATUS.Enum(),
				Args: []string{"PAUSED"},
			}.Build(),
		},
		Weight:  proto.Int32(10),
		Enabled: proto.Bool(true),
	}.Build())
	if err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	// The proto enums decoded into the domain vocabulary.
	got := svc.createdPolicy
	if got.Name != "pause-hot" || got.Weight != 10 || !got.Enabled {
		t.Errorf("forwarded = %+v", got)
	}
	if got.Definition.Matcher.Entity != indicator.EntityAsyncChannel {
		t.Errorf("entity = %v", got.Definition.Matcher.Entity)
	}
	if got.Definition.Limit.Operator != gov.OpGreaterThan {
		t.Errorf("operator = %v", got.Definition.Limit.Operator)
	}
	if len(got.Definition.Actions) != 1 || got.Definition.Actions[0].Kind != gov.ActionSetStatus {
		t.Errorf("actions = %+v", got.Definition.Actions)
	}

	// And the response rendered back out, with the configured FRN prefix.
	p := resp.GetPolicy()
	if p.GetFrn() != "frn:default:policy:pause-hot" {
		t.Errorf("frn = %q", p.GetFrn())
	}
	if p.GetMatcher().GetEntity() != franzv1.Entity_ENTITY_ASYNC_CHANNEL {
		t.Errorf("rendered entity = %v", p.GetMatcher().GetEntity())
	}
	if p.GetLimit().GetOperator() != franzv1.Operator_OPERATOR_GREATER_THAN {
		t.Errorf("rendered operator = %v", p.GetLimit().GetOperator())
	}
	if p.GetActions()[0].GetKind() != franzv1.ActionKind_ACTION_KIND_SET_STATUS {
		t.Errorf("rendered action kind = %v", p.GetActions()[0].GetKind())
	}
	// last_fired_at is absent on a policy that has never fired, rather than the
	// zero instant.
	if p.HasLastFiredAt() {
		t.Error("an unfired policy must not carry last_fired_at")
	}
}

func TestUpdatePolicyMask(t *testing.T) {
	svc := &fakeGovernanceSvc{policy: samplePolicy(t)}
	h := newGovernanceHandler(svc, time.Now())

	req := franzv1.UpdatePolicyRequest_builder{
		Name:    proto.String("pause-hot"),
		Weight:  proto.Int32(3),
		Enabled: proto.Bool(false),
		Limit: franzv1.Limit_builder{
			Operator: franzv1.Operator_OPERATOR_LESS_THAN.Enum(),
			Value:    proto.String("5"),
		}.Build(),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"weight", "enabled", "limit"}},
	}.Build()

	if _, err := h.UpdatePolicy(context.Background(), req); err != nil {
		t.Fatalf("UpdatePolicy: %v", err)
	}
	got := svc.updatedPolicy
	if got.Weight == nil || *got.Weight != 3 {
		t.Errorf("weight = %v", got.Weight)
	}
	if got.Enabled == nil || *got.Enabled {
		t.Errorf("enabled = %v", got.Enabled)
	}
	if got.Limit == nil || got.Limit.Operator != gov.OpLessThan || got.Limit.Value != "5" {
		t.Errorf("limit = %+v", got.Limit)
	}
	// Unmasked fields stay nil — "leave unchanged" (003.1).
	if got.Indicator != nil || got.Matcher != nil || got.Actions != nil {
		t.Errorf("unmasked fields leaked: %+v", got)
	}
}

func TestUpdatePolicyRejectsAnEmptyMask(t *testing.T) {
	h := newGovernanceHandler(&fakeGovernanceSvc{policy: samplePolicy(t)}, time.Now())
	_, err := h.UpdatePolicy(context.Background(), franzv1.UpdatePolicyRequest_builder{
		Name: proto.String("pause-hot"),
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestUpdatePolicyRejectsAnImmutableField(t *testing.T) {
	h := newGovernanceHandler(&fakeGovernanceSvc{policy: samplePolicy(t)}, time.Now())
	_, err := h.UpdatePolicy(context.Background(), franzv1.UpdatePolicyRequest_builder{
		Name:       proto.String("pause-hot"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestDryRunPolicyForwardsInlineDefinition(t *testing.T) {
	svc := &fakeGovernanceSvc{matches: []in.DryRunMatch{
		{ResourceFRN: "default:async-channel:orders", IndicatorValue: "150", WouldTrigger: true},
		{ResourceFRN: "default:async-channel:invoices", IndicatorValue: "10"},
	}}
	h := newGovernanceHandler(svc, time.Now())

	resp, err := h.DryRunPolicy(context.Background(), franzv1.DryRunPolicyRequest_builder{
		Indicator: proto.String("lag"),
		Matcher: franzv1.Matcher_builder{
			Entity: franzv1.Entity_ENTITY_ASYNC_CHANNEL.Enum(),
		}.Build(),
		Limit: franzv1.Limit_builder{
			Operator: franzv1.Operator_OPERATOR_GREATER_THAN.Enum(),
			Value:    proto.String("100"),
		}.Build(),
		Actions: []*franzv1.Action{
			franzv1.Action_builder{
				Kind: franzv1.ActionKind_ACTION_KIND_SET_STATUS.Enum(),
				Args: []string{"PAUSED"},
			}.Build(),
		},
	}.Build())
	if err != nil {
		t.Fatalf("DryRunPolicy: %v", err)
	}
	if svc.dryRunDefinition.Indicator != "lag" ||
		svc.dryRunDefinition.Matcher.Entity != indicator.EntityAsyncChannel {
		t.Errorf("forwarded = %+v", svc.dryRunDefinition)
	}
	if len(resp.GetMatches()) != 2 {
		t.Fatalf("matches = %d", len(resp.GetMatches()))
	}
	if !resp.GetMatches()[0].GetWouldTrigger() || resp.GetMatches()[1].GetWouldTrigger() {
		t.Errorf("would_trigger = %v, %v",
			resp.GetMatches()[0].GetWouldTrigger(), resp.GetMatches()[1].GetWouldTrigger())
	}
}

func TestListPolicyActionsRenders(t *testing.T) {
	occurred := time.Unix(1700000000, 0).UTC()
	svc := &fakeGovernanceSvc{actions: []*gov.ActionRecord{
		gov.NewActionRecord(realm.DefaultID, samplePolicy(t).ID, "pause-hot",
			"default:async-channel:orders", "150",
			gov.Action{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}},
			"state=PAUSED", occurred),
		// A cluster sub-resource FRN is not parseable; it must survive verbatim
		// rather than be dropped (005 ADR §2.1).
		gov.NewActionRecord(realm.DefaultID, samplePolicy(t).ID, "pause-hot",
			"default:kafka-cluster:east-1/broker/3", "200Gi",
			gov.Action{Kind: gov.ActionAddLabel, Args: []string{"incident", "disk"}},
			"incident=disk", occurred),
	}}
	h := newGovernanceHandler(svc, time.Now())

	resp, err := h.ListPolicyActions(context.Background(), franzv1.ListPolicyActionsRequest_builder{
		Name: proto.String("pause-hot"),
	}.Build())
	if err != nil {
		t.Fatalf("ListPolicyActions: %v", err)
	}
	if len(resp.GetActions()) != 2 {
		t.Fatalf("actions = %d", len(resp.GetActions()))
	}
	first := resp.GetActions()[0]
	if first.GetResourceFrn() != "frn:default:async-channel:orders" {
		t.Errorf("resource_frn = %q, want the configured prefix applied", first.GetResourceFrn())
	}
	if first.GetAction().GetKind() != franzv1.ActionKind_ACTION_KIND_SET_STATUS {
		t.Errorf("kind = %v", first.GetAction().GetKind())
	}
	if first.GetResult() != "state=PAUSED" || first.GetIndicatorValue() != "150" {
		t.Errorf("outcome = (%q, %q)", first.GetResult(), first.GetIndicatorValue())
	}
	if got := resp.GetActions()[1].GetResourceFrn(); got != "default:kafka-cluster:east-1/broker/3" {
		t.Errorf("sub-resource frn = %q, want it passed through unchanged", got)
	}
}

// --- Indicator ------------------------------------------------------------

func TestCreateIndicatorForwardsAndDerivesHealth(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	sampledAt := now.Add(-30 * time.Minute)

	ind, err := indicator.NewIndicator(realm.Realm{Slug: "default"}, "disk-used",
		indicator.UnitBytes, indicator.EntityKafkaCluster, "1h", []string{"gregor-samsa"})
	if err != nil {
		t.Fatal(err)
	}
	ind.LastSampleAt = &sampledAt

	svc := &fakeGovernanceSvc{indicator: ind}
	h := newGovernanceHandler(svc, now)

	resp, err := h.CreateIndicator(context.Background(), franzv1.CreateIndicatorRequest_builder{
		Name:               proto.String("disk-used"),
		Unit:               proto.String("bytes"),
		AppliesTo:          franzv1.Entity_ENTITY_KAFKA_CLUSTER.Enum(),
		StalenessThreshold: proto.String("1h"),
		SourceAgents:       []string{"gregor-samsa"},
	}.Build())
	if err != nil {
		t.Fatalf("CreateIndicator: %v", err)
	}
	if svc.createdIndicator.Unit != indicator.UnitBytes ||
		svc.createdIndicator.AppliesTo != indicator.EntityKafkaCluster {
		t.Errorf("forwarded = %+v", svc.createdIndicator)
	}

	got := resp.GetIndicator()
	// health is derived at render time from last_sample_at + the threshold.
	if got.GetHealth() != franzv1.IndicatorHealth_INDICATOR_HEALTH_HEALTHY {
		t.Errorf("health = %v, want HEALTHY 30m into a 1h window", got.GetHealth())
	}
	// The operator's verbatim threshold text is what comes back.
	if got.GetStalenessThreshold() != "1h" {
		t.Errorf("staleness_threshold = %q", got.GetStalenessThreshold())
	}
	if got.GetAppliesTo() != franzv1.Entity_ENTITY_KAFKA_CLUSTER {
		t.Errorf("applies_to = %v", got.GetAppliesTo())
	}

	// Move the clock past the threshold and the same row renders STALE.
	stale := newGovernanceHandler(svc, now.Add(2*time.Hour))
	staleResp, err := stale.GetIndicator(context.Background(),
		franzv1.GetIndicatorRequest_builder{Name: proto.String("disk-used")}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if staleResp.GetIndicator().GetHealth() != franzv1.IndicatorHealth_INDICATOR_HEALTH_STALE {
		t.Errorf("health = %v, want STALE past the window",
			staleResp.GetIndicator().GetHealth())
	}
}

func TestUpdateIndicatorMaskRejectsAppliesTo(t *testing.T) {
	ind, err := indicator.NewIndicator(realm.Realm{Slug: "default"}, "disk-used",
		indicator.UnitBytes, indicator.EntityKafkaCluster, "1h", nil)
	if err != nil {
		t.Fatal(err)
	}
	svc := &fakeGovernanceSvc{indicator: ind}
	h := newGovernanceHandler(svc, time.Now())

	// The three maskable fields forward.
	if _, err := h.UpdateIndicator(context.Background(), franzv1.UpdateIndicatorRequest_builder{
		Name:               proto.String("disk-used"),
		Unit:               proto.String("count"),
		StalenessThreshold: proto.String("6h"),
		SourceAgents:       []string{"agent-b"},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"unit", "staleness_threshold", "source_agents"},
		},
	}.Build()); err != nil {
		t.Fatalf("UpdateIndicator: %v", err)
	}
	got := svc.updatedIndicator
	if got.Unit == nil || *got.Unit != indicator.UnitCount {
		t.Errorf("unit = %v", got.Unit)
	}
	if got.StalenessThreshold == nil || *got.StalenessThreshold != "6h" {
		t.Errorf("staleness_threshold = %v", got.StalenessThreshold)
	}
	if got.SourceAgents == nil || len(*got.SourceAgents) != 1 {
		t.Errorf("source_agents = %v", got.SourceAgents)
	}

	// applies_to is not on UpdateIndicatorRequest at all, so masking it is an
	// unknown field — the 003.14 immutability rule, enforced at the edge.
	_, err = h.UpdateIndicator(context.Background(), franzv1.UpdateIndicatorRequest_builder{
		Name:       proto.String("disk-used"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"applies_to"}},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestListIndicatorSamplesForwardsFilters(t *testing.T) {
	from := time.Unix(1700000000, 0).UTC()
	to := from.Add(time.Hour)
	sample, err := indicator.NewSample(realm.DefaultID, "lag",
		"default:async-channel:orders", indicator.EntityAsyncChannel, "150", "agent-a", from, from)
	if err != nil {
		t.Fatal(err)
	}
	svc := &fakeGovernanceSvc{samples: []*indicator.Sample{sample}}
	h := newGovernanceHandler(svc, time.Now())

	resp, err := h.ListIndicatorSamples(context.Background(),
		franzv1.ListIndicatorSamplesRequest_builder{
			Indicator:   proto.String("lag"),
			ResourceFrn: proto.String("default:async-channel:orders"),
			From:        timestamppb.New(from),
			To:          timestamppb.New(to),
		}.Build())
	if err != nil {
		t.Fatalf("ListIndicatorSamples: %v", err)
	}
	if svc.listedSamples.Indicator != "lag" ||
		svc.listedSamples.ResourceFRN != "default:async-channel:orders" {
		t.Errorf("forwarded = %+v", svc.listedSamples)
	}
	if !svc.listedSamples.From.Equal(from) || !svc.listedSamples.To.Equal(to) {
		t.Errorf("window = (%v, %v)", svc.listedSamples.From, svc.listedSamples.To)
	}
	if len(resp.GetSamples()) != 1 {
		t.Fatalf("samples = %d", len(resp.GetSamples()))
	}
	got := resp.GetSamples()[0]
	if got.GetResourceFrn() != "frn:default:async-channel:orders" {
		t.Errorf("resource_frn = %q", got.GetResourceFrn())
	}
	if got.GetResourceEntity() != franzv1.Entity_ENTITY_ASYNC_CHANNEL || got.GetValue() != "150" {
		t.Errorf("rendered = %+v", got)
	}
}

// TestGovernanceErrorsMapToStatusCodes checks the domain-error vocabulary
// reaches the client as the codes 003.1 mandates.
func TestGovernanceErrorsMapToStatusCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"unknown indicator", errs.Preconditionf("not registered"), codes.FailedPrecondition},
		{"out of whitelist", errs.InvalidField("actions[0]", "not whitelisted"), codes.InvalidArgument},
		{"missing policy", errs.NotFoundf("no such policy"), codes.NotFound},
		{"duplicate name", errs.Existsf("taken"), codes.AlreadyExists},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newGovernanceHandler(&fakeGovernanceSvc{err: tc.err}, time.Now())
			_, err := h.CreatePolicy(context.Background(),
				franzv1.CreatePolicyRequest_builder{Name: proto.String("p")}.Build())
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v, want %v", status.Code(err), tc.want)
			}
		})
	}
}
