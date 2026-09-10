package governance_test

import (
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/governance"
)

func testRealm() realm.Realm {
	return realm.Realm{ID: realm.DefaultID, Slug: realm.DefaultSlug}
}

// registeredIndicator builds an indicator that is fresh at `now`.
func registeredIndicator(name string, unit indicator.Unit, appliesTo indicator.Entity) *indicator.Indicator {
	i, err := indicator.NewIndicator(testRealm(), name, unit, appliesTo, "1h", nil)
	if err != nil {
		panic(err)
	}
	at := time.Now().UTC()
	i.LastSampleAt = &at
	return i
}

func pauseAction() gov.Action {
	return gov.Action{Kind: gov.ActionSetStatus, Args: []string{"PAUSED"}}
}

func channelDefinition(indicatorName, selector string) gov.Definition {
	return gov.Definition{
		Indicator: indicatorName,
		Matcher:   gov.Matcher{Entity: indicator.EntityAsyncChannel, Selector: selector},
		Limit:     gov.Limit{Operator: gov.OpGreaterThan, Value: "100"},
		Actions:   []gov.Action{pauseAction()},
	}
}

// testChannel is a governable async channel with labels.
func testChannel(name string, labels map[string]string) *channel.AsyncChannel {
	c, err := channel.New(testRealm(), name, channel.TypeKafkaTopic, 1, labels, accesspolicy.Policy{})
	if err != nil {
		panic(err)
	}
	return c
}

// serviceFixture wires the governance service over in-memory fakes.
type serviceFixture struct {
	svc        *governance.Service
	policies   *fakePolicyRepo
	indicators *fakeIndicatorRepo
	actions    *fakeActionRepo
	samples    *fakeSampleRepo
	channels   *fakeChannelRepo
}

func newServiceFixture(
	indicators []*indicator.Indicator, policies []*gov.Policy,
	channels map[string]*channel.AsyncChannel,
) serviceFixture {
	f := serviceFixture{
		policies:   newPolicyRepo(policies...),
		indicators: newIndicatorRepo(indicators...),
		actions:    &fakeActionRepo{},
		samples:    &fakeSampleRepo{},
		channels:   &fakeChannelRepo{rows: channels},
	}
	f.svc = governance.NewService(f.policies, f.indicators, f.actions, f.samples,
		f.channels, &fakeClusterRepo{rows: nil}, &fakeTopicRepo{rows: nil})
	return f
}

// TestCreatePolicyRejectsUnknownIndicator is the "Done when" check: a policy
// referencing an unregistered indicator is rejected, with FAILED_PRECONDITION —
// the request is well formed, the fleet is just not ready for it.
func TestCreatePolicyRejectsUnknownIndicator(t *testing.T) {
	f := newServiceFixture(nil, nil, nil)

	_, err := f.svc.CreatePolicy(ctxWithRealm(), in.CreatePolicyInput{
		Name:       "pause-hot-channels",
		Definition: channelDefinition("never-registered", ""),
		Enabled:    true,
	})
	if errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v (err %v), want FailedPrecondition", errs.KindOf(err), err)
	}
	if len(f.policies.rows) != 0 {
		t.Fatal("a rejected policy must not be stored")
	}
}

// TestCreatePolicyRejectsOutOfWhitelistAction is the second "Done when" check.
func TestCreatePolicyRejectsOutOfWhitelistAction(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)

	def := channelDefinition("lag", "")
	// `replication_factor` is a KAFKA_TOPIC field; a channel matcher may not write it.
	def.Actions = []gov.Action{{Kind: gov.ActionUpdateField, Args: []string{"replication_factor", "3"}}}

	_, err := f.svc.CreatePolicy(ctxWithRealm(), in.CreatePolicyInput{
		Name: "bad", Definition: def, Enabled: true,
	})
	if errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v (err %v), want InvalidArgument", errs.KindOf(err), err)
	}
	if len(f.policies.rows) != 0 {
		t.Fatal("a rejected policy must not be stored")
	}
}

// TestCreatePolicyRejectsPlacementActions covers the deviation this deliverable
// takes: the placement rows 003.8 whitelists need the migration flow (003.13),
// so they are refused at write rather than accepted and silently ignored.
func TestCreatePolicyRejectsPlacementActions(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)

	tests := []struct {
		name   string
		action gov.Action
	}{
		{"affinity-label", gov.Action{Kind: gov.ActionAddLabel,
			Args: []string{"franz.affinity/region", "eu-west"}}},
		{"antiaffinity-label", gov.Action{Kind: gov.ActionRemoveLabel,
			Args: []string{"franz.antiaffinity/rack"}}},
		{"channel-partitions-reshard", gov.Action{Kind: gov.ActionIncreaseFieldBy,
			Args: []string{"channel_partitions", "1"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			def := channelDefinition("lag", "")
			def.Actions = []gov.Action{tc.action}
			_, err := f.svc.CreatePolicy(ctxWithRealm(), in.CreatePolicyInput{
				Name: "p-" + tc.name, Definition: def, Enabled: true,
			})
			if errs.KindOf(err) != errs.FailedPrecondition {
				t.Fatalf("kind = %v (err %v), want FailedPrecondition", errs.KindOf(err), err)
			}
		})
	}
}

// TestCreatePolicyRejectsEntityMismatch: a cluster-scoped indicator cannot drive
// a channel matcher (003.8 invariant).
func TestCreatePolicyRejectsEntityMismatch(t *testing.T) {
	ind := registeredIndicator("disk-used", indicator.UnitBytes, indicator.EntityKafkaCluster)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)

	_, err := f.svc.CreatePolicy(ctxWithRealm(), in.CreatePolicyInput{
		Name: "mismatch", Definition: channelDefinition("disk-used", ""), Enabled: true,
	})
	if errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v (err %v), want InvalidArgument", errs.KindOf(err), err)
	}
}

func TestCreateAndGetPolicy(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)
	ctx := ctxWithRealm()

	created, err := f.svc.CreatePolicy(ctx, in.CreatePolicyInput{
		Name: "pause-hot", Definition: channelDefinition("lag", "tier=gold"),
		Weight: 10, Enabled: true,
	})
	if err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if created.FRN.Name() != "pause-hot" || created.Weight != 10 || !created.Enabled {
		t.Fatalf("created = %+v", created)
	}

	got, err := f.svc.GetPolicy(ctx, "pause-hot")
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if got.Name != "pause-hot" || got.Matcher.Selector != "tier=gold" {
		t.Fatalf("got = %+v", got)
	}

	// A duplicate name is rejected — `name` is realm-unique (003.8).
	if _, err := f.svc.CreatePolicy(ctx, in.CreatePolicyInput{
		Name: "pause-hot", Definition: channelDefinition("lag", ""), Enabled: true,
	}); errs.KindOf(err) != errs.AlreadyExists {
		t.Fatalf("duplicate name kind = %v, want AlreadyExists", errs.KindOf(err))
	}
}

// TestUpdatePolicyRevalidatesWholeDefinition: a mask that touches only one half
// of the definition can still invalidate the policy, so the whole thing is
// re-checked.
func TestUpdatePolicyRevalidatesWholeDefinition(t *testing.T) {
	ind := registeredIndicator("flapping", indicator.UnitBoolean, indicator.EntityAsyncChannel)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)
	ctx := ctxWithRealm()

	def := channelDefinition("flapping", "")
	def.Limit = gov.Limit{Operator: gov.OpEqual, Value: "true"}
	if _, err := f.svc.CreatePolicy(ctx, in.CreatePolicyInput{
		Name: "pause-flapping", Definition: def, Enabled: true,
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	// A limit value that does not parse as a boolean must be rejected.
	badLimit := gov.Limit{Operator: gov.OpEqual, Value: "150Gi"}
	if _, err := f.svc.UpdatePolicy(ctx, in.UpdatePolicyInput{
		Name: "pause-flapping", Limit: &badLimit,
	}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v, want InvalidArgument", errs.KindOf(err))
	}

	// A masked weight/enabled change leaves the definition alone and succeeds.
	weight := int32(7)
	enabled := false
	updated, err := f.svc.UpdatePolicy(ctx, in.UpdatePolicyInput{
		Name: "pause-flapping", Weight: &weight, Enabled: &enabled,
	})
	if err != nil {
		t.Fatalf("UpdatePolicy: %v", err)
	}
	if updated.Weight != 7 || updated.Enabled {
		t.Fatalf("updated = %+v", updated)
	}
	if updated.Limit.Value != "true" {
		t.Fatalf("an unmasked limit must not change: %+v", updated.Limit)
	}
}

// TestUpdatePolicyRejectsUnknownIndicator: repointing at an unregistered
// indicator is refused, so a stored policy can never name one.
func TestUpdatePolicyRejectsUnknownIndicator(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)
	ctx := ctxWithRealm()

	if _, err := f.svc.CreatePolicy(ctx, in.CreatePolicyInput{
		Name: "p", Definition: channelDefinition("lag", ""), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	other := "not-registered"
	if _, err := f.svc.UpdatePolicy(ctx, in.UpdatePolicyInput{
		Name: "p", Indicator: &other,
	}); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v, want FailedPrecondition", errs.KindOf(err))
	}
	if f.policies.rows["p"].Indicator != "lag" {
		t.Fatal("a rejected update must not be persisted")
	}
}

// --- Indicator registry ---------------------------------------------------

func TestIndicatorCRUD(t *testing.T) {
	f := newServiceFixture(nil, nil, nil)
	ctx := ctxWithRealm()

	created, err := f.svc.CreateIndicator(ctx, in.CreateIndicatorInput{
		Name: "disk-used", Unit: indicator.UnitBytes, AppliesTo: indicator.EntityKafkaCluster,
		StalenessThreshold: "90d", SourceAgents: []string{"gregor-samsa"},
	})
	if err != nil {
		t.Fatalf("CreateIndicator: %v", err)
	}
	if created.StalenessThreshold != 90*24*time.Hour {
		t.Errorf("StalenessThreshold = %v", created.StalenessThreshold)
	}

	unit := indicator.UnitCount
	staleness := "12h"
	updated, err := f.svc.UpdateIndicator(ctx, in.UpdateIndicatorInput{
		Name: "disk-used", Unit: &unit, StalenessThreshold: &staleness,
	})
	if err != nil {
		t.Fatalf("UpdateIndicator: %v", err)
	}
	if updated.Unit != indicator.UnitCount || updated.StalenessThreshold != 12*time.Hour {
		t.Fatalf("updated = %+v", updated)
	}
	// applies_to is immutable (003.14) — no mask can reach it.
	if updated.AppliesTo != indicator.EntityKafkaCluster {
		t.Fatalf("AppliesTo = %v, want it unchanged", updated.AppliesTo)
	}

	if err := f.svc.DeleteIndicator(ctx, "disk-used"); err != nil {
		t.Fatalf("DeleteIndicator: %v", err)
	}
	if _, err := f.svc.GetIndicator(ctx, "disk-used"); errs.KindOf(err) != errs.NotFound {
		t.Fatalf("after delete, kind = %v, want NotFound", errs.KindOf(err))
	}
}

// TestDeleteIndicatorRefusesWhileReferenced: deleting an indicator a policy
// names would leave a rule that can never be evaluated and never repaired.
func TestDeleteIndicatorRefusesWhileReferenced(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)
	ctx := ctxWithRealm()

	if _, err := f.svc.CreatePolicy(ctx, in.CreatePolicyInput{
		Name: "watcher", Definition: channelDefinition("lag", ""), Enabled: false,
	}); err != nil {
		t.Fatal(err)
	}

	// The reference holds even though the policy is disabled: a disabled policy
	// is still a rule someone may re-enable.
	err := f.svc.DeleteIndicator(ctx, "lag")
	if errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v (err %v), want FailedPrecondition", errs.KindOf(err), err)
	}
	if len(f.indicators.deleted) != 0 {
		t.Fatal("a refused delete must not touch the store")
	}

	if err := f.svc.DeletePolicy(ctx, "watcher"); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.DeleteIndicator(ctx, "lag"); err != nil {
		t.Fatalf("after removing the policy, DeleteIndicator: %v", err)
	}
}

func TestDeleteIndicatorNotFound(t *testing.T) {
	f := newServiceFixture(nil, nil, nil)
	if errs.KindOf(f.svc.DeleteIndicator(ctxWithRealm(), "ghost")) != errs.NotFound {
		t.Fatal("deleting an unregistered indicator must be NotFound")
	}
}

// --- dry run --------------------------------------------------------------

// TestDryRunEvaluatesWithoutMutating is the 003.8 "Dry run" contract: it reports
// per-resource would_trigger and writes nothing — no entity change, and no
// PolicyAction.
func TestDryRunEvaluatesWithoutMutating(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	channels := map[string]*channel.AsyncChannel{
		"orders":    testChannel("orders", map[string]string{"tier": "gold"}),
		"invoices":  testChannel("invoices", map[string]string{"tier": "gold"}),
		"telemetry": testChannel("telemetry", map[string]string{"tier": "bronze"}),
	}
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, channels)
	ctx := ctxWithRealm()

	now := time.Now().UTC()
	for name, value := range map[string]string{"orders": "150", "invoices": "10", "telemetry": "900"} {
		s, err := indicator.NewSample(realm.DefaultID, "lag",
			channels[name].FRN.Path(), indicator.EntityAsyncChannel, value, "agent", now, now)
		if err != nil {
			t.Fatal(err)
		}
		f.samples.samples = append(f.samples.samples, s)
	}

	matches, err := f.svc.DryRunPolicy(ctx, channelDefinition("lag", "tier=gold"))
	if err != nil {
		t.Fatalf("DryRunPolicy: %v", err)
	}
	// Only the two gold channels match the selector; the bronze one is excluded
	// even though its value crosses the limit.
	if len(matches) != 2 {
		t.Fatalf("matches = %d (%+v), want 2", len(matches), matches)
	}
	byFRN := map[string]in.DryRunMatch{}
	for _, m := range matches {
		byFRN[m.ResourceFRN] = m
	}
	if got := byFRN[channels["orders"].FRN.Path()]; !got.WouldTrigger || got.IndicatorValue != "150" {
		t.Errorf("orders = %+v, want would_trigger with value 150", got)
	}
	if got := byFRN[channels["invoices"].FRN.Path()]; got.WouldTrigger {
		t.Errorf("invoices = %+v, want would_trigger=false", got)
	}

	if len(f.actions.records) != 0 {
		t.Fatalf("a dry run wrote %d PolicyAction(s)", len(f.actions.records))
	}
	if channels["orders"].State != channel.StateActive {
		t.Fatal("a dry run must not change the resource")
	}
	if len(f.policies.rows) != 0 {
		t.Fatal("a dry run must not store the definition")
	}
}

// TestDryRunValidatesTheDefinition: an inline definition gets the same write-time
// checks as a saved one, so a dry run cannot be used to preview an illegal rule.
func TestDryRunValidatesTheDefinition(t *testing.T) {
	f := newServiceFixture(nil, nil, nil)
	_, err := f.svc.DryRunPolicy(ctxWithRealm(), channelDefinition("never-registered", ""))
	if errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v, want FailedPrecondition", errs.KindOf(err))
	}
}

// TestDryRunSkipsSamplesForTheWrongEntity keeps a matcher honest when an
// indicator's history contains rows from before an operator repointed things.
func TestDryRunSkipsSamplesForTheWrongEntity(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	channels := map[string]*channel.AsyncChannel{"orders": testChannel("orders", nil)}
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, channels)

	now := time.Now().UTC()
	stray, err := indicator.NewSample(realm.DefaultID, "lag",
		"default:kafka-cluster:east-1", indicator.EntityKafkaCluster, "999", "agent", now, now)
	if err != nil {
		t.Fatal(err)
	}
	f.samples.samples = append(f.samples.samples, stray)

	matches, err := f.svc.DryRunPolicy(ctxWithRealm(), channelDefinition("lag", ""))
	if err != nil {
		t.Fatalf("DryRunPolicy: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("matches = %+v, want none", matches)
	}
}

// --- listing --------------------------------------------------------------

func TestListPoliciesPaginates(t *testing.T) {
	ind := registeredIndicator("lag", indicator.UnitCount, indicator.EntityAsyncChannel)
	f := newServiceFixture([]*indicator.Indicator{ind}, nil, nil)
	ctx := ctxWithRealm()

	for _, name := range []string{"alpha", "bravo", "charlie"} {
		if _, err := f.svc.CreatePolicy(ctx, in.CreatePolicyInput{
			Name: name, Definition: channelDefinition("lag", ""), Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	first, err := f.svc.ListPolicies(ctx, in.ListPoliciesInput{PageSize: 2})
	if err != nil {
		t.Fatalf("ListPolicies: %v", err)
	}
	if len(first.Policies) != 2 || first.Policies[0].Name != "alpha" {
		t.Fatalf("first page = %+v", first.Policies)
	}
	if first.NextPageToken == "" {
		t.Fatal("a full page must carry a continuation token")
	}

	second, err := f.svc.ListPolicies(ctx, in.ListPoliciesInput{
		PageSize: 2, PageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatalf("ListPolicies(page 2): %v", err)
	}
	if len(second.Policies) != 1 || second.Policies[0].Name != "charlie" {
		t.Fatalf("second page = %+v", second.Policies)
	}
	if second.NextPageToken != "" {
		t.Fatal("the last page must not carry a token")
	}

	// A token minted for one query cannot be replayed against another (003.1).
	if _, err := f.svc.ListIndicators(ctx, in.ListIndicatorsInput{
		PageToken: first.NextPageToken,
	}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatal("a cross-query token must be rejected")
	}
}

func TestListIndicatorSamplesValidates(t *testing.T) {
	f := newServiceFixture(nil, nil, nil)
	ctx := ctxWithRealm()

	if _, err := f.svc.ListIndicatorSamples(ctx, in.ListIndicatorSamplesInput{}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatal("an unnamed indicator must be rejected")
	}
	now := time.Now()
	if _, err := f.svc.ListIndicatorSamples(ctx, in.ListIndicatorSamplesInput{
		Indicator: "lag", From: now, To: now.Add(-time.Hour),
	}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatal("an inverted time range must be rejected")
	}
}

func TestListPolicyActionsRequiresAName(t *testing.T) {
	f := newServiceFixture(nil, nil, nil)
	if _, err := f.svc.ListPolicyActions(ctxWithRealm(), in.ListPolicyActionsInput{}); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatal("an unnamed policy must be rejected")
	}
}
