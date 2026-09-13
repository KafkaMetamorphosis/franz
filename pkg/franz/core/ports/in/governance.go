package in

import (
	"context"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
)

// CreatePolicyInput is the client-settable state for a new Policy.
type CreatePolicyInput struct {
	Name       string
	Definition governance.Definition
	Weight     int32
	Enabled    bool
}

// UpdatePolicyInput carries only the fields named in the request's FieldMask; a
// nil pointer means "leave unchanged". `name` selects the policy and is not
// itself mutable.
type UpdatePolicyInput struct {
	Name      string
	Indicator *string
	Matcher   *governance.Matcher
	Limit     *governance.Limit
	Actions   *[]governance.Action
	Weight    *int32
	Enabled   *bool
}

// ListPoliciesInput parameterises a List call. PageToken is opaque.
type ListPoliciesInput struct {
	PageSize  int32
	PageToken string
}

// PolicyPage is a page of List results.
type PolicyPage struct {
	Policies      []*governance.Policy
	NextPageToken string
	TotalSize     int32
}

// DryRunMatch is one resource a dry run evaluated.
type DryRunMatch struct {
	ResourceFRN    string
	IndicatorValue string
	WouldTrigger   bool
}

// ListPolicyActionsInput parameterises the audit read.
type ListPolicyActionsInput struct {
	PolicyName string
	PageSize   int32
	PageToken  string
}

// PolicyActionPage is a page of audit rows, newest first.
type PolicyActionPage struct {
	Actions       []*governance.ActionRecord
	NextPageToken string
}

// CreateIndicatorInput is the client-settable state for a new Indicator.
type CreateIndicatorInput struct {
	Name               string
	Unit               indicator.Unit
	AppliesTo          indicator.Entity
	StalenessThreshold string
	SourceAgents       []string
}

// UpdateIndicatorInput carries only the masked fields. `applies_to` is absent by
// design — it is immutable (003.14).
type UpdateIndicatorInput struct {
	Name               string
	Unit               *indicator.Unit
	StalenessThreshold *string
	SourceAgents       *[]string
}

// ListIndicatorsInput parameterises a List call.
type ListIndicatorsInput struct {
	PageSize  int32
	PageToken string
}

// IndicatorPage is a page of List results.
type IndicatorPage struct {
	Indicators    []*indicator.Indicator
	NextPageToken string
	TotalSize     int32
}

// ListIndicatorSamplesInput parameterises the sample history read (003.14). All
// filters but `Indicator` are optional; zero times mean "unbounded".
type ListIndicatorSamplesInput struct {
	Indicator   string
	ResourceFRN string
	From        time.Time
	To          time.Time
	PageSize    int32
	PageToken   string
}

// IndicatorSamplePage is a page of sample history, newest first.
type IndicatorSamplePage struct {
	Samples       []*indicator.Sample
	NextPageToken string
}

// GovernanceService is the driving port for governance (003.8): the Indicator
// registry, Policy CRUD, the dry run, and the two read-only series. The realm is
// taken from the request context, never from the input.
type GovernanceService interface {
	CreatePolicy(ctx context.Context, in CreatePolicyInput) (*governance.Policy, error)
	GetPolicy(ctx context.Context, name string) (*governance.Policy, error)
	ListPolicies(ctx context.Context, in ListPoliciesInput) (PolicyPage, error)
	UpdatePolicy(ctx context.Context, in UpdatePolicyInput) (*governance.Policy, error)
	DeletePolicy(ctx context.Context, name string) error

	// DryRunPolicy evaluates an inline, unsaved definition against the latest
	// samples. It performs no mutation and writes no PolicyAction (003.8).
	DryRunPolicy(ctx context.Context, def governance.Definition) ([]DryRunMatch, error)

	// ListPolicyActions reads the audit series of one policy, newest first. The
	// rows outlive the policy, so a deleted policy's history is still readable.
	ListPolicyActions(ctx context.Context, in ListPolicyActionsInput) (PolicyActionPage, error)

	CreateIndicator(ctx context.Context, in CreateIndicatorInput) (*indicator.Indicator, error)
	GetIndicator(ctx context.Context, name string) (*indicator.Indicator, error)
	ListIndicators(ctx context.Context, in ListIndicatorsInput) (IndicatorPage, error)
	UpdateIndicator(ctx context.Context, in UpdateIndicatorInput) (*indicator.Indicator, error)
	// DeleteIndicator refuses (FAILED_PRECONDITION) while any policy references
	// the indicator (003.8).
	DeleteIndicator(ctx context.Context, name string) error

	ListIndicatorSamples(ctx context.Context, in ListIndicatorSamplesInput) (IndicatorSamplePage, error)
}

// GovernanceEvaluator is the event-driven evaluation entry point (003.8
// "Evaluation"): telemetry ingest calls it in-process after a sample updates the
// current value for (indicator, resource), and it applies every enabled policy
// bound to that indicator whose matcher selects the resource.
//
// It is deliberately separate from GovernanceService: ingest is agent-facing and
// must not be able to reach policy CRUD, and the split lets deliverable 15 wire
// the call against a no-op while it is built.
type GovernanceEvaluator interface {
	// Evaluate runs one pass. It returns an error only when the pass could not
	// run at all (the store is unreachable); a policy that fails to apply is
	// recorded as a PolicyAction with the failure in `result` and does not fail
	// the pass, because ingest must not be blocked by one bad rule.
	Evaluate(ctx context.Context, indicatorName, resourceFRN, newValue string) error
}
