package governance

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
)

// Service implements in.GovernanceService.
type Service struct {
	policies   out.PolicyRepository
	indicators out.IndicatorRepository
	actions    out.PolicyActionRepository
	samples    out.IndicatorSampleRepository
	resolve    resolver
}

var _ in.GovernanceService = (*Service)(nil)

// NewService wires the governance service to its repositories. The three entity
// repositories are read-only here — they resolve the `resource_frn` a dry run
// evaluates; writing is the Evaluator's job, through the entity services.
func NewService(
	policies out.PolicyRepository,
	indicators out.IndicatorRepository,
	actions out.PolicyActionRepository,
	samples out.IndicatorSampleRepository,
	channels out.AsyncChannelRepository,
	clusters out.ClusterRepository,
	topics out.TopicRepository,
) *Service {
	return &Service{
		policies:   policies,
		indicators: indicators,
		actions:    actions,
		samples:    samples,
		resolve:    resolver{channels: channels, clusters: clusters, topics: topics},
	}
}

// --- Policy CRUD ---------------------------------------------------------

// CreatePolicy registers a policy after running every 003.8 write-time check:
// the whitelist, argument arity and shape, `applies_to` vs `matcher.entity`, and
// that `indicator` names a registered Indicator.
func (s *Service) CreatePolicy(
	ctx context.Context, input in.CreatePolicyInput,
) (*gov.Policy, error) {
	r := realm.MustFromContext(ctx)
	if err := gov.ValidateName(input.Name); err != nil {
		return nil, err
	}
	if err := s.validateDefinition(ctx, input.Definition); err != nil {
		return nil, err
	}
	p, err := gov.New(r, input.Name, input.Definition, input.Weight, input.Enabled)
	if err != nil {
		return nil, err
	}
	if err := s.policies.Create(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

// GetPolicy returns one policy by name.
func (s *Service) GetPolicy(ctx context.Context, name string) (*gov.Policy, error) {
	r := realm.MustFromContext(ctx)
	return s.policies.Get(ctx, r.ID, name)
}

// ListPolicies returns one page, ordered by name.
func (s *Service) ListPolicies(
	ctx context.Context, input in.ListPoliciesInput,
) (in.PolicyPage, error) {
	r := realm.MustFromContext(ctx)

	queryKey := pagetoken.QueryKey("policy")
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.PolicyPage{}, err
	}
	page, err := s.policies.List(ctx, out.PolicyQuery{
		RealmID:   r.ID,
		Limit:     pagetoken.ClampSize(input.PageSize),
		AfterName: after,
	})
	if err != nil {
		return in.PolicyPage{}, err
	}
	return in.PolicyPage{
		Policies:      page.Policies,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}

// UpdatePolicy applies the masked fields and re-validates the whole resulting
// definition — a mask that changes only `limit.value` can still invalidate the
// policy if the indicator's unit no longer parses it, so a partial check is not
// enough.
//
// The definition is validated before the row is locked, using the indicator read
// once up front. Re-running the same pure check inside the lock keeps the write
// atomic without issuing a query from inside the transaction.
func (s *Service) UpdatePolicy(
	ctx context.Context, input in.UpdatePolicyInput,
) (*gov.Policy, error) {
	r := realm.MustFromContext(ctx)

	current, err := s.policies.Get(ctx, r.ID, input.Name)
	if err != nil {
		return nil, err
	}
	proposed := applyPolicyMask(current.Definition, input)
	ind, err := s.lookupIndicator(ctx, proposed.Indicator)
	if err != nil {
		return nil, err
	}
	if err := proposed.ValidateAgainst(ind); err != nil {
		return nil, err
	}
	return s.policies.Mutate(ctx, r.ID, input.Name, func(p *gov.Policy) error {
		p.Definition = applyPolicyMask(p.Definition, input)
		if err := p.Definition.ValidateAgainst(ind); err != nil {
			return err
		}
		if input.Weight != nil {
			p.Weight = *input.Weight
		}
		if input.Enabled != nil {
			p.Enabled = *input.Enabled
		}
		return nil
	})
}

// applyPolicyMask overlays the masked definition fields onto a base definition.
// A nil pointer means "leave unchanged" (003.1 FieldMask semantics).
func applyPolicyMask(base gov.Definition, input in.UpdatePolicyInput) gov.Definition {
	if input.Indicator != nil {
		base.Indicator = *input.Indicator
	}
	if input.Matcher != nil {
		base.Matcher = *input.Matcher
	}
	if input.Limit != nil {
		base.Limit = *input.Limit
	}
	if input.Actions != nil {
		base.Actions = *input.Actions
	}
	return base
}

// DeletePolicy removes the policy. Its PolicyAction history survives (003.8).
func (s *Service) DeletePolicy(ctx context.Context, name string) error {
	r := realm.MustFromContext(ctx)
	return s.policies.Delete(ctx, r.ID, name)
}

// validateDefinition runs the domain's write-time checks against the registered
// indicator, resolving "unknown indicator" to a nil Indicator so the domain
// gives the operator one consistent explanation.
func (s *Service) validateDefinition(ctx context.Context, def gov.Definition) error {
	ind, err := s.lookupIndicator(ctx, def.Indicator)
	if err != nil {
		return err
	}
	return def.ValidateAgainst(ind)
}

// lookupIndicator returns the registered indicator, or (nil, nil) when there is
// none. A missing indicator is not an error here: 003.8 wants it reported as a
// rejected *policy*, which ValidateAgainst does with the context of which field
// named it.
func (s *Service) lookupIndicator(ctx context.Context, name string) (*indicator.Indicator, error) {
	if name == "" {
		return nil, nil
	}
	r := realm.MustFromContext(ctx)
	ind, err := s.indicators.Get(ctx, r.ID, name)
	if errs.KindOf(err) == errs.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ind, nil
}

// --- dry run -------------------------------------------------------------

// DryRunPolicy evaluates an inline, unsaved definition against the latest sample
// per resource and reports what *would* happen. It performs no mutation and
// writes no PolicyAction (003.8 "Dry run"), so it deliberately shares only the
// matching and comparison logic with the Evaluator, not the applying half.
//
// A stale indicator is not special-cased: a dry run is a question about the rule,
// not a decision to act, and hiding every match behind "the data is old" would
// make the answer less useful than the freshness the caller can already read off
// GetIndicator.
func (s *Service) DryRunPolicy(
	ctx context.Context, def gov.Definition,
) ([]in.DryRunMatch, error) {
	r := realm.MustFromContext(ctx)

	ind, err := s.lookupIndicator(ctx, def.Indicator)
	if err != nil {
		return nil, err
	}
	if err := def.ValidateAgainst(ind); err != nil {
		return nil, err
	}
	sel, err := def.Matcher.ParseSelector()
	if err != nil {
		return nil, err
	}
	latest, err := s.samples.LatestPerResource(ctx, r.ID, def.Indicator, 0)
	if err != nil {
		return nil, err
	}

	matches := make([]in.DryRunMatch, 0, len(latest))
	for _, sample := range latest {
		if sample.ResourceEntity != def.Matcher.Entity {
			continue
		}
		res, err := s.resolve.resolveFRN(ctx, r.ID, sample.ResourceFRN)
		if err != nil {
			// A sample for a resource that no longer exists is history, not a
			// match. Skipping it keeps a dry run readable instead of failing the
			// whole call on one stale row.
			continue
		}
		if !sel.Match(res.Labels) {
			continue
		}
		// An unparseable value is reported as "would not trigger" rather than
		// failing the dry run: the operator is asking which resources match, and
		// one bad sample should not hide the rest.
		triggers, _ := def.Limit.Triggers(ind.Unit, sample.Value)
		matches = append(matches, in.DryRunMatch{
			ResourceFRN:    sample.ResourceFRN,
			IndicatorValue: sample.Value,
			WouldTrigger:   triggers,
		})
	}
	return matches, nil
}

// --- audit series --------------------------------------------------------

// ListPolicyActions reads one policy's audit series, newest first. It does not
// require the policy to still exist: the rows outlive it (003.8).
func (s *Service) ListPolicyActions(
	ctx context.Context, input in.ListPolicyActionsInput,
) (in.PolicyActionPage, error) {
	r := realm.MustFromContext(ctx)
	if input.PolicyName == "" {
		return in.PolicyActionPage{}, errs.InvalidField("name", "must not be empty")
	}

	queryKey := pagetoken.QueryKey("policy-action", input.PolicyName)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.PolicyActionPage{}, err
	}
	page, err := s.actions.List(ctx, out.PolicyActionQuery{
		RealmID:     r.ID,
		PolicyName:  input.PolicyName,
		Limit:       pagetoken.ClampSize(input.PageSize),
		AfterCursor: after,
	})
	if err != nil {
		return in.PolicyActionPage{}, err
	}
	return in.PolicyActionPage{
		Actions:       page.Actions,
		NextPageToken: pagetoken.Encode(page.LastCursor, queryKey),
	}, nil
}

// --- Indicator registry --------------------------------------------------

// CreateIndicator registers an indicator. Registration is an admin action; there
// is no auto-creation from a sample (003.14).
func (s *Service) CreateIndicator(
	ctx context.Context, input in.CreateIndicatorInput,
) (*indicator.Indicator, error) {
	r := realm.MustFromContext(ctx)
	if err := indicator.ValidateName(input.Name); err != nil {
		return nil, err
	}
	i, err := indicator.NewIndicator(r, input.Name, input.Unit, input.AppliesTo,
		input.StalenessThreshold, input.SourceAgents)
	if err != nil {
		return nil, err
	}
	if err := s.indicators.Create(ctx, i); err != nil {
		return nil, err
	}
	return i, nil
}

// GetIndicator returns one indicator by name.
func (s *Service) GetIndicator(ctx context.Context, name string) (*indicator.Indicator, error) {
	r := realm.MustFromContext(ctx)
	return s.indicators.Get(ctx, r.ID, name)
}

// ListIndicators returns one page, ordered by name.
func (s *Service) ListIndicators(
	ctx context.Context, input in.ListIndicatorsInput,
) (in.IndicatorPage, error) {
	r := realm.MustFromContext(ctx)

	queryKey := pagetoken.QueryKey("indicator")
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.IndicatorPage{}, err
	}
	page, err := s.indicators.List(ctx, out.IndicatorQuery{
		RealmID:   r.ID,
		Limit:     pagetoken.ClampSize(input.PageSize),
		AfterName: after,
	})
	if err != nil {
		return in.IndicatorPage{}, err
	}
	return in.IndicatorPage{
		Indicators:    page.Indicators,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}

// UpdateIndicator applies the masked fields. `applies_to` is not among them: it
// is immutable (003.14), because changing it would silently invalidate every
// policy bound to the indicator and every sample already stored against it.
func (s *Service) UpdateIndicator(
	ctx context.Context, input in.UpdateIndicatorInput,
) (*indicator.Indicator, error) {
	r := realm.MustFromContext(ctx)
	return s.indicators.Mutate(ctx, r.ID, input.Name, func(i *indicator.Indicator) error {
		if input.Unit != nil {
			if err := i.SetUnit(*input.Unit); err != nil {
				return err
			}
		}
		if input.StalenessThreshold != nil {
			if err := i.SetStalenessThreshold(*input.StalenessThreshold); err != nil {
				return err
			}
		}
		if input.SourceAgents != nil {
			if err := i.SetSourceAgents(*input.SourceAgents); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteIndicator refuses while any policy — enabled or not — still names the
// indicator (003.8). Deleting it out from under a policy would leave a rule that
// can never be evaluated and can never be repaired, since UpdatePolicy would
// reject the same unknown name.
func (s *Service) DeleteIndicator(ctx context.Context, name string) error {
	r := realm.MustFromContext(ctx)

	if _, err := s.indicators.Get(ctx, r.ID, name); err != nil {
		return err
	}
	n, err := s.policies.CountByIndicator(ctx, r.ID, name)
	if err != nil {
		return err
	}
	if n > 0 {
		return errs.Preconditionf(
			"indicator %q is referenced by %d polic(ies) — delete or repoint them first",
			name, n)
	}
	return s.indicators.Delete(ctx, r.ID, name)
}

// ListIndicatorSamples reads the append-only sample history, newest first
// (003.14). The series is 30-day pruned, so an empty page is a normal answer for
// an indicator nothing has published to lately.
func (s *Service) ListIndicatorSamples(
	ctx context.Context, input in.ListIndicatorSamplesInput,
) (in.IndicatorSamplePage, error) {
	r := realm.MustFromContext(ctx)
	if input.Indicator == "" {
		return in.IndicatorSamplePage{}, errs.InvalidField("indicator", "must not be empty")
	}
	if !input.From.IsZero() && !input.To.IsZero() && input.To.Before(input.From) {
		return in.IndicatorSamplePage{}, errs.InvalidField("to", "must not be before `from`")
	}

	queryKey := pagetoken.QueryKey("indicator-sample", input.Indicator, input.ResourceFRN,
		input.From.String(), input.To.String())
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.IndicatorSamplePage{}, err
	}
	page, err := s.samples.List(ctx, out.SampleQuery{
		RealmID:     r.ID,
		Indicator:   input.Indicator,
		ResourceFRN: input.ResourceFRN,
		From:        input.From,
		To:          input.To,
		Limit:       pagetoken.ClampSize(input.PageSize),
		AfterCursor: after,
	})
	if err != nil {
		return in.IndicatorSamplePage{}, err
	}
	return in.IndicatorSamplePage{
		Samples:       page.Samples,
		NextPageToken: pagetoken.Encode(page.LastCursor, queryKey),
	}, nil
}
