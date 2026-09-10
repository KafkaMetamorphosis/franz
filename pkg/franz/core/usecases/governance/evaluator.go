package governance

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// Evaluator implements in.GovernanceEvaluator — the event-driven evaluation pass
// of 003.8. Telemetry ingest calls Evaluate in-process once a sample has
// advanced the current value for (indicator, resource); it is not a scheduled
// sweep, so a policy only ever reacts to a sample that actually arrived.
//
// There is no cooldown or hysteresis (003.8 OQ2, a deliberate documented gap):
// a policy re-fires as fast as samples arrive, and the per-action caps (cap.go)
// are the only bound on where that lands.
type Evaluator struct {
	policies   out.PolicyRepository
	indicators out.IndicatorRepository
	actions    out.PolicyActionRepository
	apply      applier
	resolve    resolver
	log        *slog.Logger
	// now is the clock, injectable so a test can drive staleness. Defaults to
	// time.Now.
	now func() time.Time
}

var _ in.GovernanceEvaluator = (*Evaluator)(nil)

// NewEvaluator wires the evaluation pass. The three entity services are how it
// changes declared state — it never calls an agent (003.8 invariant) — and the
// topic repository plus notifier cover the shard fields no service exposes.
func NewEvaluator(
	policies out.PolicyRepository,
	indicators out.IndicatorRepository,
	actions out.PolicyActionRepository,
	channelRepo out.AsyncChannelRepository,
	clusterRepo out.ClusterRepository,
	topicRepo out.TopicRepository,
	channelSvc in.AsyncChannelService,
	clusterSvc in.KafkaClusterService,
	topicSvc in.KafkaTopicService,
	notifier out.PartitionNotifier,
	log *slog.Logger,
) *Evaluator {
	return &Evaluator{
		policies:   policies,
		indicators: indicators,
		actions:    actions,
		apply: applier{
			channels:    channelSvc,
			clusters:    clusterSvc,
			topics:      topicSvc,
			topicRepo:   topicRepo,
			clusterRepo: clusterRepo,
			notifier:    notifier,
		},
		resolve: resolver{channels: channelRepo, clusters: clusterRepo, topics: topicRepo},
		log:     log,
		now:     time.Now,
	}
}

// Evaluate runs one pass for (indicatorName, resourceFRN, newValue).
//
// It returns an error only when the pass could not run at all — the indicator is
// unreadable, the work list is unreadable, the resource cannot be resolved. A
// policy that fails to *apply* is recorded as a PolicyAction carrying the failure
// in `result` and the pass continues, because telemetry ingest must not be
// blocked by one bad rule (003.8 "Auditability": there is no silent mutation,
// and that includes a mutation that silently failed).
func (e *Evaluator) Evaluate(ctx context.Context, indicatorName, resourceFRN, newValue string) error {
	r := realm.MustFromContext(ctx)
	now := e.clock()

	ind, err := e.indicators.Get(ctx, r.ID, indicatorName)
	if err != nil {
		return err
	}
	// Step 1 of 003.8 "Evaluation": a stale indicator never acts. The freshness
	// checked is the indicator's own last_sample_at, which ingest has already
	// advanced with this very sample, so the guard catches an indicator whose
	// *producer* has gone quiet — not the sample in hand.
	if ind.Health(now) == indicator.HealthStale {
		e.log.Debug("governance: indicator is stale, not acting",
			"indicator", indicatorName, "resource", resourceFRN)
		return nil
	}

	candidates, err := e.policies.ListEnabledByIndicator(ctx, r.ID, indicatorName)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}

	res, err := e.resolve.resolveFRN(ctx, r.ID, resourceFRN)
	if err != nil {
		return err
	}

	triggered := e.selectTriggered(candidates, res, ind, newValue, indicatorName, resourceFRN)
	if len(triggered) == 0 {
		return nil
	}
	// 003.8 "Evaluation": when more than one triggered policy targets the same
	// (resource, field) they are applied in (weight desc, name asc) order and the
	// last write wins. Ordering the whole triggered set — not just the contested
	// pairs — is what makes the pass deterministic to reason about and to test.
	gov.Order(triggered)

	return e.applyAll(ctx, r.ID, res, triggered, newValue, now)
}

// selectTriggered narrows the work list to the policies whose matcher selects
// this resource and whose limit the new value crosses (003.8 steps 2 and 3).
func (e *Evaluator) selectTriggered(
	candidates []*gov.Policy, res *resource, ind *indicator.Indicator,
	newValue, indicatorName, resourceFRN string,
) []*gov.Policy {
	var triggered []*gov.Policy
	for _, p := range candidates {
		if p.Matcher.Entity != res.Entity {
			continue
		}
		sel, err := p.Matcher.ParseSelector()
		if err != nil {
			// A stored selector that no longer parses is a corrupt rule, not a
			// reason to abandon the other policies watching this indicator.
			e.log.Warn("governance: policy has an unparseable selector, skipping",
				"policy", p.Name, "err", err)
			continue
		}
		if !sel.Match(res.Labels) {
			continue
		}
		crosses, err := p.Limit.Triggers(ind.Unit, newValue)
		if err != nil {
			e.log.Warn("governance: cannot compare sample to limit, skipping policy",
				"policy", p.Name, "indicator", indicatorName, "resource", resourceFRN, "err", err)
			continue
		}
		if crosses {
			triggered = append(triggered, p)
		}
	}
	return triggered
}

// applyAll runs every triggered policy's actions in order, recording one
// PolicyAction per action and stamping last_fired_at. Failures are logged into
// the audit row, never returned: the batch of records is written even when some
// of the actions in it failed.
func (e *Evaluator) applyAll(
	ctx context.Context, realmID uuid.UUID, res *resource, triggered []*gov.Policy,
	newValue string, now time.Time,
) error {
	var records []*gov.ActionRecord
	fired := make([]*gov.Policy, 0, len(triggered))

	for _, p := range triggered {
		for _, action := range p.Actions {
			result, err := e.apply.apply(ctx, realmID, res, action)
			if err != nil {
				result = "failed: " + err.Error()
				e.log.Warn("governance: action failed",
					"policy", p.Name, "resource", res.FRN.Path(),
					"action", string(action.Kind), "err", err)
			}
			records = append(records, gov.NewActionRecord(
				p.RealmID, p.ID, p.Name, res.FRN.Path(), newValue, action, result, now))
		}
		fired = append(fired, p)
	}

	if err := e.actions.Append(ctx, records); err != nil {
		// The mutations already happened. Losing the audit row is the one failure
		// 003.8 calls out by name ("there is no silent mutation"), so it is
		// surfaced rather than swallowed.
		return errs.Internalf("governance: applied actions but could not record them").Wrap(err)
	}
	for _, p := range fired {
		if err := e.policies.MarkFired(ctx, p.RealmID, p.Name, now); err != nil {
			e.log.Warn("governance: could not stamp last_fired_at",
				"policy", p.Name, "err", err)
		}
	}
	return nil
}

func (e *Evaluator) clock() time.Time {
	if e.now == nil {
		return time.Now().UTC()
	}
	return e.now().UTC()
}

// NoopEvaluator is a GovernanceEvaluator that does nothing. It exists so a
// caller that has to hold the port — a test harness, or a deployment with
// governance switched off — can do so without a store behind it.
type NoopEvaluator struct{}

var _ in.GovernanceEvaluator = NoopEvaluator{}

// Evaluate does nothing and reports success.
func (NoopEvaluator) Evaluate(context.Context, string, string, string) error { return nil }
