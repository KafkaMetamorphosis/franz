package out

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
)

// IndicatorQuery parameterises IndicatorRepository.List.
type IndicatorQuery struct {
	RealmID   uuid.UUID
	Limit     int    // page size (already clamped)
	AfterName string // exclusive lower bound, "" ⇒ first page
}

// IndicatorPage is one page of a List result, ordered by name ascending.
type IndicatorPage struct {
	Indicators []*indicator.Indicator
	LastName   string
	TotalSize  int
}

// IndicatorRepository persists the pre-registered Indicator registry (003.14).
// Realm scoping is the caller's responsibility.
type IndicatorRepository interface {
	// Create inserts a new row. A name/FRN collision is errs.AlreadyExists.
	Create(ctx context.Context, i *indicator.Indicator) error

	// Get returns the indicator by (realm, name). errs.NotFound if absent.
	Get(ctx context.Context, realmID uuid.UUID, name string) (*indicator.Indicator, error)

	// List returns one page per IndicatorQuery.
	List(ctx context.Context, q IndicatorQuery) (IndicatorPage, error)

	// Mutate loads the row FOR UPDATE, runs mutate, and persists the result in
	// one transaction. `applies_to` is not written back — it is immutable
	// (003.14) — so a mutate that changes it has no effect.
	Mutate(ctx context.Context, realmID uuid.UUID, name string,
		mutate func(*indicator.Indicator) error) (*indicator.Indicator, error)

	// Delete removes the row. errs.NotFound if absent. The caller checks the
	// policy-reference guard first (003.8).
	Delete(ctx context.Context, realmID uuid.UUID, name string) error

	// RecordSample updates the current-value projection — current_value,
	// current_resource_frn, last_sample_at — when sampleAt is newer than the
	// stored last_sample_at. An out-of-order sample is stored as history but does
	// not become "current" (003.14), so this reports whether it advanced.
	// Deliverable 15's ingest path is the caller; deliverable 14 owns the columns.
	RecordSample(ctx context.Context, realmID uuid.UUID, name, resourceFRN, value string,
		sampleAt time.Time) (advanced bool, err error)
}

// PolicyQuery parameterises PolicyRepository.List.
type PolicyQuery struct {
	RealmID   uuid.UUID
	Limit     int
	AfterName string
}

// PolicyPage is one page of a List result, ordered by name ascending.
type PolicyPage struct {
	Policies  []*governance.Policy
	LastName  string
	TotalSize int
}

// PolicyRepository persists governance Policies (003.8 / 003.12).
type PolicyRepository interface {
	// Create inserts a new row. A name/FRN collision is errs.AlreadyExists.
	Create(ctx context.Context, p *governance.Policy) error

	// Get returns the policy by (realm, name). errs.NotFound if absent.
	Get(ctx context.Context, realmID uuid.UUID, name string) (*governance.Policy, error)

	// List returns one page per PolicyQuery.
	List(ctx context.Context, q PolicyQuery) (PolicyPage, error)

	// Mutate loads the row FOR UPDATE, runs mutate, and persists the result in
	// one transaction (003.12 "Concurrency").
	Mutate(ctx context.Context, realmID uuid.UUID, name string,
		mutate func(*governance.Policy) error) (*governance.Policy, error)

	// Delete removes the policy row. Its policy_action audit rows survive — they
	// carry no foreign key (003.8 "Auditability").
	Delete(ctx context.Context, realmID uuid.UUID, name string) error

	// ListEnabledByIndicator returns every enabled policy in the realm bound to
	// indicatorName, ordered by name. It is the evaluation pass's work list; a
	// realm's policy count is bounded, so it is not paginated.
	ListEnabledByIndicator(ctx context.Context, realmID uuid.UUID, indicatorName string) ([]*governance.Policy, error)

	// CountByIndicator counts the policies — enabled or not — referencing
	// indicatorName. DeleteIndicator refuses while it is non-zero (003.8).
	CountByIndicator(ctx context.Context, realmID uuid.UUID, indicatorName string) (int, error)

	// MarkFired stamps last_fired_at on the policy. Separate from Mutate because
	// it runs after the actions were applied and must not re-run validation.
	MarkFired(ctx context.Context, realmID uuid.UUID, name string, at time.Time) error
}

// PolicyActionQuery parameterises PolicyActionRepository.List. Rows come back
// newest first.
type PolicyActionQuery struct {
	RealmID    uuid.UUID
	PolicyName string
	Limit      int
	// AfterCursor is the opaque "<occurred_at>|<id>" position of the last row of
	// the previous page, "" ⇒ first page.
	AfterCursor string
}

// PolicyActionPage is one page of the audit series.
type PolicyActionPage struct {
	Actions []*governance.ActionRecord
	// LastCursor positions the next page ("" ⇒ no more rows).
	LastCursor string
}

// PolicyActionRepository persists the append-only policy_action audit series
// (003.8 "Auditability", 003.12 retention).
type PolicyActionRepository interface {
	// Append writes a batch in one round trip.
	Append(ctx context.Context, records []*governance.ActionRecord) error

	// List returns one page per PolicyActionQuery, newest first.
	List(ctx context.Context, q PolicyActionQuery) (PolicyActionPage, error)

	// PruneOlderThan deletes rows with occurred_at < cutoff (nightly 30-day
	// prune) and returns the count removed.
	PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// SampleQuery parameterises IndicatorSampleRepository.List — the
// ListIndicatorSamples history read (003.14). ResourceFRN is optional; From/To
// are optional bounds on sample_at.
type SampleQuery struct {
	RealmID     uuid.UUID
	Indicator   string
	ResourceFRN string
	From        time.Time
	To          time.Time
	Limit       int
	AfterCursor string
}

// SamplePage is one page of the sample history, newest first.
type SamplePage struct {
	Samples    []*indicator.Sample
	LastCursor string
}
