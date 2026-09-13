// Package governance is the reactive-policy domain (003.8): a Policy watches one
// Indicator, and when the latest sample crosses a Limit it runs whitelisted
// Actions on every resource its Matcher selects.
//
// It is pure logic with no I/O. Governance is *reactive only* — a policy never
// rejects or delays a Create/Update; it mutates Franz's declared state after the
// fact and the normal reconciliation path (003.6) realises the change.
package governance

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/naming"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
)

// Operator is the comparison a Limit applies to the latest indicator value
// (003.8 `Operator`).
type Operator string

const (
	OpLessThan           Operator = "LESS_THAN"
	OpLessThanOrEqual    Operator = "LESS_THAN_OR_EQUAL"
	OpEqual              Operator = "EQUAL"
	OpNotEqual           Operator = "NOT_EQUAL"
	OpGreaterThanOrEqual Operator = "GREATER_THAN_OR_EQUAL"
	OpGreaterThan        Operator = "GREATER_THAN"
)

// Valid reports whether o is a known operator.
func (o Operator) Valid() bool {
	switch o {
	case OpLessThan, OpLessThanOrEqual, OpEqual, OpNotEqual,
		OpGreaterThanOrEqual, OpGreaterThan:
		return true
	default:
		return false
	}
}

// holds applies the operator to the result of value.Compare(limit).
func (o Operator) holds(cmp int) bool {
	switch o {
	case OpLessThan:
		return cmp < 0
	case OpLessThanOrEqual:
		return cmp <= 0
	case OpEqual:
		return cmp == 0
	case OpNotEqual:
		return cmp != 0
	case OpGreaterThanOrEqual:
		return cmp >= 0
	case OpGreaterThan:
		return cmp > 0
	default:
		return false
	}
}

// ActionKind is what an Action does (003.8 `ActionKind`).
type ActionKind string

const (
	ActionAddLabel        ActionKind = "ADD_LABEL"
	ActionRemoveLabel     ActionKind = "REMOVE_LABEL"
	ActionSetStatus       ActionKind = "SET_STATUS"
	ActionUpdateField     ActionKind = "UPDATE_FIELD"
	ActionIncreaseFieldBy ActionKind = "INCREASE_FIELD_BY"
	ActionDecreaseFieldBy ActionKind = "DECREASE_FIELD_BY"
)

// Valid reports whether k is a known action kind.
func (k ActionKind) Valid() bool {
	switch k {
	case ActionAddLabel, ActionRemoveLabel, ActionSetStatus,
		ActionUpdateField, ActionIncreaseFieldBy, ActionDecreaseFieldBy:
		return true
	default:
		return false
	}
}

// arity is the positional `args` count each kind takes (003.8 "Action"). The
// two arithmetic kinds accept an optional third arg — the per-action cap, see
// cap.go — so their entry is the minimum.
var arity = map[ActionKind]int{
	ActionAddLabel:        2, // [key, value]
	ActionRemoveLabel:     1, // [key]
	ActionSetStatus:       1, // [status]
	ActionUpdateField:     2, // [field, value]
	ActionIncreaseFieldBy: 2, // [field, amount] (+ optional cap)
	ActionDecreaseFieldBy: 2, // [field, amount] (+ optional cap)
}

// IsArithmetic reports whether the kind changes a field by a delta rather than
// setting it outright — the two kinds that take a cap.
func (k ActionKind) IsArithmetic() bool {
	return k == ActionIncreaseFieldBy || k == ActionDecreaseFieldBy
}

// Action is one whitelisted change a triggered policy makes.
type Action struct {
	Kind ActionKind
	Args []string
}

// Target is the args[0] of a field / label action — the field path or the label
// key. Empty for SET_STATUS, whose single arg is the status.
func (a Action) Target() string {
	if a.Kind == ActionSetStatus || len(a.Args) == 0 {
		return ""
	}
	return a.Args[0]
}

// Matcher selects the resources a policy acts on (003.8 `Matcher`): an entity
// kind plus a 003.1 label selector. An empty selector matches every resource of
// that kind.
type Matcher struct {
	Entity   indicator.Entity
	Selector string
}

// ParseSelector parses the matcher's selector expression.
func (m Matcher) ParseSelector() (selector.Selector, error) {
	return selector.Parse(m.Selector)
}

// Limit is the threshold the latest indicator value is compared against. Value
// is a string encoded in the indicator's unit (003.8: `"150Gi"`, `"3"`, `"90d"`).
// A lower+upper band is two policies.
type Limit struct {
	Operator Operator
	Value    string
}

// Definition is the policy body shared by CreatePolicy, UpdatePolicy and
// DryRunPolicy — everything that decides *whether and what* a policy does, with
// none of the identity (`name`) or scheduling (`weight`, `enabled`) around it.
type Definition struct {
	Indicator string
	Matcher   Matcher
	Limit     Limit
	Actions   []Action
}

// Policy is a stored governance rule.
type Policy struct {
	ID      uuid.UUID // surrogate key; assigned by the repository on Create
	FRN     frn.FRN
	RealmID uuid.UUID
	Name    string

	Definition

	// Weight breaks ties when several triggered policies write the same
	// (resource, field). Higher wins; equal weights fall back to name ascending
	// (003.8 "Evaluation").
	Weight int32
	// Enabled false means the policy never triggers.
	Enabled bool
	// LastFiredAt is stamped every time the policy triggers. nil until it does.
	LastFiredAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// New builds a Policy with an FRN assigned. It runs the structural half of
// validation only; ValidateAgainst couples it to the registered indicator and
// must be called before the policy is persisted.
func New(
	r realm.Realm, name string, def Definition, weight int32, enabled bool,
) (*Policy, error) {
	id, err := frn.New(r.Slug, frn.TypePolicy, name)
	if err != nil {
		return nil, err
	}
	return &Policy{
		FRN:        id,
		RealmID:    r.ID,
		Name:       name,
		Definition: def,
		Weight:     weight,
		Enabled:    enabled,
	}, nil
}

// ValidateAgainst enforces every 003.8 write-time invariant on a definition.
// ind is the registered Indicator the definition names, or nil when there is no
// such indicator — which is itself one of the rejections (003.14: "CreatePolicy
// referencing an unknown indicator is rejected").
//
// The checks, in the order a caller most wants to hear about them:
//
//	indicator named          → INVALID_ARGUMENT
//	indicator registered     → FAILED_PRECONDITION
//	matcher.entity set       → INVALID_ARGUMENT
//	applies_to agrees        → INVALID_ARGUMENT
//	selector parses          → INVALID_ARGUMENT
//	limit operator + value   → INVALID_ARGUMENT
//	actions non-empty        → INVALID_ARGUMENT
//	each action: kind, arity, whitelist, cap, argument shape
//	                         → INVALID_ARGUMENT, or FAILED_PRECONDITION for a
//	                           whitelisted action this deliverable defers
func (d Definition) ValidateAgainst(ind *indicator.Indicator) error {
	if d.Indicator == "" {
		return errs.InvalidField("indicator", "must not be empty")
	}
	if ind == nil {
		return errs.Preconditionf(
			"indicator %q is not registered — register it with CreateIndicator first", d.Indicator)
	}
	if !d.Matcher.Entity.Valid() {
		return errs.InvalidField("matcher.entity",
			"must be one of ASYNC_CHANNEL, KAFKA_TOPIC, KAFKA_CLUSTER")
	}
	if ind.AppliesTo != d.Matcher.Entity {
		return errs.InvalidField("matcher.entity",
			"indicator "+ind.Name+" applies to "+string(ind.AppliesTo)+
				", so the matcher cannot select "+string(d.Matcher.Entity))
	}
	if _, err := d.Matcher.ParseSelector(); err != nil {
		return err
	}
	if !d.Limit.Operator.Valid() {
		return errs.InvalidField("limit.operator", "must not be UNSPECIFIED")
	}
	if _, err := indicator.ParseValue(ind.Unit, "limit.value", d.Limit.Value); err != nil {
		return err
	}
	if len(d.Actions) == 0 {
		return errs.InvalidField("actions", "must not be empty")
	}
	for i, a := range d.Actions {
		if err := validateAction(d.Matcher.Entity, i, a); err != nil {
			return err
		}
	}
	return nil
}

// Triggers reports whether value — the latest sample for the resource, encoded
// in unit — crosses the limit. An unparseable value or limit is an error, not a
// silent false: a policy that cannot be evaluated must be visible.
func (l Limit) Triggers(unit indicator.Unit, value string) (bool, error) {
	got, err := indicator.ParseValue(unit, "value", value)
	if err != nil {
		return false, err
	}
	want, err := indicator.ParseValue(unit, "limit.value", l.Value)
	if err != nil {
		return false, err
	}
	cmp, err := got.Compare(want)
	if err != nil {
		return false, err
	}
	return l.Operator.holds(cmp), nil
}

// MarkFired stamps last_fired_at (003.8 evaluation step 4).
func (p *Policy) MarkFired(at time.Time) { t := at.UTC(); p.LastFiredAt = &t }

// ValidateName is exposed so a handler can reject a bad policy name before
// touching the store.
func ValidateName(name string) error { return naming.Validate(name) }

// Order sorts triggered policies into the application order 003.8 mandates for
// one evaluation pass: **weight descending, then name ascending**. Actions are
// applied by walking the result front to back and the last write wins, so on a
// contested (resource, field) the policy that ends up last — the lowest weight,
// or the highest name among equals — is the one whose value survives.
//
// NOTE (owed 003.8 edit): the spec is internally inconsistent here. "Key
// shapes" and governance.proto both describe `weight` as "higher wins", but
// "Evaluation" prescribes (weight desc, name asc) *with last-write-wins*, which
// makes the lowest weight win. The literal Evaluation text is implemented
// because it is the operative rule and the one the ordering is tested against;
// resolving the contradiction — either order ascending, or keep the first write
// — is a spec decision, not one to make silently in code.
func Order(policies []*Policy) {
	sort.SliceStable(policies, func(i, j int) bool {
		if policies[i].Weight != policies[j].Weight {
			return policies[i].Weight > policies[j].Weight // weight desc
		}
		return policies[i].Name < policies[j].Name // name asc
	})
}
