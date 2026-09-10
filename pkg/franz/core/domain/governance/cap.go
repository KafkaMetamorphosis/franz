package governance

import (
	"math"
	"strconv"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
)

// Per-action caps — the resolution of 003.8 OQ1.
//
// 003.8 leaves the cap encoding open ("absolute ceiling, max delta per fire,
// floor") while relying on it as "the only bound" on a policy that re-fires as
// fast as samples arrive (OQ2 defers anti-thrash). The encoding chosen here is
// an **optional third positional arg** on the two arithmetic actions:
//
//	INCREASE_FIELD_BY [field, amount, "max=<ceiling>"]
//	DECREASE_FIELD_BY [field, amount, "min=<floor>"]
//
// It is an **absolute ceiling / floor on the resulting field value**, not a
// max delta per fire: a bound on the field is a bound on the fleet no matter how
// often the policy fires, whereas a per-fire delta bounds nothing over time.
//
// Why per action rather than per policy: `Action.args` is already
// `repeated string`, so this needs no proto change, and a policy whose actions
// touch several fields needs a cap per field, not one shared number. The cost is
// that the cap is positional and stringly typed like the rest of `args`.
//
// The cap is optional in general and **required** for
// `INCREASE_FIELD_BY partitions` (see requireCap): that increase is
// irreversible under 003.6.
//
// At apply time a cap **clamps** rather than fails: the field is moved as far as
// the cap allows and the PolicyAction records what happened. A policy that has
// driven a field to its ceiling then becomes a no-op, which is the intended
// steady state — not an error to page someone about.
const (
	capPrefixMax = "max="
	capPrefixMin = "min="
)

// CapKind is which side a Cap bounds.
type CapKind string

const (
	// CapMax is the ceiling an INCREASE_FIELD_BY may not push the field past.
	CapMax CapKind = "max"
	// CapMin is the floor a DECREASE_FIELD_BY may not push the field below.
	CapMin CapKind = "min"
)

// Cap is the parsed optional third arg of an arithmetic action.
type Cap struct {
	Present bool
	Kind    CapKind
	// Raw is the operator-authored bound, kept verbatim for the audit record.
	Raw string
	// Limit is Raw decoded as a quantity, so it compares against a field value.
	Limit float64
}

// ParseCap reads the optional cap off an arithmetic action's args. A non
// arithmetic action, or one with no third arg, yields an absent Cap.
func ParseCap(a Action) (Cap, error) {
	if !a.Kind.IsArithmetic() || len(a.Args) < 3 {
		return Cap{}, nil
	}
	raw := strings.TrimSpace(a.Args[2])
	switch {
	case strings.HasPrefix(raw, capPrefixMax):
		if a.Kind != ActionIncreaseFieldBy {
			return Cap{}, errs.Invalidf("a %q cap only applies to INCREASE_FIELD_BY", capPrefixMax)
		}
		return parseCapValue(CapMax, raw, strings.TrimPrefix(raw, capPrefixMax))
	case strings.HasPrefix(raw, capPrefixMin):
		if a.Kind != ActionDecreaseFieldBy {
			return Cap{}, errs.Invalidf("a %q cap only applies to DECREASE_FIELD_BY", capPrefixMin)
		}
		return parseCapValue(CapMin, raw, strings.TrimPrefix(raw, capPrefixMin))
	default:
		return Cap{}, errs.Invalidf(
			"cap must be %q<ceiling> on INCREASE_FIELD_BY or %q<floor> on DECREASE_FIELD_BY (got %q)",
			capPrefixMax, capPrefixMin, raw)
	}
}

func parseCapValue(kind CapKind, raw, value string) (Cap, error) {
	n, err := indicator.ParseQuantity(value)
	if err != nil {
		return Cap{}, errs.Invalidf("cap %q is not a number", raw)
	}
	return Cap{Present: true, Kind: kind, Raw: raw, Limit: n}, nil
}

// Clamp bounds want to the cap, returning the value to write and whether the cap
// bit. An absent cap passes want through.
func (c Cap) Clamp(want float64) (float64, bool) {
	if !c.Present {
		return want, false
	}
	switch c.Kind {
	case CapMax:
		if want > c.Limit {
			return c.Limit, true
		}
	case CapMin:
		if want < c.Limit {
			return c.Limit, true
		}
	}
	return want, false
}

// Amount is the `amount` arg of INCREASE_FIELD_BY / DECREASE_FIELD_BY: absolute
// ("2", "10Gi") or a percentage of the field's current value ("50%") — 003.8
// "Action".
type Amount struct {
	Percent bool
	Value   float64
}

// ParseAmount decodes an amount. An absolute amount uses the same quantity
// grammar as an indicator byte value, so "10Gi" works on a byte-valued config
// key and "2" on a partition count.
func ParseAmount(raw string) (Amount, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Amount{}, errs.Invalidf("amount must not be empty")
	}
	if pct, ok := strings.CutSuffix(s, "%"); ok {
		n, err := strconv.ParseFloat(strings.TrimSpace(pct), 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return Amount{}, errs.Invalidf("amount %q is not a percentage", raw)
		}
		if n < 0 {
			return Amount{}, errs.Invalidf("amount %q must not be negative", raw)
		}
		return Amount{Percent: true, Value: n}, nil
	}
	n, err := indicator.ParseQuantity(s)
	if err != nil {
		return Amount{}, errs.Invalidf("amount %q is not a number", raw)
	}
	if n < 0 {
		return Amount{}, errs.Invalidf("amount %q must not be negative", raw)
	}
	return Amount{Percent: false, Value: n}, nil
}

// Delta is how much the amount moves a field whose current value is current.
func (a Amount) Delta(current float64) float64 {
	if a.Percent {
		return current * a.Value / 100
	}
	return a.Value
}

// ApplyArithmetic computes the new value of a numeric field: current moved by
// amount in the direction kind names, clamped by bound. It returns the result
// and whether the cap bit.
func ApplyArithmetic(kind ActionKind, current float64, amount Amount, bound Cap) (float64, bool) {
	delta := amount.Delta(current)
	want := current + delta
	if kind == ActionDecreaseFieldBy {
		want = current - delta
	}
	return bound.Clamp(want)
}
