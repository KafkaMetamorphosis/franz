package indicator

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// Unit is the encoding of an indicator's sample values and of the Limit a policy
// compares against (003.8: `"150Gi"`, `"3"`, `"90d"`). The set is open — an
// agent may publish any unit name — but Franz has to know how to *compare* two
// values, so every unit resolves to one of the closed comparison families below.
// An unrecognised unit falls back to Numeric.
type Unit string

// The units Franz interprets specially. Any other non-empty string is accepted
// and compared numerically.
const (
	UnitCount    Unit = "count"
	UnitBytes    Unit = "bytes"
	UnitDuration Unit = "duration"
	UnitBoolean  Unit = "boolean"
	UnitPercent  Unit = "percent"
	UnitRatio    Unit = "ratio"
)

// maxUnitLen bounds the free-form unit string.
const maxUnitLen = 64

// Validate rejects an empty or over-long unit. Any other value is accepted:
// Franz classifies rather than enumerates (003.8 "unit": `"bytes"`, `"count"`,
// `"duration"`, `"boolean"`, …).
func (u Unit) Validate() error {
	s := strings.TrimSpace(string(u))
	if s == "" {
		return errs.InvalidField("unit", "must not be empty")
	}
	if len(s) > maxUnitLen {
		return errs.InvalidField("unit",
			fmt.Sprintf("must be at most %d characters", maxUnitLen))
	}
	return nil
}

// Family is the comparison family a Unit resolves to.
type Family int

const (
	// FamilyNumeric covers counts, percentages, ratios and any unrecognised
	// unit: the value is a decimal number.
	FamilyNumeric Family = iota
	// FamilyBytes is a byte size, with the SI ("1G") and binary ("1Gi")
	// suffixes Kubernetes-style quantities use.
	FamilyBytes
	// FamilyDuration is a duration string ("90d", "5m"), compared as nanoseconds.
	FamilyDuration
	// FamilyBoolean is "true" / "false", compared as 0 / 1 so ordering operators
	// stay total.
	FamilyBoolean
)

// Family classifies the unit. The match is case-insensitive and tolerates the
// common aliases an agent is likely to publish.
func (u Unit) Family() Family {
	switch strings.ToLower(strings.TrimSpace(string(u))) {
	case string(UnitBytes), "byte", "b", "bytes/s", "byte_size":
		return FamilyBytes
	case string(UnitDuration), "time", "lag", "age":
		return FamilyDuration
	case string(UnitBoolean), "bool":
		return FamilyBoolean
	default:
		return FamilyNumeric
	}
}

// Value is a parsed indicator value or policy limit, normalised to a float so
// two values in the same family are totally ordered. Family is carried so a
// boolean can never be silently compared against a byte size.
type Value struct {
	Family Family
	Num    float64
}

// ParseValue decodes raw in unit's family. field names the request field the
// error is attributed to ("value", "limit.value", …).
func ParseValue(unit Unit, field, raw string) (Value, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Value{}, errs.InvalidField(field, "must not be empty")
	}
	fam := unit.Family()
	switch fam {
	case FamilyBoolean:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return Value{}, errs.InvalidField(field,
				"must be a boolean for unit "+string(unit)+" (got "+raw+")")
		}
		n := 0.0
		if b {
			n = 1
		}
		return Value{Family: fam, Num: n}, nil
	case FamilyDuration:
		d, err := ParseDurationSpec(s)
		if err != nil {
			return Value{}, errs.InvalidField(field,
				"must be a duration for unit "+string(unit)+" (got "+raw+")")
		}
		return Value{Family: fam, Num: float64(d)}, nil
	case FamilyBytes:
		n, err := ParseQuantity(s)
		if err != nil {
			return Value{}, errs.InvalidField(field,
				"must be a byte size for unit "+string(unit)+" (got "+raw+")")
		}
		return Value{Family: fam, Num: n}, nil
	default:
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return Value{}, errs.InvalidField(field,
				"must be a number for unit "+string(unit)+" (got "+raw+")")
		}
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return Value{}, errs.InvalidField(field, "must be finite (got "+raw+")")
		}
		return Value{Family: fam, Num: n}, nil
	}
}

// Compare returns -1, 0 or +1. Values from different families are not
// comparable, which can only happen if an indicator's unit changed under a
// stored limit; the caller surfaces it rather than guessing.
func (v Value) Compare(o Value) (int, error) {
	if v.Family != o.Family {
		return 0, errs.Invalidf("cannot compare values of different units")
	}
	switch {
	case v.Num < o.Num:
		return -1, nil
	case v.Num > o.Num:
		return 1, nil
	default:
		return 0, nil
	}
}

// quantitySuffixes are the byte-size multipliers, longest first so "Ki" is
// matched before "K".
var quantitySuffixes = []struct {
	suffix string
	mult   float64
}{
	{"Ei", 1 << 60}, {"Pi", 1 << 50}, {"Ti", 1 << 40}, {"Gi", 1 << 30},
	{"Mi", 1 << 20}, {"Ki", 1 << 10},
	{"E", 1e18}, {"P", 1e15}, {"T", 1e12}, {"G", 1e9}, {"M", 1e6}, {"K", 1e3},
	{"k", 1e3},
}

// ParseQuantity decodes a byte-size quantity: a decimal number with an optional
// binary ("Ki", "Mi", "Gi", "Ti", "Pi", "Ei") or SI ("K", "M", "G", "T", "P",
// "E") multiplier and an optional trailing "B" ("10MB", "150Gi", "1048576").
// The result is a count of bytes.
func ParseQuantity(raw string) (float64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errs.Invalidf("empty quantity")
	}
	// A trailing "B" is decoration: "10MB" == "10M", "150GiB" == "150Gi",
	// "512B" == "512". A bare decimal never ends in "B", so trimming is safe.
	if len(s) > 1 && strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}
	for _, q := range quantitySuffixes {
		if !strings.HasSuffix(s, q.suffix) {
			continue
		}
		n, err := strconv.ParseFloat(strings.TrimSuffix(s, q.suffix), 64)
		if err != nil {
			return 0, errs.Invalidf("malformed quantity %q", raw)
		}
		return n * q.mult, nil
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errs.Invalidf("malformed quantity %q", raw)
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, errs.Invalidf("quantity %q is not finite", raw)
	}
	return n, nil
}
