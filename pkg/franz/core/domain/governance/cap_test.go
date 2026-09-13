package governance_test

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
)

// TestParseCap pins the OQ1 cap encoding: an optional third positional arg,
// "max=<ceiling>" on INCREASE_FIELD_BY and "min=<floor>" on DECREASE_FIELD_BY.
func TestParseCap(t *testing.T) {
	tests := []struct {
		name      string
		action    governance.Action
		wantErr   bool
		present   bool
		wantKind  governance.CapKind
		wantLimit float64
	}{
		{name: "absent on a two-arg increase",
			action: action(governance.ActionIncreaseFieldBy, "brokers", "1")},
		{name: "absent on a non-arithmetic action",
			action: action(governance.ActionUpdateField, "brokers", "3")},
		{name: "max ceiling", action: action(governance.ActionIncreaseFieldBy, "brokers", "1", "max=9"),
			present: true, wantKind: governance.CapMax, wantLimit: 9},
		{name: "max ceiling with a byte suffix",
			action:  action(governance.ActionIncreaseFieldBy, "topic_configuration.retention.bytes", "1Gi", "max=10Gi"),
			present: true, wantKind: governance.CapMax, wantLimit: 10 << 30},
		{name: "min floor", action: action(governance.ActionDecreaseFieldBy, "brokers", "1", "min=3"),
			present: true, wantKind: governance.CapMin, wantLimit: 3},
		{name: "max on a decrease is rejected",
			action: action(governance.ActionDecreaseFieldBy, "brokers", "1", "max=9"), wantErr: true},
		{name: "min on an increase is rejected",
			action: action(governance.ActionIncreaseFieldBy, "brokers", "1", "min=3"), wantErr: true},
		{name: "an unprefixed cap is rejected",
			action: action(governance.ActionIncreaseFieldBy, "brokers", "1", "9"), wantErr: true},
		{name: "a non-numeric cap is rejected",
			action: action(governance.ActionIncreaseFieldBy, "brokers", "1", "max=lots"), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := governance.ParseCap(tc.action)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCap: %v", err)
			}
			if got.Present != tc.present {
				t.Fatalf("Present = %v, want %v", got.Present, tc.present)
			}
			if !tc.present {
				return
			}
			if got.Kind != tc.wantKind {
				t.Errorf("Kind = %v, want %v", got.Kind, tc.wantKind)
			}
			if got.Limit != tc.wantLimit {
				t.Errorf("Limit = %v, want %v", got.Limit, tc.wantLimit)
			}
		})
	}
}

// TestCapClamps: a cap bounds the *resulting field value*, and it clamps rather
// than fails — a policy that has driven a field to its ceiling becomes a no-op,
// which is the intended steady state.
func TestCapClamps(t *testing.T) {
	max := governance.Cap{Present: true, Kind: governance.CapMax, Limit: 10, Raw: "max=10"}
	min := governance.Cap{Present: true, Kind: governance.CapMin, Limit: 2, Raw: "min=2"}
	absent := governance.Cap{}

	tests := []struct {
		name       string
		cap        governance.Cap
		want       float64
		wantValue  float64
		wantCapped bool
	}{
		{name: "max bites above the ceiling", cap: max, want: 12, wantValue: 10, wantCapped: true},
		{name: "max passes below the ceiling", cap: max, want: 8, wantValue: 8},
		{name: "max passes at the ceiling", cap: max, want: 10, wantValue: 10},
		{name: "min bites below the floor", cap: min, want: 1, wantValue: 2, wantCapped: true},
		{name: "min passes above the floor", cap: min, want: 5, wantValue: 5},
		{name: "an absent cap passes anything", cap: absent, want: 1e9, wantValue: 1e9},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, capped := tc.cap.Clamp(tc.want)
			if got != tc.wantValue || capped != tc.wantCapped {
				t.Fatalf("Clamp(%v) = (%v, %v), want (%v, %v)",
					tc.want, got, capped, tc.wantValue, tc.wantCapped)
			}
		})
	}
}

// TestParseAmount covers the absolute / percentage forms of an action's amount.
func TestParseAmount(t *testing.T) {
	tests := []struct {
		raw     string
		wantErr bool
		percent bool
		value   float64
	}{
		{raw: "2", value: 2},
		{raw: "10Gi", value: 10 << 30},
		{raw: "1MB", value: 1e6},
		{raw: "50%", percent: true, value: 50},
		// Whitespace is tolerated on both sides of the number and before the "%",
		// so an operator's " 25 % " means the same as "25%".
		{raw: " 25 % ", percent: true, value: 25},
		{raw: "-1", wantErr: true},
		{raw: "-10%", wantErr: true},
		{raw: "", wantErr: true},
		{raw: "lots", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := governance.ParseAmount(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want an error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAmount(%q): %v", tc.raw, err)
			}
			if got.Percent != tc.percent || got.Value != tc.value {
				t.Fatalf("= {Percent:%v Value:%v}, want {Percent:%v Value:%v}",
					got.Percent, got.Value, tc.percent, tc.value)
			}
		})
	}
}

// TestApplyArithmetic checks the delta a percentage amount produces is relative
// to the field's current value, and that the cap is applied to the result.
func TestApplyArithmetic(t *testing.T) {
	tests := []struct {
		name       string
		kind       governance.ActionKind
		current    float64
		amount     governance.Amount
		bound      governance.Cap
		want       float64
		wantCapped bool
	}{
		{name: "absolute increase", kind: governance.ActionIncreaseFieldBy, current: 6,
			amount: governance.Amount{Value: 2}, want: 8},
		{name: "percentage increase is relative to current",
			kind: governance.ActionIncreaseFieldBy, current: 200,
			amount: governance.Amount{Percent: true, Value: 50}, want: 300},
		{name: "absolute decrease", kind: governance.ActionDecreaseFieldBy, current: 6,
			amount: governance.Amount{Value: 2}, want: 4},
		{name: "percentage decrease", kind: governance.ActionDecreaseFieldBy, current: 200,
			amount: governance.Amount{Percent: true, Value: 25}, want: 150},
		{name: "the cap bounds the result, not the delta",
			kind: governance.ActionIncreaseFieldBy, current: 60,
			amount: governance.Amount{Percent: true, Value: 100},
			bound:  governance.Cap{Present: true, Kind: governance.CapMax, Limit: 64},
			want:   64, wantCapped: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, capped := governance.ApplyArithmetic(tc.kind, tc.current, tc.amount, tc.bound)
			if got != tc.want || capped != tc.wantCapped {
				t.Fatalf("= (%v, %v), want (%v, %v)", got, capped, tc.want, tc.wantCapped)
			}
		})
	}
}
