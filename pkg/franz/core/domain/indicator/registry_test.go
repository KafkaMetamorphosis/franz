package indicator_test

import (
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

func testRealm() realm.Realm {
	return realm.Realm{ID: realm.DefaultID, Slug: realm.DefaultSlug}
}

func TestNewIndicator(t *testing.T) {
	i, err := indicator.NewIndicator(testRealm(), "disk-used", indicator.UnitBytes,
		indicator.EntityKafkaCluster, "90d", []string{"gregor-samsa"})
	if err != nil {
		t.Fatalf("NewIndicator: %v", err)
	}
	if i.FRN.Type() != "indicator" || i.FRN.Name() != "disk-used" {
		t.Errorf("FRN = %s", i.FRN.Path())
	}
	if i.StalenessThreshold != 90*24*time.Hour {
		t.Errorf("StalenessThreshold = %v, want 90d", i.StalenessThreshold)
	}
	// The operator's text is kept verbatim so a read returns what was written.
	if i.StalenessSpec != "90d" {
		t.Errorf("StalenessSpec = %q, want %q", i.StalenessSpec, "90d")
	}
	if i.LastSampleAt != nil || i.CurrentValue != "" {
		t.Error("a fresh indicator has no current value")
	}
}

func TestNewIndicatorRejections(t *testing.T) {
	tests := []struct {
		name      string
		indName   string
		unit      indicator.Unit
		appliesTo indicator.Entity
		staleness string
		agents    []string
	}{
		{name: "bad name", indName: "Not A Name", unit: indicator.UnitCount,
			appliesTo: indicator.EntityKafkaCluster, staleness: "1h"},
		{name: "unknown entity", indName: "x", unit: indicator.UnitCount,
			appliesTo: indicator.Entity("REALM"), staleness: "1h"},
		{name: "empty unit", indName: "x", unit: "",
			appliesTo: indicator.EntityKafkaCluster, staleness: "1h"},
		{name: "empty staleness", indName: "x", unit: indicator.UnitCount,
			appliesTo: indicator.EntityKafkaCluster, staleness: ""},
		{name: "malformed staleness", indName: "x", unit: indicator.UnitCount,
			appliesTo: indicator.EntityKafkaCluster, staleness: "soon"},
		{name: "zero staleness", indName: "x", unit: indicator.UnitCount,
			appliesTo: indicator.EntityKafkaCluster, staleness: "0s"},
		{name: "empty source agent", indName: "x", unit: indicator.UnitCount,
			appliesTo: indicator.EntityKafkaCluster, staleness: "1h", agents: []string{" "}},
		{name: "invalid source agent name", indName: "x", unit: indicator.UnitCount,
			appliesTo: indicator.EntityKafkaCluster, staleness: "1h", agents: []string{"Not A Name"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := indicator.NewIndicator(testRealm(), tc.indName, tc.unit,
				tc.appliesTo, tc.staleness, tc.agents)
			if err == nil {
				t.Fatal("want a rejection")
			}
		})
	}
}

// TestHealthIsDerived pins 003.14: health is computed from last_sample_at and
// staleness_threshold on every read, never stored — and an indicator that has
// never been sampled is STALE, not HEALTHY.
func TestHealthIsDerived(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		lastSampleAt *time.Time
		threshold    time.Duration
		want         indicator.Health
	}{
		{name: "never sampled is stale", lastSampleAt: nil,
			threshold: time.Hour, want: indicator.HealthStale},
		{name: "a zero timestamp is stale", lastSampleAt: &time.Time{},
			threshold: time.Hour, want: indicator.HealthStale},
		{name: "inside the window is healthy", lastSampleAt: ptr(now.Add(-30 * time.Minute)),
			threshold: time.Hour, want: indicator.HealthHealthy},
		{name: "exactly at the threshold is still healthy",
			lastSampleAt: ptr(now.Add(-time.Hour)),
			threshold:    time.Hour, want: indicator.HealthHealthy},
		{name: "past the threshold is stale", lastSampleAt: ptr(now.Add(-2 * time.Hour)),
			threshold: time.Hour, want: indicator.HealthStale},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			i := &indicator.Indicator{
				LastSampleAt: tc.lastSampleAt, StalenessThreshold: tc.threshold,
			}
			if got := i.Health(now); got != tc.want {
				t.Fatalf("Health = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestAppliesToIsNotSettable is the domain half of the 003.14 immutability rule:
// the entity exposes setters for every mutable field, and applies_to is not
// among them. The persistence half — the UPDATE statement that omits the column
// — is covered by the repository integration test.
func TestAppliesToIsNotSettable(t *testing.T) {
	i, err := indicator.NewIndicator(testRealm(), "disk-used", indicator.UnitBytes,
		indicator.EntityKafkaCluster, "1h", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := i.SetUnit(indicator.UnitCount); err != nil {
		t.Fatalf("SetUnit: %v", err)
	}
	if err := i.SetStalenessThreshold("2w"); err != nil {
		t.Fatalf("SetStalenessThreshold: %v", err)
	}
	if err := i.SetSourceAgents([]string{"agent-a"}); err != nil {
		t.Fatalf("SetSourceAgents: %v", err)
	}
	if i.AppliesTo != indicator.EntityKafkaCluster {
		t.Fatalf("AppliesTo = %v, want it unchanged by every setter", i.AppliesTo)
	}
	if i.StalenessThreshold != 14*24*time.Hour {
		t.Errorf("2w = %v, want 336h", i.StalenessThreshold)
	}
}

// TestEnsureSampleEntity rejects a sample whose entity disagrees with the
// registration (003.14).
func TestEnsureSampleEntity(t *testing.T) {
	i := &indicator.Indicator{Name: "disk-used", AppliesTo: indicator.EntityKafkaCluster}
	if err := i.EnsureSampleEntity(indicator.EntityKafkaCluster); err != nil {
		t.Fatalf("matching entity: %v", err)
	}
	if err := i.EnsureSampleEntity(indicator.EntityAsyncChannel); err == nil {
		t.Fatal("a disagreeing entity must be rejected")
	}
}

func TestParseDurationSpec(t *testing.T) {
	tests := []struct {
		spec    string
		want    time.Duration
		wantErr bool
	}{
		{spec: "5m", want: 5 * time.Minute},
		{spec: "1h30m", want: 90 * time.Minute},
		{spec: "250ms", want: 250 * time.Millisecond},
		{spec: "90d", want: 90 * 24 * time.Hour},
		{spec: "2w", want: 14 * 24 * time.Hour},
		{spec: "0.5d", want: 12 * time.Hour},
		{spec: " 1h ", want: time.Hour},
		{spec: "", wantErr: true},
		{spec: "90 days", wantErr: true},
		{spec: "d", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			got, err := indicator.ParseDurationSpec(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want an error for %q", tc.spec)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDurationSpec(%q): %v", tc.spec, err)
			}
			if got != tc.want {
				t.Fatalf("= %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUnitFamily pins which comparison family each unit resolves to — an
// unrecognised unit must fall back to numeric rather than fail.
func TestUnitFamily(t *testing.T) {
	tests := []struct {
		unit indicator.Unit
		want indicator.Family
	}{
		{indicator.UnitCount, indicator.FamilyNumeric},
		{indicator.UnitPercent, indicator.FamilyNumeric},
		{indicator.UnitRatio, indicator.FamilyNumeric},
		{indicator.UnitBytes, indicator.FamilyBytes},
		{indicator.Unit("BYTES"), indicator.FamilyBytes},
		{indicator.Unit("byte_size"), indicator.FamilyBytes},
		{indicator.UnitDuration, indicator.FamilyDuration},
		{indicator.Unit("lag"), indicator.FamilyDuration},
		{indicator.UnitBoolean, indicator.FamilyBoolean},
		{indicator.Unit("bool"), indicator.FamilyBoolean},
		{indicator.Unit("widgets-per-fortnight"), indicator.FamilyNumeric},
	}

	for _, tc := range tests {
		t.Run(string(tc.unit), func(t *testing.T) {
			if got := tc.unit.Family(); got != tc.want {
				t.Fatalf("Family = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCompareRejectsMixedFamilies: values from different families are not
// comparable, which the caller surfaces rather than guessing at.
func TestCompareRejectsMixedFamilies(t *testing.T) {
	bytes, err := indicator.ParseValue(indicator.UnitBytes, "value", "1Gi")
	if err != nil {
		t.Fatal(err)
	}
	flag, err := indicator.ParseValue(indicator.UnitBoolean, "value", "true")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bytes.Compare(flag); err == nil {
		t.Fatal("comparing a byte size to a boolean must error")
	}
}

func ptr[T any](v T) *T { return &v }
