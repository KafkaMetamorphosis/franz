package topic

import (
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

func testRealm() realm.Realm {
	return realm.Realm{ID: uuid.New(), Slug: "default", Name: "Default"}
}

func newShard(t *testing.T) *KafkaTopic {
	t.Helper()
	sh, err := New(testRealm(), uuid.New(), "orders", 0,
		map[string]string{"retention.ms": "60000"},
		map[string]string{"cleanup.policy": "compact"}, 3, 1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return sh
}

func TestNew(t *testing.T) {
	sh := newShard(t)
	if sh.Name != "orders-0" {
		t.Errorf("name = %q", sh.Name)
	}
	if sh.FRN.String() != "frn:default:kafka-topic:orders-0" {
		t.Errorf("frn = %q", sh.FRN.String())
	}
	if sh.State != StatePending || sh.Consumption != ConsumptionEnabled || sh.Generation != 1 {
		t.Errorf("bad initial state: %+v", sh)
	}
	if sh.TrafficShare.Unit != TrafficShareUnit {
		t.Errorf("unit = %q", sh.TrafficShare.Unit)
	}
	// materialised = cluster ⊕ topic (topic wins)
	want := map[string]string{"retention.ms": "60000", "cleanup.policy": "compact"}
	if !mapEq(sh.MaterializedConfiguration, want) {
		t.Errorf("materialized = %v", sh.MaterializedConfiguration)
	}
}

func TestNewRejectsBadCounts(t *testing.T) {
	if _, err := New(testRealm(), uuid.New(), "x", 0, nil, nil, 0, 1); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("partitions=0 → %v", err)
	}
	if _, err := New(testRealm(), uuid.New(), "x", 0, nil, nil, 1, 0); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("rf=0 → %v", err)
	}
}

func TestSetConsumption(t *testing.T) {
	sh := newShard(t)
	changed, err := sh.SetConsumption(ConsumptionDisabled)
	if err != nil || !changed || sh.Consumption != ConsumptionDisabled {
		t.Fatalf("disable: changed=%v err=%v", changed, err)
	}
	if sh.Generation != 2 {
		t.Errorf("generation = %d, want bump", sh.Generation)
	}
	changed, _ = sh.SetConsumption(ConsumptionDisabled)
	if changed || sh.Generation != 2 {
		t.Errorf("no-op should not bump generation")
	}
	if _, err := sh.SetConsumption("SOMETHING"); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("bad value → %v", err)
	}
}

func TestIncreasePartitionsOnly(t *testing.T) {
	sh := newShard(t) // partitions 3
	if err := sh.IncreasePartitions(2); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("decrease → %v", err)
	}
	if err := sh.IncreasePartitions(3); err != nil || sh.Generation != 1 {
		t.Fatalf("equal is a no-op: %v gen=%d", err, sh.Generation)
	}
	if err := sh.IncreasePartitions(6); err != nil || sh.Partitions != 6 || sh.Generation != 2 {
		t.Fatalf("increase: %v part=%d gen=%d", err, sh.Partitions, sh.Generation)
	}
}

func TestDeletedRejectsEverything(t *testing.T) {
	sh := newShard(t)
	sh.State = StateDeleted
	if _, err := sh.SetConsumption(ConsumptionDisabled); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("SetConsumption on deleted → %v", err)
	}
	if err := sh.IncreasePartitions(10); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("IncreasePartitions on deleted → %v", err)
	}
	if err := sh.EnsureMutable(); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("EnsureMutable on deleted → %v", err)
	}
}

func TestStateMachine(t *testing.T) {
	sh := newShard(t) // PENDING
	if err := sh.SetState(StateReady); err != nil || sh.State != StateReady {
		t.Fatalf("PENDING→READY: %v", err)
	}
	if err := sh.SetState(StateError); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("READY→ERROR should be illegal: %v", err)
	}
	if err := sh.SetState(StatePending); err != nil {
		t.Fatalf("READY→PENDING: %v", err)
	}
	if err := sh.SetState(StateDeleted); err != nil {
		t.Fatalf("PENDING→DELETED: %v", err)
	}
	if err := sh.SetState(StatePending); errs.KindOf(err) != errs.FailedPrecondition {
		t.Errorf("DELETED→anything should be illegal: %v", err)
	}
}

func TestRematerialize(t *testing.T) {
	sh := newShard(t)
	gen := sh.Generation
	sh.Rematerialize(map[string]string{"retention.ms": "999", "min.insync.replicas": "2"})
	want := map[string]string{
		"retention.ms":        "999",     // new cluster value
		"min.insync.replicas": "2",       // new cluster key
		"cleanup.policy":      "compact", // topic key still wins
	}
	if !mapEq(sh.MaterializedConfiguration, want) {
		t.Errorf("rematerialised = %v", sh.MaterializedConfiguration)
	}
	if sh.Generation != gen+1 {
		t.Errorf("generation not bumped")
	}
}

func TestEqualSharePercent(t *testing.T) {
	cases := map[int]float64{0: 0, 1: 100, 2: 50, 4: 25}
	for n, want := range cases {
		if got := EqualSharePercent(n); got != want {
			t.Errorf("EqualSharePercent(%d) = %v, want %v", n, got, want)
		}
	}
	if got := EqualSharePercent(3); got < 33.33 || got > 33.34 {
		t.Errorf("EqualSharePercent(3) = %v", got)
	}
}

func mapEq(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
