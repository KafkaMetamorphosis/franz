package migration_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/migration"
)

func newTestMigration(t *testing.T) *migration.ShardMigration {
	t.Helper()
	m, err := migration.New(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(),
		migration.ReasonOperator, time.Unix(1700000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewRejectsSameSourceAndTarget(t *testing.T) {
	id := uuid.New()
	if _, err := migration.New(uuid.New(), uuid.New(), id, id, uuid.New(), uuid.New(),
		migration.ReasonOperator, time.Now()); err == nil {
		t.Fatal("want error migrating a shard to itself")
	}
}

func TestNewRejectsSameSourceAndTargetCluster(t *testing.T) {
	clusterID := uuid.New()
	if _, err := migration.New(uuid.New(), uuid.New(), uuid.New(), uuid.New(), clusterID, clusterID,
		migration.ReasonOperator, time.Now()); err == nil {
		t.Fatal("want error migrating within the same cluster")
	}
}

func TestNewRejectsEmptyReason(t *testing.T) {
	if _, err := migration.New(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(),
		"", time.Now()); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestFullHappyPathTransitionOrder(t *testing.T) {
	m := newTestMigration(t)
	now := time.Unix(1700000000, 0)

	if m.Phase != migration.PhaseProvisioning {
		t.Fatalf("initial phase = %v", m.Phase)
	}
	if err := m.AdvanceToCutover(); err != nil {
		t.Fatal(err)
	}
	if err := m.AdvanceToDraining(now); err != nil {
		t.Fatal(err)
	}
	if m.DrainDeadline != now.Add(migration.DrainDeadlineWindow) {
		t.Fatalf("DrainDeadline = %v", m.DrainDeadline)
	}
	if err := m.AdvanceToRetiring(); err != nil {
		t.Fatal(err)
	}
	if err := m.Complete(now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if m.Phase != migration.PhaseDone || m.CompletedAt.IsZero() {
		t.Fatalf("final state = %+v", m)
	}
}

func TestPhaseTransitionsRejectOutOfOrder(t *testing.T) {
	m := newTestMigration(t)
	if err := m.AdvanceToDraining(time.Now()); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("draining from PROVISIONING kind = %v", errs.KindOf(err))
	}
	if err := m.AdvanceToRetiring(); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("retiring from PROVISIONING kind = %v", errs.KindOf(err))
	}
	if err := m.Complete(time.Now()); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("complete from PROVISIONING kind = %v", errs.KindOf(err))
	}
}

func TestReadyToRetireOnDrainedAndDisconnected(t *testing.T) {
	m := newTestMigration(t)
	m.Phase = migration.PhaseDraining
	m.DrainDeadline = time.Unix(1700010000, 0)

	before := time.Unix(1700000001, 0) // well before the deadline
	if !m.ReadyToRetire(true, false, before) {
		t.Error("drained + no consumer should be ready regardless of deadline")
	}
	if m.ReadyToRetire(false, false, before) {
		t.Error("not drained yet, before deadline, should not be ready")
	}
	if m.ReadyToRetire(true, true, before) {
		t.Error("drained but still has a connected consumer should not be ready")
	}
}

func TestReadyToRetireOnDeadlineRegardlessOfSignals(t *testing.T) {
	m := newTestMigration(t)
	m.Phase = migration.PhaseDraining
	m.DrainDeadline = time.Unix(1700010000, 0)

	atDeadline := time.Unix(1700010000, 0)
	past := time.Unix(1700020000, 0)
	if !m.ReadyToRetire(false, true, atDeadline) {
		t.Error("at the deadline, ready regardless of signals")
	}
	if !m.ReadyToRetire(false, true, past) {
		t.Error("past the deadline, ready regardless of signals")
	}
}

func TestReadyToRetireFalseOutsideDraining(t *testing.T) {
	m := newTestMigration(t)
	if m.ReadyToRetire(true, false, time.Now()) {
		t.Error("PROVISIONING should never be ready to retire")
	}
}

func TestFailFromEveryNonTerminalPhase(t *testing.T) {
	for _, phase := range []migration.Phase{
		migration.PhaseProvisioning, migration.PhaseCutover,
		migration.PhaseDraining, migration.PhaseRetiring,
	} {
		m := newTestMigration(t)
		m.Phase = phase
		if err := m.Fail("target cluster unreachable", time.Now()); err != nil {
			t.Fatalf("Fail from %v: %v", phase, err)
		}
		if m.Phase != migration.PhaseFailed || m.FailureReason == "" || m.CompletedAt.IsZero() {
			t.Fatalf("Fail from %v left %+v", phase, m)
		}
	}
}

func TestFailRejectsAlreadyTerminal(t *testing.T) {
	m := newTestMigration(t)
	m.Phase = migration.PhaseDone
	if err := m.Fail("x", time.Now()); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
	m.Phase = migration.PhaseFailed
	if err := m.Fail("x", time.Now()); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestPhaseValidAndTerminal(t *testing.T) {
	if !migration.PhaseDraining.Valid() || migration.Phase("BOGUS").Valid() {
		t.Fatal("Valid() misbehaves")
	}
	if migration.PhaseDraining.Terminal() || !migration.PhaseDone.Terminal() || !migration.PhaseFailed.Terminal() {
		t.Fatal("Terminal() misbehaves")
	}
}
