package topic_test

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
)

func shard(state topic.State, generation int64) *topic.KafkaTopic {
	return &topic.KafkaTopic{
		Name:       "billing-events-0",
		State:      state,
		Generation: generation,
	}
}

func TestRecordReconciliationDrivesTheStateMachine(t *testing.T) {
	cases := []struct {
		name        string
		from        topic.State
		outcome     topic.Outcome
		message     string
		wantApplied bool
		wantState   topic.State
		wantStamped bool
	}{
		{
			name: "created moves a pending shard to ready", from: topic.StatePending,
			outcome: topic.OutcomeCreated, wantApplied: true,
			wantState: topic.StateReady, wantStamped: true,
		},
		{
			name: "updated moves a ready shard back to ready", from: topic.StateReady,
			outcome: topic.OutcomeUpdated, wantApplied: true,
			wantState: topic.StateReady, wantStamped: true,
		},
		{
			name: "noop confirms the generation is satisfied", from: topic.StatePending,
			outcome: topic.OutcomeNoop, wantApplied: true,
			wantState: topic.StateReady, wantStamped: true,
		},
		{
			// 005 ADR §1.5 collapses the 003.6 two-hop ERROR → PENDING → READY into
			// one report-driven transition.
			name: "a success report recovers an errored shard", from: topic.StateError,
			outcome: topic.OutcomeCreated, wantApplied: true,
			wantState: topic.StateReady, wantStamped: true,
		},
		{
			name: "error moves a pending shard to error", from: topic.StatePending,
			outcome: topic.OutcomeError, message: "topic has unconsumed data",
			wantApplied: true, wantState: topic.StateError,
		},
		{
			name: "error moves a ready shard to error", from: topic.StateReady,
			outcome: topic.OutcomeError, message: "broker unreachable",
			wantApplied: true, wantState: topic.StateError,
		},
		{
			name: "deleted is terminal", from: topic.StatePending,
			outcome: topic.OutcomeDeleted, wantApplied: true, wantState: topic.StateDeleted,
		},
		{
			name: "a paused shard is excluded from agent work", from: topic.StatePaused,
			outcome: topic.OutcomeCreated, wantApplied: false, wantState: topic.StatePaused,
		},
		{
			name: "a deleted shard never comes back", from: topic.StateDeleted,
			outcome: topic.OutcomeCreated, wantApplied: false, wantState: topic.StateDeleted,
		},
		{
			// DELETED is terminal (003.6), so a refused delete records why without
			// resurrecting the row.
			name: "a failed delete records the reason but keeps DELETED", from: topic.StateDeleted,
			outcome: topic.OutcomeError, message: "topic has unconsumed data",
			wantApplied: true, wantState: topic.StateDeleted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shd := shard(tc.from, 7)
			applied, err := shd.RecordReconciliation(7, tc.outcome, tc.message, nil)
			if err != nil {
				t.Fatalf("RecordReconciliation() error = %v", err)
			}
			if applied != tc.wantApplied {
				t.Errorf("applied = %v, want %v", applied, tc.wantApplied)
			}
			if shd.State != tc.wantState {
				t.Errorf("state = %s, want %s", shd.State, tc.wantState)
			}
			if tc.wantStamped {
				if shd.ReconciledGeneration == nil || *shd.ReconciledGeneration != 7 {
					t.Errorf("reconciled_generation = %v, want 7", shd.ReconciledGeneration)
				}
			} else if shd.ReconciledGeneration != nil {
				t.Errorf("reconciled_generation = %v, want unset", *shd.ReconciledGeneration)
			}
		})
	}
}

// A reconcile report is not a desired-state change, so it must never bump the
// generation — otherwise the row would never look converged.
func TestRecordReconciliationDoesNotBumpGeneration(t *testing.T) {
	shd := shard(topic.StatePending, 4)
	if _, err := shd.RecordReconciliation(4, topic.OutcomeCreated, "", nil); err != nil {
		t.Fatal(err)
	}
	if shd.Generation != 4 {
		t.Fatalf("generation = %d, want 4 (a report is not a desired-state change)", shd.Generation)
	}
	if shd.ReconciledGeneration == nil || *shd.ReconciledGeneration != shd.Generation {
		t.Fatal("a converged shard must have reconciled_generation == generation")
	}
}

func TestRecordReconciliationIsGenerationGated(t *testing.T) {
	t.Run("a stale report is acknowledged but changes nothing", func(t *testing.T) {
		shd := shard(topic.StatePending, 9)
		applied, err := shd.RecordReconciliation(8, topic.OutcomeCreated, "", nil)
		if err != nil {
			t.Fatalf("a stale report must not be an error, got %v", err)
		}
		if applied {
			t.Error("applied = true, want false for a stale generation")
		}
		if shd.State != topic.StatePending {
			t.Errorf("state = %s, want PENDING — a stale report must not move the row to READY", shd.State)
		}
		if shd.ReconciledGeneration != nil {
			t.Error("a stale report must not stamp reconciled_generation")
		}
	})

	t.Run("a report from the future is equally ignored", func(t *testing.T) {
		shd := shard(topic.StatePending, 3)
		applied, err := shd.RecordReconciliation(4, topic.OutcomeCreated, "", nil)
		if err != nil || applied {
			t.Fatalf("applied = %v, err = %v; want false, nil", applied, err)
		}
		if shd.State != topic.StatePending {
			t.Errorf("state = %s, want PENDING", shd.State)
		}
	})
}

func TestRecordReconciliationStoresTheMessage(t *testing.T) {
	shd := shard(topic.StatePending, 1)
	const detail = "topic has active consumers (groups: audit, payments-worker)"
	if _, err := shd.RecordReconciliation(1, topic.OutcomeError, detail, nil); err != nil {
		t.Fatal(err)
	}
	if shd.LastReconcileMessage != detail {
		t.Fatalf("last_reconcile_message = %q, want %q", shd.LastReconcileMessage, detail)
	}
}

func TestRecordReconciliationValidates(t *testing.T) {
	t.Run("an unknown outcome is rejected", func(t *testing.T) {
		shd := shard(topic.StatePending, 1)
		if _, err := shd.RecordReconciliation(1, topic.Outcome("WAT"), "", nil); err == nil {
			t.Fatal("expected an error for an unknown outcome")
		}
	})
	t.Run("an ERROR without a message is rejected", func(t *testing.T) {
		shd := shard(topic.StatePending, 1)
		if _, err := shd.RecordReconciliation(1, topic.OutcomeError, "", nil); err == nil {
			t.Fatal("expected an error: message is required on ERROR")
		}
	})
	t.Run("validation runs before the generation gate", func(t *testing.T) {
		shd := shard(topic.StatePending, 9)
		if _, err := shd.RecordReconciliation(1, topic.Outcome("WAT"), "", nil); err == nil {
			t.Fatal("a malformed stale report is still a malformed report")
		}
	})
}
