package topic

import (
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// Outcome is what a Resource Provider agent reports for one reconcile attempt
// (005 ADR §1.5).
type Outcome string

const (
	OutcomeCreated Outcome = "CREATED"
	OutcomeUpdated Outcome = "UPDATED"
	OutcomeNoop    Outcome = "NOOP"
	OutcomeDeleted Outcome = "DELETED"
	OutcomeError   Outcome = "ERROR"
)

// Valid reports whether o is a known outcome.
func (o Outcome) Valid() bool {
	switch o {
	case OutcomeCreated, OutcomeUpdated, OutcomeNoop, OutcomeDeleted, OutcomeError:
		return true
	default:
		return false
	}
}

// Satisfied reports whether the outcome means the desired state is now realised.
func (o Outcome) Satisfied() bool {
	return o == OutcomeCreated || o == OutcomeUpdated || o == OutcomeNoop
}

// AppliedState is the topic state the agent read back off the broker. It is
// carried on the report for telemetry and operator diagnosis; v1 persists no
// column for it (005 ADR "Persistence" adds only reconciled_generation and
// last_reconcile_message), so RecordReconciliation validates it and the caller
// logs it.
type AppliedState struct {
	Partitions        int32
	ReplicationFactor int32
	Config            map[string]string
}

// RecordReconciliation applies one agent report to the shard (005 ADR §1.5).
//
// Generation gating: the report is accepted only when generation matches the
// row's *current* generation. A stale report — the desired state changed while
// the agent was working — returns applied=false with no error and no state
// change: Franz has already re-emitted the assignment with the new generation
// and the agent will report again.
//
// A reconcile report is not a desired-state change, so it never bumps
// `generation`; it stamps `reconciled_generation` instead.
//
// State mapping (005 ADR §1.5):
//
//	CREATED / UPDATED / NOOP → READY, reconciled_generation = generation
//	DELETED                  → DELETED (terminal)
//	ERROR                    → ERROR, last_reconcile_message = message
//
// The one refinement: an ERROR on an already-DELETED row records the message but
// leaves the state DELETED, because 003.6 makes DELETED terminal.
//
// The ADR's "→ READY (from PENDING/ERROR)" collapses the 003.6 two-hop
// ERROR → PENDING → READY into a single report-driven transition; that is why
// this method sets state directly rather than going through SetState.
func (t *KafkaTopic) RecordReconciliation(
	generation int64, outcome Outcome, message string, applied *AppliedState,
) (bool, error) {
	if !outcome.Valid() {
		return false, errs.InvalidField("outcome",
			"must be one of CREATED, UPDATED, NOOP, DELETED, ERROR")
	}
	if outcome == OutcomeError && message == "" {
		return false, errs.InvalidField("message", "is required on an ERROR outcome")
	}
	if applied != nil {
		if applied.Partitions < 0 {
			return false, errs.InvalidField("applied_config.partitions", "must be >= 0")
		}
		if applied.ReplicationFactor < 0 {
			return false, errs.InvalidField("applied_config.replication_factor", "must be >= 0")
		}
	}
	if generation != t.Generation {
		return false, nil
	}

	// A PAUSED shard is excluded from the agent's work; a report that races the
	// pause is acknowledged and dropped (005 ADR §1.7).
	if t.State == StatePaused {
		return false, nil
	}

	switch {
	case outcome == OutcomeDeleted:
		t.State = StateDeleted
		t.LastReconcileMessage = message
	case outcome == OutcomeError:
		t.LastReconcileMessage = message
		if t.State == StateDeleted {
			// 003.6 makes DELETED terminal, and Franz sets it optimistically when
			// the owning channel is deleted — so a failed delete (the deletion
			// safety checks refused) cannot move the row back to ERROR. Record why
			// instead, which is what an operator needs to see: the row says
			// deleted, the message says the real topic is still there and why.
			return true, nil
		}
		t.State = StateError
	default: // CREATED / UPDATED / NOOP
		if t.State == StateDeleted {
			return false, nil
		}
		t.State = StateReady
		t.LastReconcileMessage = message
		t.ReconciledGeneration = &generation
	}
	return true, nil
}
