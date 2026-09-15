// Package migration is the shard migration domain entity (003.13): the single
// staged flow that moves a shard's serving position from one cluster to
// another.
//
// v1 is drain-based and reuses existing primitives end to end rather than
// inventing a parallel mechanism: PROVISIONING creates an ordinary new shard
// (topic.New + PlaceOn, the same as placement's newShard) on the target
// cluster; cutover is topics.Service.SetConsumption(DISABLED) on the source,
// which already re-normalises traffic_share across the channel's siblings;
// RETIRING deletes the source shard through the normal delete path, which
// already tells the source cluster's agent to remove the real topic — no new
// agent protocol. shard_migration is bookkeeping and a sweep driver, not a
// second state machine layered onto kafka_topic.
package migration

import (
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// Phase is a migration's position in the drain-based flow (003.13):
//
//	PROVISIONING ─▶ CUTOVER ─▶ DRAINING ─▶ RETIRING ─▶ DONE
//	     │             │                                ▲
//	     └─────────────┴──────────────────────────▶ FAILED
type Phase string

const (
	PhaseProvisioning Phase = "PROVISIONING"
	PhaseCutover      Phase = "CUTOVER"
	PhaseDraining     Phase = "DRAINING"
	PhaseRetiring     Phase = "RETIRING"
	PhaseDone         Phase = "DONE"
	PhaseFailed       Phase = "FAILED"
)

// Valid reports whether p is one of the six defined phases.
func (p Phase) Valid() bool {
	switch p {
	case PhaseProvisioning, PhaseCutover, PhaseDraining, PhaseRetiring, PhaseDone, PhaseFailed:
		return true
	default:
		return false
	}
}

// Terminal reports whether p is DONE or FAILED — the sweep skips these rows.
func (p Phase) Terminal() bool { return p == PhaseDone || p == PhaseFailed }

// Reason is why a migration started (003.13's `reason` field), free-form text
// with a closed prefix set for the four built-in triggers plus an open
// `governance:<policy-name>` form for the fifth.
const (
	ReasonOperator      = "operator"       // an explicit MigrateKafkaTopic / MigrateCluster call
	ReasonDrainTaint    = "drain-taint"    // a cluster gained franz.taint=drain
	ReasonClusterDelete = "cluster-delete" // DeleteKafkaCluster force=true on a cluster with live shards
	ReasonMisplaced     = "misplaced"      // 13.5's "real move" for a shard the placement sweep marked misplaced
	ReasonReshard       = "re-shard"       // a channel_partitions decrease retiring a shard
	// GovernanceReasonPrefix + a policy name is the fifth form (003.13
	// "governance:<policy>") — governance actions build the string themselves,
	// there is no Go constant for an open-ended value.
	GovernanceReasonPrefix = "governance:"
)

// ShardMigration is one in-flight (or completed) move of a shard from
// SourceTopicID to TargetTopicID. Both are real kafka_topic rows throughout —
// the source is never repointed to the target cluster; it is deleted once
// drained.
type ShardMigration struct {
	ID             uuid.UUID
	RealmID        uuid.UUID
	AsyncChannelID uuid.UUID
	SourceTopicID  uuid.UUID
	TargetTopicID  uuid.UUID
	// SourceClusterID / TargetClusterID are denormalised from the two topic
	// rows at creation time (003.13's own field list names them directly) —
	// the per-cluster concurrency limit (18.5) counts active rows by these
	// without joining kafka_topic.
	SourceClusterID uuid.UUID
	TargetClusterID uuid.UUID

	Phase  Phase
	Reason string

	// DrainDeadline is set when DRAINING starts (003.13 OQ3: a fixed 1h
	// deadline, not per-channel or lag-derived). Zero before DRAINING.
	DrainDeadline time.Time
	// FailureReason is set only in PhaseFailed.
	FailureReason string

	StartedAt   time.Time
	CompletedAt time.Time // zero until DONE or FAILED

	CreatedAt time.Time
	UpdatedAt time.Time

	// Read-path projections, joined for the API (which speaks resource names,
	// not surrogate ids) — the same convention topic.KafkaTopic's
	// ChannelName/ClusterName use. Never persisted; empty until the usecase's
	// Get/List enriches them.
	AsyncChannelName  string
	SourceTopicName   string
	TargetTopicName   string
	SourceClusterName string
	TargetClusterName string
}

// DrainDeadlineWindow is 003.13 OQ3's resolved fixed, config-wide deadline.
const DrainDeadlineWindow = time.Hour

// New starts a migration in PROVISIONING. The caller has already created (or
// is creating in the same transaction) the target shard row and validated the
// source/target clusters differ and the source is not already migrating.
func New(
	realmID, channelID, sourceTopicID, targetTopicID, sourceClusterID, targetClusterID uuid.UUID,
	reason string, now time.Time,
) (*ShardMigration, error) {
	if sourceTopicID == targetTopicID {
		return nil, errs.Invalidf("a shard cannot migrate to itself")
	}
	if sourceClusterID == targetClusterID {
		return nil, errs.Invalidf("source and target clusters must differ")
	}
	if reason == "" {
		return nil, errs.InvalidField("reason", "must not be empty")
	}
	return &ShardMigration{
		RealmID:         realmID,
		AsyncChannelID:  channelID,
		SourceTopicID:   sourceTopicID,
		TargetTopicID:   targetTopicID,
		SourceClusterID: sourceClusterID,
		TargetClusterID: targetClusterID,
		Phase:           PhaseProvisioning,
		Reason:          reason,
		StartedAt:       now,
	}, nil
}

// AdvanceToCutover moves PROVISIONING → CUTOVER once the target shard reports
// READY. Idempotent: a no-op if already past this phase.
func (m *ShardMigration) AdvanceToCutover() error {
	if m.Phase != PhaseProvisioning {
		return m.phaseErr(PhaseProvisioning)
	}
	m.Phase = PhaseCutover
	return nil
}

// AdvanceToDraining moves CUTOVER → DRAINING once SetConsumption(DISABLED) has
// been applied to the source, and sets the drain deadline.
func (m *ShardMigration) AdvanceToDraining(now time.Time) error {
	if m.Phase != PhaseCutover {
		return m.phaseErr(PhaseCutover)
	}
	m.Phase = PhaseDraining
	m.DrainDeadline = now.Add(DrainDeadlineWindow)
	return nil
}

// ReadyToRetire reports whether DRAINING's exit condition holds: the source is
// drained and has no connected consumer (the early-completion signal, 003.13
// "lag = 0"), or the deadline has passed regardless (the safety net every
// reason accepts — 003.13's per-reason "block vs force" split is future work;
// v1 always forces at the deadline, since drain-based migration has no data
// left to lose once the deadline forces retirement — see the tracker).
func (m *ShardMigration) ReadyToRetire(drained, consumerConnected bool, now time.Time) bool {
	if m.Phase != PhaseDraining {
		return false
	}
	if drained && !consumerConnected {
		return true
	}
	return !m.DrainDeadline.IsZero() && !now.Before(m.DrainDeadline)
}

// AdvanceToRetiring moves DRAINING → RETIRING once ReadyToRetire is true.
func (m *ShardMigration) AdvanceToRetiring() error {
	if m.Phase != PhaseDraining {
		return m.phaseErr(PhaseDraining)
	}
	m.Phase = PhaseRetiring
	return nil
}

// Complete moves RETIRING → DONE once the source shard's delete has been
// issued.
func (m *ShardMigration) Complete(now time.Time) error {
	if m.Phase != PhaseRetiring {
		return m.phaseErr(PhaseRetiring)
	}
	m.Phase = PhaseDone
	m.CompletedAt = now
	return nil
}

// Fail moves any non-terminal phase to FAILED. Idempotent on an
// already-terminal migration (rejected — a terminal migration does not fail
// again).
func (m *ShardMigration) Fail(reason string, now time.Time) error {
	if m.Phase.Terminal() {
		return errs.Preconditionf("migration %s is already %s", m.ID, m.Phase)
	}
	m.Phase = PhaseFailed
	m.FailureReason = reason
	m.CompletedAt = now
	return nil
}

func (m *ShardMigration) phaseErr(want Phase) error {
	return errs.Preconditionf("migration %s is %s, not %s", m.ID, m.Phase, want)
}
