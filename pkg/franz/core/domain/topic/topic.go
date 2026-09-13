// Package topic is the Kafka Topic domain entity (003.6): one shard of an Async
// Channel, placed on one Kafka Cluster, tracking reconciliation with the real
// topic. Franz owns every field; the only client-initiated mutation is
// SetConsumption. Rows are created by the Async Channel (deliverable 10), never
// through an API.
package topic

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// State is the reconciliation state (003.6 "State transitions"). Franz and the
// agents manage it; clients never write it.
type State string

const (
	StatePending State = "PENDING"
	StateReady   State = "READY"
	StatePaused  State = "PAUSED"
	StateError   State = "ERROR"
	StateDeleted State = "DELETED"
)

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateReady, StatePaused, StateError, StateDeleted:
		return true
	default:
		return false
	}
}

// transitions is the 003.6 state machine. consumption is orthogonal and not
// modelled here.
var transitions = map[State]map[State]bool{
	StatePending: {StateReady: true, StateError: true, StatePaused: true, StateDeleted: true},
	StateReady:   {StatePending: true, StatePaused: true, StateDeleted: true},
	StateError:   {StatePending: true, StatePaused: true, StateDeleted: true},
	StatePaused:  {StatePending: true, StateDeleted: true},
	StateDeleted: {},
}

// CanTransition reports whether state → to is allowed by 003.6.
func (s State) CanTransition(to State) bool { return transitions[s][to] }

// Consumption is whether consumers may read the shard (003.6). Orthogonal to
// state. Changed only via SetConsumption.
type Consumption string

const (
	ConsumptionEnabled  Consumption = "ENABLED"
	ConsumptionDisabled Consumption = "DISABLED"
)

// Valid reports whether c is a known consumption value.
func (c Consumption) Valid() bool {
	return c == ConsumptionEnabled || c == ConsumptionDisabled
}

// TrafficShare is the intended (not observed) proportion of channel traffic
// routed to this shard (003.6). Franz keeps an equal split across the shards
// whose consumption is ENABLED; a drained shard is zero.
type TrafficShare struct {
	Value float64
	Unit  string
}

// TrafficShareUnit is the unit Franz always uses for the equal split.
const TrafficShareUnit = "percent"

// EqualSharePercent is each ENABLED shard's share when a channel has
// enabledCount of them. Zero when there are none.
func EqualSharePercent(enabledCount int) float64 {
	if enabledCount <= 0 {
		return 0
	}
	return 100.0 / float64(enabledCount)
}

// KafkaTopic is one shard.
type KafkaTopic struct {
	ID             uuid.UUID // surrogate key; assigned by the repository on Create
	FRN            frn.FRN
	RealmID        uuid.UUID
	AsyncChannelID uuid.UUID
	KafkaClusterID *uuid.UUID // nil while unplaced (deliverable 11)
	Name           string

	TopicConfiguration        map[string]string // per-shard layer of the merge
	MaterializedConfiguration map[string]string // frozen cluster⊕topic merge; internal, not on the proto
	Partitions                int32
	ReplicationFactor         int32

	State        State
	Consumption  Consumption
	TrafficShare TrafficShare
	Generation   int64

	// ReconciledGeneration is the last generation a Resource Provider agent
	// confirmed the real Kafka topic satisfies (005 ADR §1.5). nil until the
	// first successful report; lagging `Generation` means the shard is not yet
	// converged.
	ReconciledGeneration *int64
	// LastReconcileMessage is the detail from the newest agent report — the
	// operator-facing "why is this shard in ERROR" (003.6 OQ5, first cut).
	LastReconcileMessage string

	// Misplaced marks an async-channel shard whose cluster stopped satisfying
	// its channel's placement rules — re-labelled out of the affinity selector,
	// moved to PAUSED/DELETED, or given a `drain` taint (003.7 "Re-placement").
	// Franz never moves the shard on its own; the migration flow (003.13) does.
	// MisplacedReason is the operator-facing why, empty while Misplaced is false.
	Misplaced       bool
	MisplacedReason string

	CreatedAt time.Time
	UpdatedAt time.Time

	// Read-path projections, joined from async_channel / kafka_cluster for the
	// API (which speaks resource names). Never persisted on this row; empty on a
	// freshly-created shard until re-read. ClusterName is "" while unplaced.
	ChannelName string
	ClusterName string
}

// New builds a shard in PENDING / ENABLED, with the config merge materialised.
// shardIndex is 0-based; the name is "<channelName>-<shardIndex>". partitions and
// replicationFactor are seeded by the caller from cluster defaults (003.6).
func New(
	r realm.Realm,
	channelID uuid.UUID,
	channelName string,
	shardIndex int,
	clusterConfig, topicConfig map[string]string,
	partitions, replicationFactor int32,
) (*KafkaTopic, error) {
	name := fmt.Sprintf("%s-%d", channelName, shardIndex)
	id, err := frn.New(r.Slug, frn.TypeKafkaTopic, name)
	if err != nil {
		return nil, err
	}
	if partitions < 1 {
		return nil, errs.InvalidField("partitions", "must be >= 1")
	}
	if replicationFactor < 1 {
		return nil, errs.InvalidField("replication_factor", "must be >= 1")
	}
	return &KafkaTopic{
		FRN:                       id,
		RealmID:                   r.ID,
		AsyncChannelID:            channelID,
		Name:                      name,
		TopicConfiguration:        nonNil(topicConfig),
		MaterializedConfiguration: Materialize(clusterConfig, topicConfig),
		Partitions:                partitions,
		ReplicationFactor:         replicationFactor,
		State:                     StatePending,
		Consumption:               ConsumptionEnabled,
		TrafficShare:              TrafficShare{Value: 0, Unit: TrafficShareUnit},
		Generation:                1,
	}, nil
}

// EnsureMutable rejects any operation on a soft-deleted shard (003.6 invariant).
func (t *KafkaTopic) EnsureMutable() error {
	if t.State == StateDeleted {
		return errs.Preconditionf("kafka topic %q is deleted", t.Name)
	}
	return nil
}

// SetConsumption changes the drain state. It never touches `state` (orthogonal)
// or `traffic_share` (the application service re-normalises the siblings).
// Returns whether the value actually changed.
func (t *KafkaTopic) SetConsumption(c Consumption) (changed bool, err error) {
	if err := t.EnsureMutable(); err != nil {
		return false, err
	}
	if !c.Valid() {
		return false, errs.InvalidField("consumption", "must be ENABLED or DISABLED")
	}
	if t.Consumption == c {
		return false, nil
	}
	t.Consumption = c
	t.bumpGeneration()
	return true, nil
}

// SetTrafficShare is set by the application service as it re-normalises a
// channel's shards. Not a client operation.
func (t *KafkaTopic) SetTrafficShare(value float64) {
	t.TrafficShare = TrafficShare{Value: value, Unit: TrafficShareUnit}
}

// IncreasePartitions raises the desired partition count. A decrease is rejected
// (003.6 invariant); an equal value is a no-op. Bumps `generation` on a change.
func (t *KafkaTopic) IncreasePartitions(n int32) error {
	if err := t.EnsureMutable(); err != nil {
		return err
	}
	if n < t.Partitions {
		return errs.InvalidField("partitions",
			fmt.Sprintf("may only increase (have %d, requested %d)", t.Partitions, n))
	}
	if n == t.Partitions {
		return nil
	}
	t.Partitions = n
	t.bumpGeneration()
	return nil
}

// SetReplicationFactor changes the desired replication factor. Unlike
// `partitions` this may move in either direction — 003.6 constrains only
// partitions — but it must stay >= 1, and 003.8 additionally bounds it by the
// target cluster's broker count, which the caller checks because the shard does
// not know its cluster's shape. Bumps `generation` on a change.
func (t *KafkaTopic) SetReplicationFactor(n int32) error {
	if err := t.EnsureMutable(); err != nil {
		return err
	}
	if n < 1 {
		return errs.InvalidField("replication_factor", "must be >= 1")
	}
	if n == t.ReplicationFactor {
		return nil
	}
	t.ReplicationFactor = n
	t.bumpGeneration()
	return nil
}

// SetTopicConfigurationKey writes one key of this shard's `topic_configuration`
// layer and re-freezes the cluster⊕topic merge from clusterConfig, so the
// materialized view an agent reconciles matches the new declared state. An empty
// value removes the key, dropping the shard back to the cluster default.
//
// It is the governance write path (003.8) for `topic_configuration.<key>`; a
// key-at-a-time signature rather than a whole-map replace, because a policy
// changes one setting and must not clobber the rest. Returns whether anything
// changed; Rematerialize bumps `generation` when it did.
func (t *KafkaTopic) SetTopicConfigurationKey(
	key, value string, clusterConfig map[string]string,
) (changed bool, err error) {
	if err := t.EnsureMutable(); err != nil {
		return false, err
	}
	if key == "" {
		return false, errs.InvalidField("topic_configuration", "key must not be empty")
	}
	if t.TopicConfiguration == nil {
		t.TopicConfiguration = map[string]string{}
	}
	current, present := t.TopicConfiguration[key]
	switch {
	case value == "" && !present:
		return false, nil
	case value == "":
		delete(t.TopicConfiguration, key)
	case present && current == value:
		return false, nil
	default:
		t.TopicConfiguration[key] = value
	}
	t.Rematerialize(clusterConfig)
	return true, nil
}

// EffectiveConfigValue is the value of one configuration key as the shard
// currently declares it: its own layer first, then the frozen cluster⊕topic
// merge. It is what an arithmetic governance action reads before computing a
// delta (003.8), so an INCREASE against a cluster-default key starts from that
// default rather than from zero.
func (t *KafkaTopic) EffectiveConfigValue(key string) (string, bool) {
	if v, ok := t.TopicConfiguration[key]; ok {
		return v, true
	}
	v, ok := t.MaterializedConfiguration[key]
	return v, ok
}

// Rematerialize recomputes the frozen config merge from the given (current)
// cluster configuration and this shard's topic_configuration, and bumps
// `generation`. Called on a governance-driven config change (deliverable 13),
// never by an edit to the cluster's own configuration (003.3 / 003.6).
func (t *KafkaTopic) Rematerialize(clusterConfig map[string]string) {
	t.MaterializedConfiguration = Materialize(clusterConfig, t.TopicConfiguration)
	t.bumpGeneration()
}

// SetState applies a 003.6-legal transition (used by channel propagation in
// deliverable 10 and agent reporting later). Rejects an illegal transition.
func (t *KafkaTopic) SetState(to State) error {
	if !to.Valid() {
		return errs.InvalidField("state", "unknown state "+string(to))
	}
	if t.State == to {
		return nil
	}
	if !t.State.CanTransition(to) {
		return errs.Preconditionf("kafka topic %q: illegal transition %s → %s", t.Name, t.State, to)
	}
	t.State = to
	if to != StateDeleted {
		t.bumpGeneration()
	}
	return nil
}

// MarkMisplaced records that this async-channel shard's cluster no longer
// satisfies the owning channel's placement rules (003.7). It deliberately leaves
// KafkaClusterID alone — a placed shard is only ever moved by the migration flow
// (003.13) — and does not bump `generation`: the marker changes nothing an agent
// reconciles. Returns whether the marker or its reason actually changed.
func (t *KafkaTopic) MarkMisplaced(reason string) bool {
	if t.Misplaced && t.MisplacedReason == reason {
		return false
	}
	t.Misplaced = true
	t.MisplacedReason = reason
	return true
}

// ClearMisplaced drops the marker once the shard's cluster satisfies the
// channel's placement rules again. Returns whether anything changed.
func (t *KafkaTopic) ClearMisplaced() bool {
	if !t.Misplaced && t.MisplacedReason == "" {
		return false
	}
	t.Misplaced = false
	t.MisplacedReason = ""
	return true
}

// PlaceOn records the cluster an async-channel shard was just placed on. Only
// legal while the shard is unplaced — a placed shard moves through migration
// (003.13), never here.
func (t *KafkaTopic) PlaceOn(clusterID uuid.UUID, clusterName string) error {
	if t.KafkaClusterID != nil {
		return errs.Preconditionf("kafka topic %q is already placed on %q", t.Name, t.ClusterName)
	}
	id := clusterID
	t.KafkaClusterID = &id
	t.ClusterName = clusterName
	return nil
}

// AdoptPlacement completes a shard row that exists but was never given a cluster
// — an ADR-API-009 anomaly (a stale fixture, an aborted write) that placement
// heals rather than leaves stuck. It assigns the cluster, seeds the Kafka shape,
// re-freezes the config merge from that cluster's configuration, clears any
// misplaced marker, and bumps `generation` so the agent reconciles the now-real
// desired state. Illegal once the shard is placed.
func (t *KafkaTopic) AdoptPlacement(
	clusterID uuid.UUID, clusterName string,
	clusterConfig map[string]string, partitions, replicationFactor int32,
) error {
	if t.KafkaClusterID != nil {
		return errs.Preconditionf("kafka topic %q is already placed on %q", t.Name, t.ClusterName)
	}
	if partitions < 1 {
		return errs.InvalidField("partitions", "must be >= 1")
	}
	if replicationFactor < 1 {
		return errs.InvalidField("replication_factor", "must be >= 1")
	}
	id := clusterID
	t.KafkaClusterID = &id
	t.ClusterName = clusterName
	t.Partitions = partitions
	t.ReplicationFactor = replicationFactor
	t.MaterializedConfiguration = Materialize(clusterConfig, t.TopicConfiguration)
	t.Misplaced = false
	t.MisplacedReason = ""
	t.bumpGeneration()
	return nil
}

func (t *KafkaTopic) bumpGeneration() { t.Generation++ }

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
