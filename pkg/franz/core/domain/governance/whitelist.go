package governance

import (
	"strconv"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
)

// The writable field paths of 003.8's write whitelist. FieldLabels is a
// pseudo-path standing for "the resource's label map", the target of
// ADD_LABEL / REMOVE_LABEL; the other constants are real fields.
const (
	FieldPartitions        = "partitions"
	FieldReplicationFactor = "replication_factor"
	FieldConsumption       = "consumption"
	FieldChannelPartitions = "channel_partitions"
	FieldState             = "state"
	FieldBrokers           = "brokers"
	FieldDiskSize          = "disk_size"
	FieldLabels            = "labels"

	// PrefixTopicConfiguration / PrefixClusterConfiguration introduce a keyed
	// sub-field: `topic_configuration.retention.ms`. canonicalField folds any
	// such path onto the wildcard entry in the matrix.
	PrefixTopicConfiguration   = "topic_configuration."
	PrefixClusterConfiguration = "cluster_configuration."

	wildcardTopicConfiguration   = PrefixTopicConfiguration + "*"
	wildcardClusterConfiguration = PrefixClusterConfiguration + "*"
)

// LabelPrefixFranz is the reserved label namespace (003.1). A governance action
// may only write labels outside it, except for the placement labels 003.8
// whitelists explicitly — and those are deferred, see deferredAction.
const LabelPrefixFranz = "franz."

// The reserved placement labels 003.8 whitelists but that need the migration
// flow (003.13) to mean anything (003.7 / placement domain constants).
const (
	labelPrefixAffinity     = "franz.affinity/"
	labelPrefixAntiAffinity = "franz.antiaffinity/"
	labelTaint              = "franz.taint"
)

// writeWhitelist is 003.8's "Write whitelist" table as data: the only
// (entity, field, kind) triples an Action may name. Anything outside it is
// rejected at CreatePolicy / UpdatePolicy with INVALID_ARGUMENT.
//
// Membership here means "the spec allows it", not "this deliverable performs
// it" — deferredAction filters the rows that need deliverables 16 / 18.
var writeWhitelist = map[indicator.Entity]map[string][]ActionKind{
	indicator.EntityKafkaTopic: {
		FieldPartitions:            {ActionUpdateField, ActionIncreaseFieldBy},
		FieldReplicationFactor:     {ActionUpdateField},
		wildcardTopicConfiguration: {ActionUpdateField, ActionIncreaseFieldBy, ActionDecreaseFieldBy},
		FieldConsumption:           {ActionUpdateField},
		FieldLabels:                {ActionAddLabel, ActionRemoveLabel},
	},
	indicator.EntityAsyncChannel: {
		FieldChannelPartitions: {ActionIncreaseFieldBy, ActionDecreaseFieldBy},
		FieldState:             {ActionSetStatus},
		FieldLabels:            {ActionAddLabel, ActionRemoveLabel},
	},
	indicator.EntityKafkaCluster: {
		FieldState:                   {ActionSetStatus},
		wildcardClusterConfiguration: {ActionUpdateField},
		FieldBrokers:                 {ActionUpdateField, ActionIncreaseFieldBy, ActionDecreaseFieldBy},
		FieldDiskSize:                {ActionUpdateField},
		FieldLabels:                  {ActionAddLabel, ActionRemoveLabel},
	},
}

// Whitelist exposes the matrix for callers that want to enumerate it (tests,
// the console). It returns a copy.
func Whitelist() map[indicator.Entity]map[string][]ActionKind {
	out := make(map[indicator.Entity]map[string][]ActionKind, len(writeWhitelist))
	for e, fields := range writeWhitelist {
		copied := make(map[string][]ActionKind, len(fields))
		for f, kinds := range fields {
			copied[f] = append([]ActionKind(nil), kinds...)
		}
		out[e] = copied
	}
	return out
}

// canonicalField folds a concrete field path onto its whitelist key: a keyed
// configuration path collapses to its wildcard entry, a label action collapses
// to FieldLabels, everything else is itself.
func canonicalField(kind ActionKind, field string) string {
	switch {
	case kind == ActionAddLabel || kind == ActionRemoveLabel:
		return FieldLabels
	case kind == ActionSetStatus:
		return FieldState
	case strings.HasPrefix(field, PrefixTopicConfiguration):
		return wildcardTopicConfiguration
	case strings.HasPrefix(field, PrefixClusterConfiguration):
		return wildcardClusterConfiguration
	default:
		return field
	}
}

// allowed reports whether the whitelist admits (entity, field, kind).
func allowed(entity indicator.Entity, kind ActionKind, field string) bool {
	for _, k := range writeWhitelist[entity][canonicalField(kind, field)] {
		if k == kind {
			return true
		}
	}
	return false
}

// validateAction runs every write-time check on one action of a policy whose
// matcher selects entity. index is the action's position, used to attribute the
// error to `actions[i]`.
func validateAction(entity indicator.Entity, index int, a Action) error {
	field := "actions[" + strconv.Itoa(index) + "]"

	if !a.Kind.Valid() {
		return errs.InvalidField(field, "unknown action kind "+string(a.Kind))
	}
	want := arity[a.Kind]
	switch {
	case a.Kind.IsArithmetic():
		// [field, amount] plus the optional per-action cap (cap.go).
		if len(a.Args) < want || len(a.Args) > want+1 {
			return errs.InvalidField(field+".args",
				string(a.Kind)+" takes [field, amount] and an optional cap")
		}
	default:
		if len(a.Args) != want {
			return errs.InvalidField(field+".args",
				string(a.Kind)+" takes exactly "+strconv.Itoa(want)+" argument(s), got "+
					strconv.Itoa(len(a.Args)))
		}
	}
	for i, arg := range a.Args {
		if strings.TrimSpace(arg) == "" {
			return errs.InvalidField(field+".args["+strconv.Itoa(i)+"]", "must not be empty")
		}
	}

	target := a.Target()
	if !allowed(entity, a.Kind, target) {
		return errs.InvalidField(field,
			string(a.Kind)+" on "+describeTarget(a.Kind, target)+" is not in the "+
				string(entity)+" write whitelist (003.8)")
	}
	if err := validateArgShape(entity, field, a); err != nil {
		return err
	}
	if _, err := ParseCap(a); err != nil {
		return errs.InvalidField(field+".args", err.Error())
	}
	if err := requireCap(entity, field, a); err != nil {
		return err
	}
	return deferredAction(entity, field, a)
}

func describeTarget(kind ActionKind, target string) string {
	if kind == ActionSetStatus {
		return FieldState
	}
	if kind == ActionAddLabel || kind == ActionRemoveLabel {
		return "label " + target
	}
	return target
}

// validateArgShape checks the value half of an action against the field it
// writes, so a policy that could only ever fail is rejected at write rather than
// at 3 a.m. when the limit is finally crossed.
func validateArgShape(entity indicator.Entity, field string, a Action) error {
	switch a.Kind {
	case ActionAddLabel, ActionRemoveLabel:
		return validateLabelKey(field, a.Args[0])

	case ActionSetStatus:
		if !isGovernableStatus(a.Args[0]) {
			return errs.InvalidField(field+".args[0]",
				"status must be ACTIVE, PAUSED or DELETED (got "+a.Args[0]+")")
		}
		return nil

	case ActionUpdateField:
		return validateUpdateValue(entity, field, a.Args[0], a.Args[1])

	case ActionIncreaseFieldBy, ActionDecreaseFieldBy:
		if err := validateConfigKey(field, a.Args[0]); err != nil {
			return err
		}
		if _, err := ParseAmount(a.Args[1]); err != nil {
			return errs.InvalidField(field+".args[1]", err.Error())
		}
		if a.Kind == ActionDecreaseFieldBy && a.Args[0] == FieldPartitions {
			return errs.InvalidField(field,
				"partitions may only increase (003.6)")
		}
		return nil
	}
	return nil
}

// validateUpdateValue type-checks the literal an UPDATE_FIELD writes.
func validateUpdateValue(entity indicator.Entity, field, path, value string) error {
	if err := validateConfigKey(field, path); err != nil {
		return err
	}
	switch path {
	case FieldPartitions, FieldReplicationFactor, FieldBrokers, FieldChannelPartitions:
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || n < 1 {
			return errs.InvalidField(field+".args[1]",
				path+" must be an integer >= 1 (got "+value+")")
		}
	case FieldConsumption:
		if c := topic.Consumption(strings.TrimSpace(value)); !c.Valid() {
			return errs.InvalidField(field+".args[1]",
				"consumption must be ENABLED or DISABLED (got "+value+")")
		}
	}
	_ = entity
	return nil
}

// validateConfigKey enforces 003.8's "`<key>` must be a real Kafka topic config
// key" for `topic_configuration.<key>`. `cluster_configuration.<key>` is left
// open: ADR-API-010 makes that map the free-form home for topic-config defaults
// *plus* the Franz-vocabulary keys (`partitions`, `replication-factor`,
// `kafka-version`), and Franz never validates it elsewhere.
func validateConfigKey(field, path string) error {
	if key, ok := strings.CutPrefix(path, PrefixTopicConfiguration); ok {
		if key == "" {
			return errs.InvalidField(field+".args[0]",
				"topic_configuration.<key> needs a key")
		}
		if !topic.IsKafkaConfigKey(key) {
			return errs.InvalidField(field+".args[0]",
				key+" is not a Kafka topic configuration key")
		}
		return nil
	}
	if key, ok := strings.CutPrefix(path, PrefixClusterConfiguration); ok {
		if key == "" {
			return errs.InvalidField(field+".args[0]",
				"cluster_configuration.<key> needs a key")
		}
	}
	return nil
}

// validateLabelKey rejects a reserved `franz.*` key that 003.8 does not
// whitelist. The whitelisted ones (affinity / antiaffinity on a channel,
// franz.taint on a cluster) pass here and are caught by deferredAction.
func validateLabelKey(field, key string) error {
	if !strings.HasPrefix(key, LabelPrefixFranz) {
		return nil
	}
	if isWhitelistedReservedLabel(key) {
		return nil
	}
	return errs.InvalidField(field+".args[0]",
		"the reserved label "+key+" is not writable by a policy (003.8)")
}

func isWhitelistedReservedLabel(key string) bool {
	return strings.HasPrefix(key, labelPrefixAffinity) ||
		strings.HasPrefix(key, labelPrefixAntiAffinity) ||
		key == labelTaint
}

// isGovernableStatus is the SET_STATUS value set (003.8): PAUSED / ACTIVE /
// DELETED, on a channel or a cluster only. Both domains use the same three
// names, so one check covers them.
func isGovernableStatus(s string) bool {
	switch strings.TrimSpace(s) {
	case string(channel.StateActive), string(channel.StatePaused), string(channel.StateDeleted):
		return true
	default:
		return false
	}
}

// GovernableClusterState maps a SET_STATUS argument onto a cluster state.
func GovernableClusterState(s string) (cluster.State, bool) {
	v := cluster.State(strings.TrimSpace(s))
	return v, v.Valid()
}

// GovernableChannelState maps a SET_STATUS argument onto a channel state.
func GovernableChannelState(s string) (channel.State, bool) {
	v := channel.State(strings.TrimSpace(s))
	return v, v.Valid()
}

// deferredAction rejects the whitelist rows this deliverable cannot honour. They
// are legal per 003.8 and will be accepted once the work they depend on lands;
// until then a policy carrying one is refused at write (FAILED_PRECONDITION)
// rather than accepted and silently ignored at evaluation time — an accepted
// policy that never acts is the worse failure.
func deferredAction(entity indicator.Entity, field string, a Action) error {
	// Placement / re-shard actions need the migration flow (003.13, deliverables
	// 16 and 18): moving a shard, re-shaping a channel, or draining a cluster.
	if entity == indicator.EntityAsyncChannel && a.Target() == FieldChannelPartitions {
		return errs.Preconditionf(
			"%s: changing channel_partitions is a staged re-shard and needs the migration flow (003.13)", field)
	}
	if (a.Kind == ActionAddLabel || a.Kind == ActionRemoveLabel) &&
		isWhitelistedReservedLabel(a.Target()) {
		return errs.Preconditionf(
			"%s: writing the placement label %s triggers re-placement / migration and needs the migration flow (003.13)",
			field, a.Target())
	}
	// A Kafka Topic has no label map — neither kafka.proto's KafkaTopic nor the
	// kafka_topic table carries one — so 003.8's KAFKA_TOPIC label row has
	// nothing to write to. Rejected until the entity gains labels.
	if entity == indicator.EntityKafkaTopic &&
		(a.Kind == ActionAddLabel || a.Kind == ActionRemoveLabel) {
		return errs.Preconditionf(
			"%s: a Kafka Topic has no labels (kafka.proto KafkaTopic carries no label map)", field)
	}
	return nil
}

// requireCap enforces the one place 003.8 calls a cap mandatory: an increase of
// `KAFKA_TOPIC.partitions`, which is irreversible (003.6 partitions may only
// rise) and, with no cooldown (003.8 OQ2), otherwise unbounded.
func requireCap(entity indicator.Entity, field string, a Action) error {
	if entity != indicator.EntityKafkaTopic || a.Kind != ActionIncreaseFieldBy {
		return nil
	}
	if a.Target() != FieldPartitions {
		return nil
	}
	c, err := ParseCap(a)
	if err != nil {
		return errs.InvalidField(field+".args", err.Error())
	}
	if !c.Present {
		return errs.InvalidField(field+".args",
			"INCREASE_FIELD_BY on partitions requires a cap (a third arg \"max=<n>\") — "+
				"the increase is irreversible and there is no cooldown (003.8)")
	}
	return nil
}
