package governance

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// noChange is the PolicyAction `result` for an action that was legal and ran but
// found the resource already in the desired state — the steady state of a policy
// that has driven a field to its cap.
const noChange = "no change"

// applier performs the whitelisted actions of a triggered policy (003.8 "Write
// whitelist"). It only ever writes Franz's **declared** state; the normal
// reconciliation path realises the change, and no agent is ever called from here.
//
// It reaches for the owning application service wherever one exists — pausing a
// channel has to cascade to its shards and notify the Resource Provider agents,
// and re-implementing that here would drift. It drops to the topic repository
// only for the shard fields no client-facing service exposes (partitions,
// replication factor, per-key topic configuration), because 003.6 makes those
// Franz-owned and the write whitelist is governance's own API for them.
type applier struct {
	channels in.AsyncChannelService
	clusters in.KafkaClusterService
	topics   in.KafkaTopicService

	topicRepo   out.TopicRepository
	clusterRepo out.ClusterRepository
	// notifier pushes the shard's new desired state to the agents holding its
	// cluster in scope. Optional — nil in tests that do not exercise the wire.
	notifier out.PartitionNotifier
}

// apply runs one action against one resolved resource and returns the
// operator-facing note that lands in the PolicyAction's `result`. An error is
// returned as well as a note: the caller records both and keeps going, because
// one bad rule must not stop the pass (003.8).
func (a applier) apply(
	ctx context.Context, realmID uuid.UUID, res *resource, action gov.Action,
) (string, error) {
	switch action.Kind {
	case gov.ActionAddLabel:
		return a.setLabel(ctx, res, action.Args[0], action.Args[1])
	case gov.ActionRemoveLabel:
		return a.removeLabel(ctx, res, action.Args[0])
	case gov.ActionSetStatus:
		return a.setStatus(ctx, res, action.Args[0])
	case gov.ActionUpdateField, gov.ActionIncreaseFieldBy, gov.ActionDecreaseFieldBy:
		return a.writeField(ctx, realmID, res, action)
	default:
		return "", errs.Invalidf("unsupported action kind %q", action.Kind)
	}
}

// --- labels --------------------------------------------------------------

func (a applier) setLabel(ctx context.Context, res *resource, key, value string) (string, error) {
	if current, ok := res.Labels[key]; ok && current == value {
		return noChange, nil
	}
	next := copyLabels(res.Labels)
	next[key] = value
	if err := a.writeLabels(ctx, res, next); err != nil {
		return "", err
	}
	return key + "=" + value, nil
}

func (a applier) removeLabel(ctx context.Context, res *resource, key string) (string, error) {
	if _, ok := res.Labels[key]; !ok {
		return noChange, nil
	}
	next := copyLabels(res.Labels)
	delete(next, key)
	if err := a.writeLabels(ctx, res, next); err != nil {
		return "", err
	}
	return "removed label " + key, nil
}

func (a applier) writeLabels(ctx context.Context, res *resource, labels map[string]string) error {
	switch res.Entity {
	case indicator.EntityAsyncChannel:
		_, err := a.channels.Update(ctx, in.UpdateChannelInput{Name: res.Name, Labels: &labels})
		return err
	case indicator.EntityKafkaCluster:
		_, err := a.clusters.Update(ctx, in.UpdateClusterInput{Name: res.Name, Labels: &labels})
		return err
	default:
		return errs.Preconditionf("a %s has no labels", res.Entity)
	}
}

// --- status --------------------------------------------------------------

// setStatus moves a channel or a cluster to PAUSED / ACTIVE / DELETED. A Kafka
// Topic is refused: its state is derived from its channel (003.6), so 003.8
// excludes KafkaTopic from SET_STATUS. The whitelist rejects it at write; this
// is the second line, in case a row predates the check.
func (a applier) setStatus(ctx context.Context, res *resource, status string) (string, error) {
	want := strings.TrimSpace(status)
	switch res.Entity {
	case indicator.EntityAsyncChannel:
		state, ok := gov.GovernableChannelState(want)
		if !ok {
			return "", errs.InvalidField("status", "unknown channel state "+status)
		}
		if res.Channel.State == state {
			return noChange, nil
		}
		return a.moveChannel(ctx, res.Name, string(state))

	case indicator.EntityKafkaCluster:
		state, ok := gov.GovernableClusterState(want)
		if !ok {
			return "", errs.InvalidField("status", "unknown cluster state "+status)
		}
		if res.Cluster.State == state {
			return noChange, nil
		}
		return a.moveCluster(ctx, res.Name, string(state))

	default:
		return "", errs.Preconditionf(
			"SET_STATUS is not applicable to %s — a topic's state follows its channel (003.6)",
			res.Entity)
	}
}

func (a applier) moveChannel(ctx context.Context, name, state string) (string, error) {
	var err error
	switch state {
	case "ACTIVE":
		_, err = a.channels.Resume(ctx, name)
	case "PAUSED":
		_, err = a.channels.Pause(ctx, name)
	case "DELETED":
		err = a.channels.Delete(ctx, name)
	}
	if err != nil {
		return "", err
	}
	return "state=" + state, nil
}

func (a applier) moveCluster(ctx context.Context, name, state string) (string, error) {
	var err error
	switch state {
	case "ACTIVE":
		_, err = a.clusters.Resume(ctx, name)
	case "PAUSED":
		_, err = a.clusters.Pause(ctx, name)
	case "DELETED":
		err = a.clusters.Delete(ctx, name)
	}
	if err != nil {
		return "", err
	}
	return "state=" + state, nil
}

// --- fields --------------------------------------------------------------

func (a applier) writeField(
	ctx context.Context, realmID uuid.UUID, res *resource, action gov.Action,
) (string, error) {
	switch res.Entity {
	case indicator.EntityKafkaTopic:
		return a.writeTopicField(ctx, realmID, res, action)
	case indicator.EntityKafkaCluster:
		return a.writeClusterField(ctx, res, action)
	default:
		return "", errs.Preconditionf("%s has no governable field %q",
			res.Entity, action.Target())
	}
}

// writeTopicField changes one shard field and re-offers the shard to the agents
// holding its cluster in scope, so the change is reconciled rather than left
// declared-only.
func (a applier) writeTopicField(
	ctx context.Context, realmID uuid.UUID, res *resource, action gov.Action,
) (string, error) {
	path := action.Target()

	// `consumption` goes through the topic service: draining a shard has to
	// re-normalise its siblings' traffic share (003.6), which the entity alone
	// cannot do.
	if path == gov.FieldConsumption && action.Kind == gov.ActionUpdateField {
		want := topic.Consumption(strings.TrimSpace(action.Args[1]))
		if res.Topic.Consumption == want {
			return noChange, nil
		}
		if _, err := a.topics.SetConsumption(ctx, res.Name, want); err != nil {
			return "", err
		}
		return "consumption=" + string(want), nil
	}

	// A per-key configuration change re-freezes the cluster⊕topic merge, so the
	// cluster's current configuration is loaded before the shard row is locked —
	// never from inside the shard's transaction.
	var clusterConfig map[string]string
	if strings.HasPrefix(path, gov.PrefixTopicConfiguration) {
		var err error
		if clusterConfig, err = a.clusterConfigOf(ctx, realmID, res.Topic); err != nil {
			return "", err
		}
	}

	var note string
	updated, err := a.topicRepo.MutateByFRN(ctx, realmID, res.FRN.Path(),
		func(t *topic.KafkaTopic) error {
			var err error
			note, err = applyTopicField(t, path, action, clusterConfig)
			return err
		})
	if err != nil {
		return "", err
	}
	if note != noChange && a.notifier != nil {
		a.notifier.ShardsChanged(ctx, realmID, []*topic.KafkaTopic{updated})
	}
	return note, nil
}

// applyTopicField is the pure half: it mutates the loaded shard and reports what
// it did. Split out so the arithmetic and the cap are testable without a store.
func applyTopicField(
	t *topic.KafkaTopic, path string, action gov.Action, clusterConfig map[string]string,
) (string, error) {
	switch {
	case path == gov.FieldPartitions:
		next, capped, err := resolveNumber(action, float64(t.Partitions))
		if err != nil {
			return "", err
		}
		n := int32(math.Round(next))
		if n == t.Partitions {
			return noChange, nil
		}
		if err := t.IncreasePartitions(n); err != nil {
			return "", err
		}
		return note("partitions", formatNumber(float64(n)), capped, action), nil

	case path == gov.FieldReplicationFactor:
		next, capped, err := resolveNumber(action, float64(t.ReplicationFactor))
		if err != nil {
			return "", err
		}
		n := int32(math.Round(next))
		if n == t.ReplicationFactor {
			return noChange, nil
		}
		if err := t.SetReplicationFactor(n); err != nil {
			return "", err
		}
		return note("replication_factor", formatNumber(float64(n)), capped, action), nil

	case strings.HasPrefix(path, gov.PrefixTopicConfiguration):
		key := strings.TrimPrefix(path, gov.PrefixTopicConfiguration)
		value, capped, err := resolveConfigValue(action, func() (string, bool) {
			return t.EffectiveConfigValue(key)
		})
		if err != nil {
			return "", err
		}
		changed, err := t.SetTopicConfigurationKey(key, value, clusterConfig)
		if err != nil {
			return "", err
		}
		if !changed {
			return noChange, nil
		}
		return note(path, value, capped, action), nil

	default:
		return "", errs.Preconditionf("KAFKA_TOPIC has no governable field %q", path)
	}
}

// clusterConfigOf reads the configuration of the cluster a shard sits on. An
// unplaced shard (ADR-API-009) has no cluster layer yet, which is not an error:
// the merge is just the shard's own keys until placement freezes it.
func (a applier) clusterConfigOf(
	ctx context.Context, realmID uuid.UUID, t *topic.KafkaTopic,
) (map[string]string, error) {
	if t.ClusterName == "" {
		return nil, nil
	}
	c, err := a.clusterRepo.Get(ctx, realmID, t.ClusterName)
	if err != nil {
		return nil, err
	}
	return c.Configuration, nil
}

func (a applier) writeClusterField(
	ctx context.Context, res *resource, action gov.Action,
) (string, error) {
	path := action.Target()
	input := in.UpdateClusterInput{Name: res.Name}

	switch {
	case path == gov.FieldBrokers:
		next, capped, err := resolveNumber(action, float64(res.Cluster.Brokers))
		if err != nil {
			return "", err
		}
		n := int32(math.Round(next))
		if n < 1 {
			return "", errs.InvalidField("brokers", "must be >= 1")
		}
		if n == res.Cluster.Brokers {
			return noChange, nil
		}
		input.Brokers = &n
		if _, err := a.clusters.Update(ctx, input); err != nil {
			return "", err
		}
		return note("brokers", formatNumber(float64(n)), capped, action), nil

	case path == gov.FieldDiskSize:
		value := strings.TrimSpace(action.Args[1])
		if value == res.Cluster.DiskSize {
			return noChange, nil
		}
		input.DiskSize = &value
		if _, err := a.clusters.Update(ctx, input); err != nil {
			return "", err
		}
		return "disk_size=" + value, nil

	case strings.HasPrefix(path, gov.PrefixClusterConfiguration):
		key := strings.TrimPrefix(path, gov.PrefixClusterConfiguration)
		value, capped, err := resolveConfigValue(action, func() (string, bool) {
			v, ok := res.Cluster.Configuration[key]
			return v, ok
		})
		if err != nil {
			return "", err
		}
		next := copyLabels(res.Cluster.Configuration)
		if current, ok := next[key]; ok && current == value {
			return noChange, nil
		}
		if value == "" {
			delete(next, key)
		} else {
			next[key] = value
		}
		input.Configuration = &next
		if _, err := a.clusters.Update(ctx, input); err != nil {
			return "", err
		}
		return note(path, value, capped, action), nil

	default:
		return "", errs.Preconditionf("KAFKA_CLUSTER has no governable field %q", path)
	}
}

// --- amount / cap arithmetic ---------------------------------------------

// resolveNumber computes the value a field action wants to write, given the
// field's current value. UPDATE_FIELD takes its literal; the two arithmetic
// kinds move `current` by `amount` and clamp to the per-action cap (cap.go),
// reporting whether the cap bit.
func resolveNumber(action gov.Action, current float64) (value float64, capped bool, err error) {
	if action.Kind == gov.ActionUpdateField {
		n, err := indicator.ParseQuantity(action.Args[1])
		if err != nil {
			return 0, false, errs.InvalidField("args[1]", err.Error())
		}
		return n, false, nil
	}
	amount, err := gov.ParseAmount(action.Args[1])
	if err != nil {
		return 0, false, errs.InvalidField("args[1]", err.Error())
	}
	bound, err := gov.ParseCap(action)
	if err != nil {
		return 0, false, errs.InvalidField("args[2]", err.Error())
	}
	next, capped := gov.ApplyArithmetic(action.Kind, current, amount, bound)
	return next, capped, nil
}

// resolveConfigValue is resolveNumber for a stringly-typed configuration key.
// UPDATE_FIELD writes its literal verbatim — a config value is not necessarily a
// number ("compact", "true") — while an arithmetic action requires the current
// value to parse as a quantity, since there is nothing to add to otherwise.
func resolveConfigValue(
	action gov.Action, currentOf func() (string, bool),
) (value string, capped bool, err error) {
	if action.Kind == gov.ActionUpdateField {
		return strings.TrimSpace(action.Args[1]), false, nil
	}
	raw, ok := currentOf()
	if !ok {
		return "", false, errs.Preconditionf(
			"%s has no current value to change by %s", action.Target(), action.Args[1])
	}
	current, err := indicator.ParseQuantity(raw)
	if err != nil {
		return "", false, errs.Preconditionf(
			"current value %q of %s is not a number, so it cannot be changed by %s",
			raw, action.Target(), action.Args[1])
	}
	next, capped, err := resolveNumber(action, current)
	if err != nil {
		return "", false, err
	}
	return formatNumber(next), capped, nil
}

// note renders the PolicyAction `result` for a field write, calling out a cap
// that bit so the operator can see why the policy stopped moving the field.
func note(field, value string, capped bool, action gov.Action) string {
	if !capped {
		return field + "=" + value
	}
	bound, _ := gov.ParseCap(action)
	return fmt.Sprintf("%s=%s (capped at %s)", field, value, bound.Raw)
}

// formatNumber renders a computed quantity the way an operator wrote it: as a
// plain integer whenever it is one, so "partitions=12" never comes back as
// "1.2e+01" and a byte count stays a byte count.
func formatNumber(n float64) string {
	if n == math.Trunc(n) && math.Abs(n) < 1e18 {
		return strconv.FormatInt(int64(n), 10)
	}
	return strconv.FormatFloat(n, 'f', -1, 64)
}

func copyLabels(m map[string]string) map[string]string {
	out := make(map[string]string, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}
