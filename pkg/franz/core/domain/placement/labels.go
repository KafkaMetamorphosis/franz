// Package placement is the channel → cluster selection domain (003.7): which
// Kafka Cluster each of an Async Channel's async-channel shards lives on.
//
// It is pure logic with no I/O. Placement is driven entirely by reserved
// `franz.*` labels — the channel declares what it wants, each cluster describes
// itself, and Franz matches the two with the one 003.1 selector grammar
// (domain/selector). There is no cluster field on a channel.
//
// Vocabulary: an *async-channel shard* is one `channel_partitions` slice of an
// Async Channel, materialised as one `kafka_topic` row and one real Kafka topic.
// It is not a Kafka partition — a channel shard has its own `partitions` count.
package placement

import (
	"sort"
	"strconv"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/naming"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
)

// The reserved placement labels (003.1 "Reserved labels", 003.7 "Placement
// inputs"). The first four live on an Async Channel, the last two on a Kafka
// Cluster.
const (
	// LabelAffinitySelector carries a 003.1 selector expression a cluster's
	// labels must satisfy to be a candidate. Absent ⇒ no candidates at all:
	// placement is opt-in.
	LabelAffinitySelector = "franz.affinity/selector"
	// LabelAntiAffinitySelector carries a 003.1 selector expression; a candidate
	// whose labels satisfy it is dropped. Negation lives here, not in the
	// selector grammar.
	LabelAntiAffinitySelector = "franz.antiaffinity/selector"
	// LabelShardSize is how many distinct clusters to spread the channel's
	// async-channel shards across. Integer ≥ 1, default DefaultShardSize.
	LabelShardSize = "franz.affinity/shard-size"
	// LabelToleration is a comma-separated list of `<name>:<effect>` taints the
	// channel tolerates.
	LabelToleration = "franz.taint/toleration"

	// LabelTaint is a cluster's `<name>:<effect>` taint.
	LabelTaint = "franz.taint"
	// LabelWeight is a cluster's relative preference among candidates. Integer,
	// default DefaultWeight, higher wins.
	LabelWeight = "franz.affinity/weight"
)

// Defaults for the two numeric reserved labels (003.7).
const (
	DefaultShardSize = 1
	DefaultWeight    = 1
)

// Effect is what a taint does to placement (003.7 "Taints").
type Effect string

const (
	// EffectNoCreation blocks new async-channel shard placement; existing shards
	// stay. A channel with a matching toleration is exempt.
	EffectNoCreation Effect = "no-creation"
	// EffectDrain blocks new placement and marks every shard on the cluster for
	// migration. It cannot be tolerated.
	EffectDrain Effect = "drain"
)

// Valid reports whether e is a known effect.
func (e Effect) Valid() bool { return e == EffectNoCreation || e == EffectDrain }

// Taint is one `<name>:<effect>` pair, on a cluster (`franz.taint`) or in a
// channel's toleration list (`franz.taint/toleration`).
type Taint struct {
	Name   string
	Effect Effect
}

// String renders the canonical `<name>:<effect>` form.
func (t Taint) String() string { return t.Name + ":" + string(t.Effect) }

// ParseTaint parses one `<name>:<effect>` pair. Surrounding whitespace is
// insignificant.
func ParseTaint(field, raw string) (Taint, error) {
	name, effect, found := strings.Cut(strings.TrimSpace(raw), ":")
	if !found {
		return Taint{}, errs.InvalidField(field,
			"taint "+strconv.Quote(raw)+" must be <name>:<effect>")
	}
	name, effect = strings.TrimSpace(name), strings.TrimSpace(effect)
	if !naming.ValidLabelName(name) {
		return Taint{}, errs.InvalidField(field,
			"taint name "+strconv.Quote(name)+" must match "+naming.LabelNamePattern)
	}
	if !Effect(effect).Valid() {
		return Taint{}, errs.InvalidField(field,
			"taint effect "+strconv.Quote(effect)+" must be "+
				string(EffectNoCreation)+" or "+string(EffectDrain))
	}
	return Taint{Name: name, Effect: Effect(effect)}, nil
}

// ChannelRules is an Async Channel's parsed placement labels.
type ChannelRules struct {
	affinity     selector.Selector
	hasAffinity  bool
	antiAffinity selector.Selector
	hasAnti      bool
	shardSize    int
	tolerations  []Taint
}

// ParseChannelRules reads the reserved placement labels off an Async Channel.
// A malformed value is INVALID_ARGUMENT — the channel write is rejected. Absent
// labels take their 003.7 defaults; an absent `franz.affinity/selector` leaves
// the rules opted out of placement entirely.
func ParseChannelRules(labels map[string]string) (ChannelRules, error) {
	rules := ChannelRules{shardSize: DefaultShardSize}

	if raw, ok := labels[LabelAffinitySelector]; ok {
		sel, err := selector.Parse(raw)
		if err != nil {
			return ChannelRules{}, reservedLabelErr(LabelAffinitySelector, err)
		}
		rules.affinity, rules.hasAffinity = sel, true
	}
	if raw, ok := labels[LabelAntiAffinitySelector]; ok {
		sel, err := selector.Parse(raw)
		if err != nil {
			return ChannelRules{}, reservedLabelErr(LabelAntiAffinitySelector, err)
		}
		// An empty anti-affinity expression matches everything (003.1) and would
		// silently exclude every candidate; reject it as a configuration mistake.
		if sel.Empty() {
			return ChannelRules{}, errs.InvalidField("labels",
				"reserved label "+LabelAntiAffinitySelector+" must not be empty")
		}
		rules.antiAffinity, rules.hasAnti = sel, true
	}
	if raw, ok := labels[LabelShardSize]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n < 1 {
			return ChannelRules{}, errs.InvalidField("labels",
				"reserved label "+LabelShardSize+" must be an integer >= 1, got "+strconv.Quote(raw))
		}
		rules.shardSize = n
	}
	if raw, ok := labels[LabelToleration]; ok {
		for _, part := range strings.Split(raw, ",") {
			if strings.TrimSpace(part) == "" {
				return ChannelRules{}, errs.InvalidField("labels",
					"reserved label "+LabelToleration+" has an empty entry")
			}
			t, err := ParseTaint("labels", part)
			if err != nil {
				return ChannelRules{}, reservedLabelErr(LabelToleration, err)
			}
			rules.tolerations = append(rules.tolerations, t)
		}
	}
	return rules, nil
}

// ShardSize is the declared cluster spread, defaulted.
func (r ChannelRules) ShardSize() int { return r.shardSize }

// OptedIn reports whether the channel declared a `franz.affinity/selector` at
// all. Without one there are no candidates and no async-channel shard is ever
// materialised (003.7 step 1).
func (r ChannelRules) OptedIn() bool { return r.hasAffinity }

// Tolerates reports whether the channel carries a toleration matching t.
func (r ChannelRules) Tolerates(t Taint) bool {
	for _, have := range r.tolerations {
		if have == t {
			return true
		}
	}
	return false
}

// ClusterRules is a Kafka Cluster's parsed placement labels.
type ClusterRules struct {
	// Taint is the cluster's `franz.taint`, nil when it carries none.
	Taint *Taint
	// Weight is `franz.affinity/weight`, defaulted.
	Weight int
}

// ParseClusterRules reads the reserved placement labels off a Kafka Cluster.
func ParseClusterRules(labels map[string]string) (ClusterRules, error) {
	rules := ClusterRules{Weight: DefaultWeight}
	if raw, ok := labels[LabelTaint]; ok {
		t, err := ParseTaint("labels", raw)
		if err != nil {
			return ClusterRules{}, reservedLabelErr(LabelTaint, err)
		}
		rules.Taint = &t
	}
	if raw, ok := labels[LabelWeight]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return ClusterRules{}, errs.InvalidField("labels",
				"reserved label "+LabelWeight+" must be an integer, got "+strconv.Quote(raw))
		}
		rules.Weight = n
	}
	return rules, nil
}

// ValidateChannelLabels rejects a malformed reserved placement label on an Async
// Channel write (003.7 / 003.1). It is the write-path guard that keeps
// ParseChannelRules total on stored rows.
func ValidateChannelLabels(labels map[string]string) error {
	_, err := ParseChannelRules(labels)
	return err
}

// ValidateClusterLabels rejects a malformed reserved placement label on a Kafka
// Cluster write. It complements scope.ValidateClusterLabels, which owns the
// disjoint `franz.placement/*` prefix.
func ValidateClusterLabels(labels map[string]string) error {
	_, err := ParseClusterRules(labels)
	return err
}

// CanHost reports whether c may keep hosting an async-channel shard that is
// already placed on it, and why not when it may not. It is the misplaced-marker
// test (003.7 "Re-placement"): a cluster that went PAUSED / DELETED, stopped
// satisfying the affinity selector, started satisfying the anti-affinity
// selector, or gained a `drain` taint can no longer host the shard.
//
// A `no-creation` taint deliberately does not fail this test — it blocks new
// placement only, existing channel shards stay (003.7 "Taints").
func (r ChannelRules) CanHost(c *cluster.Cluster) (bool, string) {
	if c == nil {
		return false, "cluster is deleted or no longer registered"
	}
	if c.State != cluster.StateActive {
		return false, "cluster " + c.Name + " is " + string(c.State)
	}
	if !r.hasAffinity {
		return false, "channel declares no " + LabelAffinitySelector
	}
	if !r.affinity.Match(c.Labels) {
		return false, "cluster " + c.Name + " does not satisfy " + LabelAffinitySelector
	}
	if r.hasAnti && r.antiAffinity.Match(c.Labels) {
		return false, "cluster " + c.Name + " satisfies " + LabelAntiAffinitySelector
	}
	clusterRules, err := ParseClusterRules(c.Labels)
	if err != nil {
		return false, "cluster " + c.Name + " has malformed franz.* placement labels"
	}
	if clusterRules.Taint != nil && clusterRules.Taint.Effect == EffectDrain {
		return false, "cluster " + c.Name + " carries taint " + clusterRules.Taint.String()
	}
	return true, ""
}

// CanPlace reports whether a *new* async-channel shard may be created on c: the
// CanHost test plus the `no-creation` taint gate (003.7 steps 1–3).
func (r ChannelRules) CanPlace(c *cluster.Cluster) (bool, string) {
	if ok, reason := r.CanHost(c); !ok {
		return false, reason
	}
	clusterRules, err := ParseClusterRules(c.Labels)
	if err != nil {
		return false, "cluster " + c.Name + " has malformed franz.* placement labels"
	}
	if clusterRules.Taint != nil &&
		clusterRules.Taint.Effect == EffectNoCreation &&
		!r.Tolerates(*clusterRules.Taint) {
		return false, "cluster " + c.Name + " carries untolerated taint " + clusterRules.Taint.String()
	}
	return true, ""
}

// weightOf is the ordering key of a candidate. A candidate has already passed
// CanPlace, so its labels parse.
func weightOf(c *cluster.Cluster) int {
	rules, err := ParseClusterRules(c.Labels)
	if err != nil {
		return DefaultWeight
	}
	return rules.Weight
}

// sortCandidates orders by `franz.affinity/weight` descending, ties broken by
// name ascending — the deterministic order of 003.7 step 4.
func sortCandidates(candidates []*cluster.Cluster) {
	sort.SliceStable(candidates, func(i, j int) bool {
		wi, wj := weightOf(candidates[i]), weightOf(candidates[j])
		if wi != wj {
			return wi > wj
		}
		return candidates[i].Name < candidates[j].Name
	})
}

// channelRuleLabels are the reserved keys an Async Channel's placement reads.
var channelRuleLabels = []string{
	LabelAffinitySelector, LabelAntiAffinitySelector, LabelShardSize, LabelToleration,
}

// RulesChanged reports whether a channel label edit touched any reserved
// placement label. A channel's non-`franz.*` labels are free-form metadata that
// nothing in placement matches against (003.7 "Cross-entity behavior"), so an
// edit confined to them needs no placement pass.
//
// There is no cluster counterpart: a cluster's free-form labels are exactly what
// a channel's affinity selector matches, so any cluster label edit is
// placement-relevant.
func RulesChanged(before, after map[string]string) bool {
	for _, key := range channelRuleLabels {
		if before[key] != after[key] {
			return true
		}
	}
	return false
}

func reservedLabelErr(label string, cause error) error {
	msg := cause.Error()
	if e, ok := errs.As(cause); ok {
		msg = e.Msg
	}
	return errs.InvalidField("labels", "reserved label "+label+": "+msg)
}
