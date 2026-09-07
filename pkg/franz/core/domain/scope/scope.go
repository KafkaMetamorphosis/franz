// Package scope is the Resource Provider scope resolver (005 ADR §1.2): which
// Kafka Clusters a Gregor Samsa instance is responsible for. It is pure domain
// logic with no I/O — Franz evaluates the match server-side and streams only
// in-scope work, so the agent never runs selector logic itself.
//
// The match is a plain conjunction of exact key/value pairs, deliberately *not*
// the 003.1 selector-expression grammar (that grammar stays reserved for
// channel → cluster affinity, 003.7).
package scope

import (
	"sort"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/naming"
)

const (
	// SelectorPrefix is the reserved Agent label prefix declaring an agent's
	// scope, e.g. `franz.placement-selector/env=prod`.
	SelectorPrefix = "franz.placement-selector/"

	// PlacementPrefix is the reserved Kafka Cluster label prefix declaring a
	// cluster's coordinates, e.g. `franz.placement/env=prod`.
	PlacementPrefix = "franz.placement/"
)

// Selector is an agent's scope: bare keys (the reserved prefix stripped) mapped
// to the value a cluster must carry under PlacementPrefix.
type Selector map[string]string

// SelectorFromLabels extracts the `franz.placement-selector/*` labels of an
// agent, stripping the prefix. Returns an empty (never nil) Selector when the
// agent declares none.
func SelectorFromLabels(labels map[string]string) Selector {
	sel := Selector{}
	for k, v := range labels {
		if key, ok := strings.CutPrefix(k, SelectorPrefix); ok {
			sel[key] = v
		}
	}
	return sel
}

// PlacementFromLabels extracts the `franz.placement/*` labels of a cluster,
// stripping the prefix. Note SelectorPrefix is not a prefix of PlacementPrefix,
// so the two never collide.
func PlacementFromLabels(labels map[string]string) map[string]string {
	placement := map[string]string{}
	for k, v := range labels {
		if key, ok := strings.CutPrefix(k, PlacementPrefix); ok {
			placement[key] = v
		}
	}
	return placement
}

// IsEmpty reports whether the agent declared no scope at all.
func (s Selector) IsEmpty() bool { return len(s) == 0 }

// Keys returns the selector keys, sorted — for deterministic log and error text.
func (s Selector) Keys() []string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Matches reports whether a cluster carrying clusterLabels is in scope: for
// every selector pair the cluster must carry `franz.placement/<key> = <value>`.
// Extra `franz.placement/*` keys on the cluster are ignored.
//
// An empty selector matches NOTHING — a deliberate departure from the 003.1
// "empty selector matches everything" rule, so a misconfigured instance cannot
// silently claim the whole fleet (005 ADR §1.2).
func (s Selector) Matches(clusterLabels map[string]string) bool {
	if s.IsEmpty() {
		return false
	}
	for key, want := range s {
		if got, ok := clusterLabels[PlacementPrefix+key]; !ok || got != want {
			return false
		}
	}
	return true
}

// Resolve returns the subset of clusters in scope for an agent with the given
// labels, preserving the input order. Soft-deleted clusters are never in scope —
// a deleted cluster hosts no live partitions (003.3 delete guard).
func Resolve(agentLabels map[string]string, clusters []*cluster.Cluster) []*cluster.Cluster {
	sel := SelectorFromLabels(agentLabels)
	if sel.IsEmpty() {
		return nil
	}
	var inScope []*cluster.Cluster
	for _, c := range clusters {
		if c == nil || c.State == cluster.StateDeleted {
			continue
		}
		if sel.Matches(c.Labels) {
			inScope = append(inScope, c)
		}
	}
	return inScope
}

// ValidateAgentLabels checks the reserved `franz.placement-selector/*` labels an
// agent declares: the part after the prefix must be a legal label name and the
// value must not be empty (an empty value would silently match a cluster that
// merely carries the key).
func ValidateAgentLabels(labels map[string]string) error {
	for k, v := range labels {
		key, ok := strings.CutPrefix(k, SelectorPrefix)
		if !ok {
			continue
		}
		if err := validateLabelName(key); err != nil {
			return errs.InvalidField("labels",
				"reserved label "+k+": "+err.Error())
		}
		if v == "" {
			return errs.InvalidField("labels",
				"reserved label "+k+" must have a non-empty value")
		}
	}
	return nil
}

// ValidateClusterLabels checks the reserved `franz.placement/*` labels a Kafka
// Cluster declares, with the same rules as ValidateAgentLabels.
func ValidateClusterLabels(labels map[string]string) error {
	for k, v := range labels {
		key, ok := strings.CutPrefix(k, PlacementPrefix)
		if !ok {
			continue
		}
		if err := validateLabelName(key); err != nil {
			return errs.InvalidField("labels",
				"reserved label "+k+": "+err.Error())
		}
		if v == "" {
			return errs.InvalidField("labels",
				"reserved label "+k+" must have a non-empty value")
		}
	}
	return nil
}

// maxLabelNameLen is the Kubernetes-style label-name cap (003.1 "Reserved labels").
const maxLabelNameLen = 63

// validateLabelName applies the 003.1 reserved-label `<name>` rule:
// `[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?`, at most 63 characters.
func validateLabelName(name string) error {
	if name == "" {
		return errs.Invalidf("label name must not be empty")
	}
	if len(name) > maxLabelNameLen {
		return errs.Invalidf("label name must be at most %d characters", maxLabelNameLen)
	}
	if !naming.ValidLabelName(name) {
		return errs.Invalidf("label name %q must match [A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?", name)
	}
	return nil
}
