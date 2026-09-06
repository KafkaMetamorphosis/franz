// Package selector implements the one Franz label-selector grammar (003.1
// "Label selectors"): a comma-separated list of requirements, all AND-ed.
//
//	key=value            key!=value
//	key IN (a, b, c)      key NOT IN (a, b)
//	key                   !key
//
// Whitespace around `,`, `=`, `!=`, `IN`, `(`, `)` is insignificant. Values are
// bare tokens; wrap in "…" if a value contains a space, comma, or bracket.
// An empty selector matches everything. `*` globs are allowed in values.
package selector

import (
	"sort"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/glob"
)

// Op is a requirement operator.
type Op int

const (
	OpEquals Op = iota
	OpNotEquals
	OpIn
	OpNotIn
	OpExists
	OpNotExists
)

// Requirement is one AND-ed clause of a selector.
type Requirement struct {
	Key    string
	Op     Op
	Values []string
}

func (r Requirement) matchValue(labelVal string) bool {
	for _, v := range r.Values {
		if glob.HasWildcard(v) {
			if glob.Match(v, labelVal) {
				return true
			}
		} else if glob.Unescape(v) == labelVal {
			return true
		}
	}
	return false
}

// Selector is a parsed selector expression.
type Selector struct {
	reqs []Requirement
}

// Empty reports whether the selector has no requirements (matches everything).
func (s Selector) Empty() bool { return len(s.reqs) == 0 }

// Requirements returns the parsed requirements (read-only).
func (s Selector) Requirements() []Requirement { return s.reqs }

// Match reports whether labels satisfy every requirement.
func (s Selector) Match(labels map[string]string) bool {
	for _, r := range s.reqs {
		v, present := labels[r.Key]
		switch r.Op {
		case OpExists:
			if !present {
				return false
			}
		case OpNotExists:
			if present {
				return false
			}
		case OpEquals:
			if !present || !r.matchValue(v) {
				return false
			}
		case OpNotEquals:
			if present && r.matchValue(v) {
				return false
			}
		case OpIn:
			if !present || !r.matchValue(v) {
				return false
			}
		case OpNotIn:
			if present && r.matchValue(v) {
				return false
			}
		}
	}
	return true
}

// String renders the selector back to canonical form (sorted by key).
func (s Selector) String() string {
	rs := append([]Requirement(nil), s.reqs...)
	sort.Slice(rs, func(i, j int) bool { return rs[i].Key < rs[j].Key })
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		switch r.Op {
		case OpExists:
			parts = append(parts, r.Key)
		case OpNotExists:
			parts = append(parts, "!"+r.Key)
		case OpEquals:
			parts = append(parts, r.Key+"="+quote(r.Values[0]))
		case OpNotEquals:
			parts = append(parts, r.Key+"!="+quote(r.Values[0]))
		case OpIn:
			parts = append(parts, r.Key+" IN ("+joinQuoted(r.Values)+")")
		case OpNotIn:
			parts = append(parts, r.Key+" NOT IN ("+joinQuoted(r.Values)+")")
		}
	}
	return strings.Join(parts, ", ")
}

func invalid(format string, args ...any) error {
	return errs.InvalidField("selector", errs.Invalidf(format, args...).Msg)
}
