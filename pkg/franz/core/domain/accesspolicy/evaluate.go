package accesspolicy

import (
	"strconv"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/glob"
)

// Evaluation is the resolved outcome of one policy against one client
// identity (003.5's `ChannelClientAccess` / `ClientChannelAccess`, entity
// direction abstracted away — the caller attaches whichever the row is about).
type Evaluation struct {
	// Effective is READ and/or WRITE, in that fixed order; empty means the
	// client has no access to this channel at all (zero trust).
	Effective []Permission
	// MatchedBy is a human-readable note on which statement(s) decided the
	// granted permissions. Only ever describes ALLOW statements: a DENY, once
	// it wins, means that permission is not in Effective and so has nothing to
	// explain here.
	MatchedBy string
}

// Granted reports whether the evaluation carries at least one permission.
func (e Evaluation) Granted() bool { return len(e.Effective) > 0 }

// Evaluator pre-compiles a Policy's label selectors once so evaluating it
// against many clients (a full `ListChannelClients` / `ListClientChannelAccess`
// pass) does not re-parse the same selector string per client (003.5 §
// "Selector-match cost" — no explicit bound needed, but there is no reason to
// redo work either).
type Evaluator struct {
	statements []compiledStatement
}

type compiledStatement struct {
	Statement
	index    int
	selector selector.Selector
	hasSel   bool
}

// NewEvaluator compiles policy. An unparseable LabelSelector on a statement —
// which should not happen past Policy.Validate, but this function makes no
// assumption about that — simply never matches by selector; the statement can
// still match by client_frn.
func NewEvaluator(policy Policy) *Evaluator {
	compiled := make([]compiledStatement, len(policy.Statements))
	for i, s := range policy.Statements {
		cs := compiledStatement{Statement: s, index: i}
		if s.Principal.LabelSelector != "" {
			if sel, err := selector.Parse(s.Principal.LabelSelector); err == nil {
				cs.selector = sel
				cs.hasSel = true
			}
		}
		compiled[i] = cs
	}
	return &Evaluator{statements: compiled}
}

// Evaluate resolves the compiled policy against one client identity for both
// READ and WRITE (003.5 "READ and WRITE are evaluated independently").
//
// clientFRN is the client's prefix-less stored FRN (`FRN.Path()`, e.g.
// "acme:client:xpto-1") — a Principal's client_frn glob is matched against
// that same prefix-less form, consistent with every other FRN comparison in
// the schema (resource_frn, indicator_sample, policy_action are all stored and
// compared prefix-less; the Codec only renders a prefix at the API boundary).
func (e *Evaluator) Evaluate(clientFRN string, clientLabels map[string]string) Evaluation {
	var effective []Permission
	var notes []string
	for _, perm := range []Permission{Read, Write} {
		granted, note := e.decide(clientFRN, clientLabels, perm)
		if granted {
			effective = append(effective, perm)
			notes = append(notes, string(perm)+": "+note)
		}
	}
	return Evaluation{Effective: effective, MatchedBy: strings.Join(notes, "; ")}
}

// decide applies 003.5's per-permission algorithm: gather statements whose
// principal matches and whose permissions include perm; any DENY wins over
// every ALLOW regardless of document order; otherwise any ALLOW grants it;
// otherwise zero-trust denies it.
func (e *Evaluator) decide(
	clientFRN string, clientLabels map[string]string, perm Permission,
) (granted bool, note string) {
	var allowNotes []string
	for _, cs := range e.statements {
		if !containsPermission(cs.Permissions, perm) {
			continue
		}
		if !cs.principalMatches(clientFRN, clientLabels) {
			continue
		}
		if cs.Effect == Deny {
			// 003.5 OQ4 resolved: a DENY is the sole reason reported for a
			// denied permission, but a denied permission is never in
			// Effective, so that note is never rendered — the outcome alone
			// (false) is all the caller needs.
			return false, ""
		}
		allowNotes = append(allowNotes, describeAllow(cs))
	}
	if len(allowNotes) == 0 {
		return false, ""
	}
	// 003.5 OQ4 resolved: an ALLOW outcome lists every matching ALLOW
	// statement, since none of them individually "won" over the others.
	return true, strings.Join(allowNotes, ", ")
}

func (cs compiledStatement) principalMatches(clientFRN string, clientLabels map[string]string) bool {
	if cs.Principal.ClientFRN != "" && glob.Match(cs.Principal.ClientFRN, clientFRN) {
		return true
	}
	if cs.hasSel && cs.selector.Match(clientLabels) {
		return true
	}
	return false
}

func containsPermission(perms []Permission, perm Permission) bool {
	for _, p := range perms {
		if p == perm {
			return true
		}
	}
	return false
}

func describeAllow(cs compiledStatement) string {
	var principal string
	switch {
	case cs.Principal.ClientFRN != "":
		principal = "client_frn=" + cs.Principal.ClientFRN
	case cs.Principal.LabelSelector != "":
		principal = "labels=" + cs.Principal.LabelSelector
	default:
		principal = "no principal" // unreachable past Policy.Validate
	}
	return "statement " + strconv.Itoa(cs.index) + " (ALLOW " + principal + ")"
}
