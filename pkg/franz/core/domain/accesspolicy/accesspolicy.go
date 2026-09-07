// Package accesspolicy is the channel access-policy document (003.5): the
// data-plane authorization model deciding whether a Client may read from or
// write to an Async Channel. This deliverable (10) owns the document's shape and
// its write validation only — principal matching and evaluation are the engine
// in deliverable 15.
package accesspolicy

import (
	"strconv"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// Effect is a statement's verdict. EFFECT_UNSPECIFIED is rejected at write time.
type Effect string

const (
	Allow Effect = "ALLOW"
	Deny  Effect = "DENY"
)

// Valid reports whether e is ALLOW or DENY.
func (e Effect) Valid() bool { return e == Allow || e == Deny }

// Permission is a data-plane action.
type Permission string

const (
	Read  Permission = "READ"
	Write Permission = "WRITE"
)

// Valid reports whether p is READ or WRITE.
func (p Permission) Valid() bool { return p == Read || p == Write }

// Principal selects the clients a statement applies to. A statement matches a
// client when ClientFRN matches (if set) OR the selector matches the client's
// labels (if set); at least one must be set (003.5). Both accept `*` globs — the
// matching logic lives in deliverable 15.
type Principal struct {
	ClientFRN     string
	LabelSelector string
}

// hasCriterion reports whether at least one of the two fields is set.
func (p Principal) hasCriterion() bool {
	return p.ClientFRN != "" || p.LabelSelector != ""
}

// Statement is one grant/deny rule.
type Statement struct {
	Effect      Effect
	Principal   Principal
	Permissions []Permission
}

// Policy is the whole document — an ordered list of statements. Order does not
// affect evaluation (DENY always wins); it is preserved for round-tripping.
type Policy struct {
	Statements []Statement
}

// Empty reports whether the policy has no statements (denies everyone).
func (p Policy) Empty() bool { return len(p.Statements) == 0 }

// Validate checks every statement for 003.5 well-formedness: effect is ALLOW or
// DENY, permissions is a non-empty set of valid values, and the principal has at
// least one criterion. It does not resolve any client. There is no statement
// cap yet (003.5 OQ2). Returns an INVALID_ARGUMENT domain error on the first
// problem.
func (p Policy) Validate() error {
	for i, s := range p.Statements {
		if !s.Effect.Valid() {
			return errs.InvalidField("access_policy",
				statementRef(i)+": effect must be ALLOW or DENY")
		}
		if len(s.Permissions) == 0 {
			return errs.InvalidField("access_policy",
				statementRef(i)+": permissions must be non-empty")
		}
		seen := map[Permission]bool{}
		for _, perm := range s.Permissions {
			if !perm.Valid() {
				return errs.InvalidField("access_policy",
					statementRef(i)+": unknown permission "+string(perm))
			}
			if seen[perm] {
				return errs.InvalidField("access_policy",
					statementRef(i)+": duplicate permission "+string(perm))
			}
			seen[perm] = true
		}
		if !s.Principal.hasCriterion() {
			return errs.InvalidField("access_policy",
				statementRef(i)+": principal must set client_frn and/or labels")
		}
	}
	return nil
}

func statementRef(i int) string {
	return "statement " + strconv.Itoa(i)
}
