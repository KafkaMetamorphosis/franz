package accesspolicy

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

func st(effect Effect, p Principal, perms ...Permission) Statement {
	return Statement{Effect: effect, Principal: p, Permissions: perms}
}

func TestValidate(t *testing.T) {
	frn := Principal{ClientFRN: "frn:acme:client:xpto-*"}
	sel := Principal{LabelSelector: "team=payments"}

	cases := []struct {
		name string
		p    Policy
		ok   bool
	}{
		{"empty policy is valid", Policy{}, true},
		{"well-formed", Policy{Statements: []Statement{
			st(Allow, frn, Read),
			st(Deny, sel, Read, Write),
		}}, true},
		{"unspecified effect", Policy{Statements: []Statement{st("", frn, Read)}}, false},
		{"empty permissions", Policy{Statements: []Statement{st(Allow, frn)}}, false},
		{"unknown permission", Policy{Statements: []Statement{st(Allow, frn, "EXECUTE")}}, false},
		{"duplicate permission", Policy{Statements: []Statement{st(Allow, frn, Read, Read)}}, false},
		{"principal with no criterion", Policy{Statements: []Statement{st(Allow, Principal{}, Read)}}, false},
		{"principal frn only", Policy{Statements: []Statement{st(Allow, Principal{ClientFRN: "x"}, Read)}}, true},
		{"principal selector only", Policy{Statements: []Statement{st(Allow, Principal{LabelSelector: "a=b"}, Write)}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if tc.ok && err != nil {
				t.Fatalf("want ok, got %v", err)
			}
			if !tc.ok && errs.KindOf(err) != errs.InvalidArgument {
				t.Fatalf("want InvalidArgument, got %v", err)
			}
		})
	}
}

func TestNoStatementCap(t *testing.T) {
	// 003.5 OQ2 deferred — a large well-formed policy is accepted.
	var stmts []Statement
	for i := 0; i < 500; i++ {
		stmts = append(stmts, st(Allow, Principal{ClientFRN: "x"}, Read))
	}
	if err := (Policy{Statements: stmts}).Validate(); err != nil {
		t.Fatalf("500 statements should be fine (no cap): %v", err)
	}
}
