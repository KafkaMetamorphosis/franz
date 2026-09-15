package accesspolicy_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
)

func frnPrincipal(frn string) accesspolicy.Principal {
	return accesspolicy.Principal{ClientFRN: frn}
}

func selPrincipal(sel string) accesspolicy.Principal {
	return accesspolicy.Principal{LabelSelector: sel}
}

func stmt(effect accesspolicy.Effect, p accesspolicy.Principal, perms ...accesspolicy.Permission) accesspolicy.Statement {
	return accesspolicy.Statement{Effect: effect, Principal: p, Permissions: perms}
}

// TestWorkedExample pins 003.5's own worked example verbatim.
func TestWorkedExample(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, frnPrincipal("acme:client:xpto-*"), accesspolicy.Read),
		stmt(accesspolicy.Deny, frnPrincipal("acme:client:xpto-blah"), accesspolicy.Read),
	}}
	eval := accesspolicy.NewEvaluator(policy)

	if got := eval.Evaluate("acme:client:xpto-1", nil); !reflect.DeepEqual(
		got.Effective, []accesspolicy.Permission{accesspolicy.Read}) {
		t.Fatalf("xpto-1 = %+v, want READ allowed", got)
	}
	if got := eval.Evaluate("acme:client:xpto-blah", nil); got.Granted() {
		t.Fatalf("xpto-blah = %+v, want denied (DENY beats the wildcard ALLOW)", got)
	}
	if got := eval.Evaluate("acme:client:other", nil); got.Granted() {
		t.Fatalf("other = %+v, want denied (zero trust)", got)
	}
	// Nobody has WRITE in this policy.
	if got := eval.Evaluate("acme:client:xpto-1", nil); containsPerm(got.Effective, accesspolicy.Write) {
		t.Fatalf("xpto-1 = %+v, WRITE must not be granted", got)
	}
}

func containsPerm(perms []accesspolicy.Permission, p accesspolicy.Permission) bool {
	for _, x := range perms {
		if x == p {
			return true
		}
	}
	return false
}

func TestEmptyPolicyDeniesEveryone(t *testing.T) {
	eval := accesspolicy.NewEvaluator(accesspolicy.Policy{})
	if got := eval.Evaluate("acme:client:anyone", map[string]string{"team": "x"}); got.Granted() {
		t.Fatalf("empty policy = %+v, want denied", got)
	}
}

func TestReadAndWriteAreIndependent(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, frnPrincipal("acme:client:billing"), accesspolicy.Read),
		stmt(accesspolicy.Deny, frnPrincipal("acme:client:billing"), accesspolicy.Write),
	}}
	got := accesspolicy.NewEvaluator(policy).Evaluate("acme:client:billing", nil)
	if !reflect.DeepEqual(got.Effective, []accesspolicy.Permission{accesspolicy.Read}) {
		t.Fatalf("Effective = %+v, want [READ] only", got.Effective)
	}
}

func TestLabelSelectorPrincipalMatches(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, selPrincipal("team=payments"), accesspolicy.Read, accesspolicy.Write),
	}}
	eval := accesspolicy.NewEvaluator(policy)

	if got := eval.Evaluate("acme:client:x", map[string]string{"team": "payments"}); !reflect.DeepEqual(
		got.Effective, []accesspolicy.Permission{accesspolicy.Read, accesspolicy.Write}) {
		t.Fatalf("matching labels = %+v, want both permissions", got)
	}
	if got := eval.Evaluate("acme:client:y", map[string]string{"team": "infra"}); got.Granted() {
		t.Fatalf("non-matching labels = %+v, want denied", got)
	}
}

// TestPrincipalWithBothCriteriaMatchesOnEither pins 003.5 "matches when
// client_frn (if set) OR the selector (if set)" — an OR, not an AND, when a
// statement sets both.
func TestPrincipalWithBothCriteriaMatchesOnEither(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, accesspolicy.Principal{
			ClientFRN: "acme:client:billing", LabelSelector: "team=payments",
		}, accesspolicy.Read),
	}}
	eval := accesspolicy.NewEvaluator(policy)

	if !eval.Evaluate("acme:client:billing", nil).Granted() {
		t.Error("client_frn match alone should grant")
	}
	if !eval.Evaluate("acme:client:someone-else", map[string]string{"team": "payments"}).Granted() {
		t.Error("label match alone should grant")
	}
	if eval.Evaluate("acme:client:someone-else", map[string]string{"team": "infra"}).Granted() {
		t.Error("neither criterion matching should deny")
	}
}

// TestClientFRNGlobMatchesThePrefixLessForm pins the resolved design question:
// a wildcard client_frn is matched against the client's prefix-less stored FRN,
// never a rendered/prefixed one.
func TestClientFRNGlobMatchesThePrefixLessForm(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, frnPrincipal("acme:client:billing-*"), accesspolicy.Read),
	}}
	eval := accesspolicy.NewEvaluator(policy)

	if !eval.Evaluate("acme:client:billing-eu", nil).Granted() {
		t.Error("prefix-less form should match the glob")
	}
	if eval.Evaluate("frn:acme:client:billing-eu", nil).Granted() {
		t.Error("a rendered/prefixed FRN string must not match a prefix-less pattern")
	}
}

// TestDenyWinsRegardlessOfDocumentOrder pins 003.5 "explicit DENY always wins
// regardless of statement order".
func TestDenyWinsRegardlessOfDocumentOrder(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		// DENY listed first this time (opposite of the worked example).
		stmt(accesspolicy.Deny, frnPrincipal("acme:client:xpto-blah"), accesspolicy.Read),
		stmt(accesspolicy.Allow, frnPrincipal("acme:client:xpto-*"), accesspolicy.Read),
	}}
	if accesspolicy.NewEvaluator(policy).Evaluate("acme:client:xpto-blah", nil).Granted() {
		t.Fatal("DENY should still win regardless of position in the document")
	}
}

// TestMatchedByReportsAllAllowsOnGrant pins the OQ4 resolution: multiple
// matching ALLOWs are all listed, not just the first.
func TestMatchedByReportsAllAllowsOnGrant(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, frnPrincipal("acme:client:billing"), accesspolicy.Read),
		stmt(accesspolicy.Allow, selPrincipal("team=payments"), accesspolicy.Read),
	}}
	got := accesspolicy.NewEvaluator(policy).Evaluate(
		"acme:client:billing", map[string]string{"team": "payments"})
	if !got.Granted() {
		t.Fatal("expected READ granted")
	}
	if got.MatchedBy == "" {
		t.Fatal("MatchedBy must not be empty on a grant")
	}
	// Both statement 0 and statement 1 matched and allowed — both must be
	// named, not just whichever came first.
	if !strings.Contains(got.MatchedBy, "statement 0") || !strings.Contains(got.MatchedBy, "statement 1") {
		t.Fatalf("MatchedBy = %q, want both statements named", got.MatchedBy)
	}
}

// TestMatchedByEmptyOnZeroTrustDenial pins that a client matching nothing gets
// no note to render (there is no statement to explain).
func TestMatchedByEmptyOnZeroTrustDenial(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, frnPrincipal("acme:client:billing"), accesspolicy.Read),
	}}
	got := accesspolicy.NewEvaluator(policy).Evaluate("acme:client:someone-else", nil)
	if got.MatchedBy != "" {
		t.Fatalf("MatchedBy = %q, want empty on zero-trust denial", got.MatchedBy)
	}
}

// TestClientFRNThatResolvesToNoClientStillEvaluates pins 003.5 "a statement
// referencing a client_frn that does not currently resolve to a Client is
// valid — it simply matches nothing" — from the engine's point of view this is
// just an ordinary non-match, exercised here with an identity that happens not
// to correspond to anything (the engine itself never resolves a Client).
func TestClientFRNThatResolvesToNoClientStillEvaluates(t *testing.T) {
	policy := accesspolicy.Policy{Statements: []accesspolicy.Statement{
		stmt(accesspolicy.Allow, frnPrincipal("acme:client:ghost"), accesspolicy.Read),
	}}
	if accesspolicy.NewEvaluator(policy).Evaluate("acme:client:real", nil).Granted() {
		t.Fatal("a statement naming a different client must not grant this one anything")
	}
}

// TestBroadMatrix runs a wide (effect, principal-kind, permission) grid.
func TestBroadMatrix(t *testing.T) {
	cases := []struct {
		name       string
		statements []accesspolicy.Statement
		clientFRN  string
		labels     map[string]string
		wantRead   bool
		wantWrite  bool
	}{
		{
			name: "allow read and write via one statement",
			statements: []accesspolicy.Statement{
				stmt(accesspolicy.Allow, frnPrincipal("acme:client:x"), accesspolicy.Read, accesspolicy.Write),
			},
			clientFRN: "acme:client:x", wantRead: true, wantWrite: true,
		},
		{
			name: "allow read, deny write",
			statements: []accesspolicy.Statement{
				stmt(accesspolicy.Allow, frnPrincipal("acme:client:x"), accesspolicy.Read),
				stmt(accesspolicy.Deny, frnPrincipal("acme:client:x"), accesspolicy.Write),
			},
			clientFRN: "acme:client:x", wantRead: true, wantWrite: false,
		},
		{
			name: "wildcard allow, exact deny on a different permission",
			statements: []accesspolicy.Statement{
				stmt(accesspolicy.Allow, frnPrincipal("acme:client:*"), accesspolicy.Read, accesspolicy.Write),
				stmt(accesspolicy.Deny, frnPrincipal("acme:client:x"), accesspolicy.Read),
			},
			clientFRN: "acme:client:x", wantRead: false, wantWrite: true,
		},
		{
			name: "selector allow, unrelated frn deny does not affect it",
			statements: []accesspolicy.Statement{
				stmt(accesspolicy.Allow, selPrincipal("team=infra"), accesspolicy.Read),
				stmt(accesspolicy.Deny, frnPrincipal("acme:client:other"), accesspolicy.Read),
			},
			clientFRN: "acme:client:x", labels: map[string]string{"team": "infra"},
			wantRead: true, wantWrite: false,
		},
		{
			name:       "no statements at all",
			statements: nil,
			clientFRN:  "acme:client:x", wantRead: false, wantWrite: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := accesspolicy.NewEvaluator(accesspolicy.Policy{Statements: tc.statements}).
				Evaluate(tc.clientFRN, tc.labels)
			if containsPerm(got.Effective, accesspolicy.Read) != tc.wantRead {
				t.Errorf("READ = %v, want %v", containsPerm(got.Effective, accesspolicy.Read), tc.wantRead)
			}
			if containsPerm(got.Effective, accesspolicy.Write) != tc.wantWrite {
				t.Errorf("WRITE = %v, want %v", containsPerm(got.Effective, accesspolicy.Write), tc.wantWrite)
			}
		})
	}
}
