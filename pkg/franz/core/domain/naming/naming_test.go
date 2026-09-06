package naming

import (
	"strings"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

func TestValidate(t *testing.T) {
	valid := []string{
		"a", "a1", "1a", "abc", "a-b", "a.b", "a_b",
		"my-channel", "orders.v2", "cluster_01",
		strings.Repeat("a", 200),
	}
	for _, n := range valid {
		if err := Validate(n); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", n, err)
		}
	}

	invalid := []string{
		"",                       // too short
		strings.Repeat("a", 201), // too long
		"A", "aBc",               // upper case
		"-a", "a-", ".a", "a.", "_a", "a_", // leading/trailing punctuation
		"a b",  // space
		"a/b",  // slash
		"a*b",  // wildcard
		"a:b",  // colon
		"café", // non-ascii
	}
	for _, n := range invalid {
		if err := Validate(n); err == nil {
			t.Errorf("Validate(%q) = nil, want error", n)
		}
	}
}

func TestValidateReturnsFieldViolation(t *testing.T) {
	err := Validate("BAD")
	e, ok := errs.As(err)
	if !ok {
		t.Fatal("not a domain error")
	}
	if e.Kind != errs.InvalidArgument {
		t.Errorf("kind = %v", e.Kind)
	}
	if len(e.Violations) != 1 || e.Violations[0].Field != "name" {
		t.Errorf("violations = %+v", e.Violations)
	}
}

func TestValid(t *testing.T) {
	if !Valid("ok-name") {
		t.Error("Valid(ok-name) = false")
	}
	if Valid("Bad Name") {
		t.Error("Valid(Bad Name) = true")
	}
}
