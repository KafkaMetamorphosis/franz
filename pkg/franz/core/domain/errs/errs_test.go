package errs

import (
	"errors"
	"fmt"
	"testing"
)

func TestKindOf(t *testing.T) {
	cases := []struct {
		err  error
		want Kind
	}{
		{Internalf("boom"), Internal},
		{Invalidf("bad %s", "x"), InvalidArgument},
		{NotFoundf("nope"), NotFound},
		{Existsf("dup"), AlreadyExists},
		{Preconditionf("state"), FailedPrecondition},
		{Deniedf("no"), PermissionDenied},
		{Exhaustedf("quota"), ResourceExhausted},
		{errors.New("plain"), Internal},
		{fmt.Errorf("wrapped: %w", NotFoundf("inner")), NotFound},
		{nil, Internal},
	}
	for _, c := range cases {
		if got := KindOf(c.err); got != c.want {
			t.Errorf("KindOf(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestInvalidFieldViolations(t *testing.T) {
	err := InvalidField("name", "too long")
	e, ok := As(err)
	if !ok {
		t.Fatal("As failed")
	}
	if e.Kind != InvalidArgument {
		t.Errorf("kind = %v", e.Kind)
	}
	if len(e.Violations) != 1 || e.Violations[0].Field != "name" {
		t.Errorf("violations = %+v", e.Violations)
	}
}

func TestWrapUnwrap(t *testing.T) {
	cause := errors.New("root cause")
	err := Internalf("op failed").Wrap(cause)
	if !errors.Is(err, cause) {
		t.Error("errors.Is should find the cause")
	}
	if got := err.Error(); got != "op failed: root cause" {
		t.Errorf("Error() = %q", got)
	}
}

func TestAddViolationChaining(t *testing.T) {
	err := Invalidf("bad request").AddViolation("a", "x").AddViolation("b", "y")
	if len(err.Violations) != 2 {
		t.Fatalf("want 2 violations, got %d", len(err.Violations))
	}
}
