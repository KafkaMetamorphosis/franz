package pagetoken

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

func TestClampSize(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, DefaultSize},
		{-1, DefaultSize},
		{1, 1},
		{50, 50},
		{1000, 1000},
		{1001, MaxSize},
		{1_000_000, MaxSize},
	}
	for _, c := range cases {
		if got := ClampSize(int32(c.in)); got != c.want {
			t.Errorf("ClampSize(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	qk := QueryKey("kafka-cluster", "env=prod")
	tok := Encode("cluster-042", qk)
	if tok == "" {
		t.Fatal("expected a non-empty token")
	}
	after, err := Decode(tok, qk)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if after != "cluster-042" {
		t.Errorf("after = %q, want cluster-042", after)
	}
}

func TestEmptyTokenIsFirstPage(t *testing.T) {
	after, err := Decode("", QueryKey("x"))
	if err != nil || after != "" {
		t.Errorf("Decode(\"\") = %q, %v", after, err)
	}
	if Encode("", QueryKey("x")) != "" {
		t.Error("Encode with empty lastName should be empty")
	}
}

func TestDecodeRejectsWrongQuery(t *testing.T) {
	tok := Encode("a", QueryKey("kafka-cluster", "env=prod"))
	_, err := Decode(tok, QueryKey("kafka-cluster", "env=staging"))
	if err == nil {
		t.Fatal("expected rejection for mismatched query")
	}
	if e, ok := errs.As(err); !ok || e.Kind != errs.InvalidArgument || e.Violations[0].Field != "page_token" {
		t.Errorf("wrong error: %v", err)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, bad := range []string{"!!!!", "not-base64!!", "YWJj"} { // last is valid b64 "abc" but not json
		if _, err := Decode(bad, QueryKey("x")); err == nil {
			t.Errorf("Decode(%q) = nil error", bad)
		}
	}
}

func TestQueryKeyStability(t *testing.T) {
	if QueryKey("a", "b") != QueryKey("a", "b") {
		t.Error("QueryKey not stable")
	}
	if QueryKey("a", "bc") == QueryKey("ab", "c") {
		t.Error("QueryKey collision on concatenation")
	}
}
