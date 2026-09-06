package selector

import "testing"

func TestParseAndMatch(t *testing.T) {
	labels := map[string]string{
		"org.com/env":             "prod",
		"org.com/country":         "br",
		"org.com/team":            "billing-core",
		"franz.affinity/selector": "x",
	}

	cases := []struct {
		sel   string
		match bool
	}{
		{"", true},   // empty matches everything
		{"  ", true}, // whitespace-only
		{"org.com/env=prod", true},
		{"org.com/env=staging", false},
		{"org.com/env!=staging", true},
		{"org.com/env!=prod", false},
		{"org.com/absent!=whatever", true}, // != on absent key -> matches
		{"org.com/env", true},              // exists
		{"org.com/absent", false},
		{"!org.com/absent", true}, // not-exists
		{"!org.com/env", false},
		{"org.com/country IN (br, mx)", true},
		{"org.com/country IN (mx, us)", false},
		{"org.com/country NOT IN (mx, us)", true},
		{"org.com/country NOT IN (br, mx)", false},
		{"org.com/absent IN (a, b)", false},
		{"org.com/absent NOT IN (a, b)", true},
		{"org.com/env=prod, org.com/country=br", true},
		{"org.com/env=prod, org.com/country=mx", false},
		{"org.com/team=billing-*", true},    // glob value
		{"org.com/team=billing-\\*", false}, // literal asterisk
		{"org.com/team IN (payments-*, billing-*)", true},
		{`org.com/env = prod`, true},               // whitespace around =
		{`org.com/country  IN  ( br , mx )`, true}, // whitespace in list
		{`org.com/env="prod"`, true},               // quoted value
		{"franz.affinity/selector=x", true},        // key with '/'
	}

	for _, c := range cases {
		s, err := Parse(c.sel)
		if err != nil {
			t.Errorf("Parse(%q) error: %v", c.sel, err)
			continue
		}
		if got := s.Match(labels); got != c.match {
			t.Errorf("Parse(%q).Match = %v, want %v", c.sel, got, c.match)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, bad := range []string{
		"key=",
		"=value",
		"key IN ()",
		"key IN (a, b",
		"key NOT (a)",
		"key ==",
		"key*=v", // wildcard in key
		"a=1,,b=2",
		"a=1,",
		"key !x",
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) expected error, got nil", bad)
		}
	}
}

func TestString(t *testing.T) {
	s := MustParse("b=2, a IN (x, y), !c")
	// String() sorts by key
	want := "a IN (x, y), b=2, !c"
	if got := s.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	// round-trips
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("re-parse: %v", err)
	}
}
