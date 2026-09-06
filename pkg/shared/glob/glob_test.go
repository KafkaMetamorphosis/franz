package glob

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"", "", true},
		{"", "x", false},
		{"*", "", true},
		{"*", "anything at all", true},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"billing-*", "billing-events", true},
		{"billing-*", "billing-", true},
		{"billing-*", "payments", false},
		{"*-prod", "eu-prod", true},
		{"*-prod", "eu-prod-1", false},
		{"a*c", "ac", true},
		{"a*c", "abbbc", true},
		{"a*c", "abbb", false},
		{"frn:acme:client:xpto-*", "frn:acme:client:xpto-1", true},
		{"frn:acme:client:xpto-*", "frn:acme:client:other", false},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "abc", true},
		{"a*b*c", "acb", false},
		{`a\*b`, "a*b", true},
		{`a\*b`, "axb", false},
		{`\*`, "*", true},
		{`\*`, "x", false},
		{`pre\*-*`, "pre*-anything", true},
	}
	for _, c := range cases {
		if got := Match(c.pattern, c.s); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestHasWildcard(t *testing.T) {
	for _, c := range []struct {
		in   string
		want bool
	}{
		{"plain", false},
		{"has*star", true},
		{`escaped\*only`, false},
		{`escaped\*and*real`, true},
	} {
		if got := HasWildcard(c.in); got != c.want {
			t.Errorf("HasWildcard(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestUnescape(t *testing.T) {
	if got := Unescape(`a\*b`); got != "a*b" {
		t.Errorf("Unescape = %q", got)
	}
	if got := Unescape("plain"); got != "plain" {
		t.Errorf("Unescape = %q", got)
	}
}
