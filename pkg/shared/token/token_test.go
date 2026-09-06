package token

import (
	"strings"
	"testing"
)

func TestGenerate(t *testing.T) {
	p1, h1, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(p1, Prefix) {
		t.Errorf("plaintext %q missing prefix", p1)
	}
	if h1 != Hash(p1) {
		t.Errorf("hash mismatch: %q vs %q", h1, Hash(p1))
	}
	if len(h1) != 64 {
		t.Errorf("hash len = %d, want 64 hex chars", len(h1))
	}

	p2, h2, _ := Generate()
	if p1 == p2 || h1 == h2 {
		t.Error("two Generate calls produced the same token")
	}
}

func TestHashIsStableAndTrims(t *testing.T) {
	if Hash("frnat_abc") != Hash(" frnat_abc\n") {
		t.Error("Hash should trim surrounding whitespace")
	}
	if Hash("a") == Hash("b") {
		t.Error("distinct inputs hashed equal")
	}
}
