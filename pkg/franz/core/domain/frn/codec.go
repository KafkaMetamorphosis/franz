package frn

import (
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// aliasPrefixes are accepted by Codec.Parse regardless of the configured prefix,
// so an identifier copied from another deployment — or from the pre-rename
// `orn:` era — still resolves (003.1).
var aliasPrefixes = []string{"frn", "orn"}

// Codec renders and parses FRNs against a deployment's configured prefix
// (003.1 "Prefix"). Build one at bootstrap from config `resource_prefix`; it is
// immutable.
type Codec struct {
	prefix string
}

// NewCodec validates prefix and returns a Codec. An empty prefix uses
// DefaultPrefix.
func NewCodec(prefix string) (Codec, error) {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if err := ValidatePrefix(prefix); err != nil {
		return Codec{}, err
	}
	return Codec{prefix: prefix}, nil
}

// MustCodec panics on an invalid prefix; for tests and bootstrap wiring that
// treats a bad config as fatal.
func MustCodec(prefix string) Codec {
	c, err := NewCodec(prefix)
	if err != nil {
		panic(err)
	}
	return c
}

// Prefix returns the configured prefix.
func (c Codec) Prefix() string {
	if c.prefix == "" {
		return DefaultPrefix
	}
	return c.prefix
}

// Render returns the FRN with the configured prefix — the form sent to clients.
func (c Codec) Render(f FRN) string {
	if f.IsZero() {
		return ""
	}
	return c.Prefix() + ":" + f.Path()
}

// Parse accepts the configured prefix, the `frn:` / `orn:` aliases, or a bare
// prefix-less path. A 4-segment string with an unrecognised prefix is rejected.
func (c Codec) Parse(s string) (FRN, error) {
	parts := strings.Split(s, ":")
	switch len(parts) {
	case 3:
		return New(parts[0], ResourceType(parts[1]), parts[2])
	case 4:
		if parts[0] != c.Prefix() && !isAlias(parts[0]) {
			return FRN{}, errs.InvalidField("frn",
				"unrecognised prefix "+parts[0]+" (expected "+c.Prefix()+")")
		}
		return New(parts[1], ResourceType(parts[2]), parts[3])
	default:
		return FRN{}, errs.Invalidf("malformed FRN %q", s)
	}
}

func isAlias(p string) bool {
	for _, a := range aliasPrefixes {
		if p == a {
			return true
		}
	}
	return false
}
