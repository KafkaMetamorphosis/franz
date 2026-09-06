// Package frn is the FRN (Franz Resource Name) value object — the opaque,
// server-assigned, immutable identifier every Franz resource has
// (003.1 "FRN"):
//
//	<prefix>:<realm>:<resource-type>:<name>
//
// The value object itself carries only (realm, type, name). The prefix is a
// rendering concern: Franz stores FRNs prefix-less (see FRN.Path) and applies
// the deployment's configured prefix at the API boundary via a Codec. The bare
// String / Parse helpers use DefaultPrefix and are meant for logs, errors, and
// text (un)marshaling.
package frn

import (
	"regexp"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/naming"
)

// DefaultPrefix is the prefix used when none is configured.
const DefaultPrefix = "frn"

// PrefixPattern constrains a configured resource prefix (003.1).
const PrefixPattern = `^[a-z][a-z0-9]*$`

const (
	prefixMinLen = 2
	prefixMaxLen = 16
)

var prefixRe = regexp.MustCompile(PrefixPattern)

// ValidatePrefix reports whether p is a legal resource prefix.
func ValidatePrefix(p string) error {
	if n := len(p); n < prefixMinLen || n > prefixMaxLen {
		return errs.Invalidf("resource_prefix %q must be 2–16 characters", p)
	}
	if !prefixRe.MatchString(p) {
		return errs.Invalidf("resource_prefix %q must match %s", p, PrefixPattern)
	}
	return nil
}

// ResourceType is the kebab-case type segment of an FRN.
type ResourceType string

const (
	TypeAsyncChannel ResourceType = "async-channel"
	TypeKafkaCluster ResourceType = "kafka-cluster"
	TypeKafkaTopic   ResourceType = "kafka-topic"
	TypeAgent        ResourceType = "agent"
	TypeClient       ResourceType = "client"
	TypePolicy       ResourceType = "policy"
	TypeIndicator    ResourceType = "indicator"
	TypeRealm        ResourceType = "realm"
)

var knownTypes = map[ResourceType]bool{
	TypeAsyncChannel: true, TypeKafkaCluster: true, TypeKafkaTopic: true,
	TypeAgent: true, TypeClient: true, TypePolicy: true,
	TypeIndicator: true, TypeRealm: true,
}

// Valid reports whether t is a known resource type.
func (t ResourceType) Valid() bool { return knownTypes[t] }

// FRN is an immutable identifier. The zero value is invalid.
type FRN struct {
	realm string
	typ   ResourceType
	name  string
}

// New builds an FRN, validating the realm slug, type, and name.
func New(realm string, typ ResourceType, name string) (FRN, error) {
	if err := naming.Validate(realm); err != nil {
		return FRN{}, errs.InvalidField("realm", "invalid realm slug")
	}
	if !typ.Valid() {
		return FRN{}, errs.Invalidf("unknown resource type %q", typ)
	}
	if err := naming.Validate(name); err != nil {
		return FRN{}, err
	}
	return FRN{realm: realm, typ: typ, name: name}, nil
}

func (f FRN) Realm() string      { return f.realm }
func (f FRN) Type() ResourceType { return f.typ }
func (f FRN) Name() string       { return f.name }
func (f FRN) IsZero() bool       { return f == FRN{} }

// Path is the prefix-less canonical form `<realm>:<resource-type>:<name>` — the
// value Franz persists (003.12).
func (f FRN) Path() string {
	if f.IsZero() {
		return ""
	}
	return f.realm + ":" + string(f.typ) + ":" + f.name
}

// String renders the FRN with DefaultPrefix. Use a Codec to render with the
// deployment's configured prefix.
func (f FRN) String() string {
	if f.IsZero() {
		return ""
	}
	return DefaultPrefix + ":" + f.Path()
}

// MarshalText / UnmarshalText let FRN round-trip through JSON and text encoders.
func (f FRN) MarshalText() ([]byte, error) { return []byte(f.String()), nil }

func (f *FRN) UnmarshalText(b []byte) error {
	parsed, err := Parse(string(b))
	if err != nil {
		return err
	}
	*f = parsed
	return nil
}

// ParsePath parses a prefix-less `<realm>:<resource-type>:<name>`.
func ParsePath(s string) (FRN, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return FRN{}, errs.Invalidf("malformed FRN path %q", s)
	}
	return New(parts[0], ResourceType(parts[1]), parts[2])
}

// Parse parses an FRN in either form: with a leading `<prefix>:` (any prefix is
// accepted here — the value object does not police it; a Codec does) or
// prefix-less. Names, realms, and types never contain `:`, so a 4-segment string
// is unambiguously prefixed and a 3-segment string is a bare path.
func Parse(s string) (FRN, error) {
	switch parts := strings.Split(s, ":"); len(parts) {
	case 3:
		return New(parts[0], ResourceType(parts[1]), parts[2])
	case 4:
		return New(parts[1], ResourceType(parts[2]), parts[3])
	default:
		return FRN{}, errs.Invalidf("malformed FRN %q", s)
	}
}

// MustParse panics on error; for tests and constants.
func MustParse(s string) FRN {
	f, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return f
}
