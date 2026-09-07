// Package naming validates the `name` primary key shared by every Franz resource
// (003.1 "Resource identity and naming").
package naming

import (
	"regexp"
	"unicode/utf8"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// Pattern is the resource-name regex from 003.1.
const Pattern = `^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$`

const (
	minLen = 1
	maxLen = 200
)

var re = regexp.MustCompile(Pattern)

// Validate checks a client-supplied name. Returns an INVALID_ARGUMENT domain
// error with a "name" field violation on failure.
func Validate(name string) error {
	if n := utf8.RuneCountInString(name); n < minLen || n > maxLen {
		return errs.InvalidField("name", "must be 1–200 characters")
	}
	if !re.MatchString(name) {
		return errs.InvalidField("name",
			`must be lower-case and match `+Pattern+` (letters, digits, '.', '-', '_'; not leading/trailing punctuation)`)
	}
	return nil
}

// Valid reports whether name is well-formed.
func Valid(name string) bool { return Validate(name) == nil }

// LabelNamePattern is the `<name>` half of a reserved label key (003.1
// "Reserved labels"), which follows Kubernetes label conventions and — unlike a
// resource name — allows upper case.
const LabelNamePattern = `^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`

var labelNameRe = regexp.MustCompile(LabelNamePattern)

// ValidLabelName reports whether name is a well-formed reserved-label name. The
// ≤63-character cap is the caller's to apply.
func ValidLabelName(name string) bool { return labelNameRe.MatchString(name) }
