// Package glob implements the `*` wildcard matching used by Franz label-selector
// values and access-policy FRN principals (003.1 "Wildcards").
//
//   - `*` matches any run of zero or more characters (not a regex).
//   - `\*` is a literal asterisk.
//   - a lone `*` means "match everything".
//   - `?` and character classes are NOT supported.
package glob

import "strings"

// HasWildcard reports whether pattern contains an unescaped `*`.
func HasWildcard(pattern string) bool {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			i++ // skip the escaped char
		case '*':
			return true
		}
	}
	return false
}

// Unescape turns `\*` into `*`. Use on a pattern with no unescaped wildcards to
// get the literal string it represents.
func Unescape(pattern string) string {
	if !strings.Contains(pattern, `\`) {
		return pattern
	}
	var b strings.Builder
	b.Grow(len(pattern))
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '\\' && i+1 < len(pattern) {
			i++
		}
		b.WriteByte(pattern[i])
	}
	return b.String()
}

type token struct {
	star bool
	ch   byte
}

func lex(pattern string) []token {
	out := make([]token, 0, len(pattern))
	for i := 0; i < len(pattern); i++ {
		switch {
		case pattern[i] == '\\' && i+1 < len(pattern):
			i++
			out = append(out, token{ch: pattern[i]})
		case pattern[i] == '*':
			out = append(out, token{star: true})
		default:
			out = append(out, token{ch: pattern[i]})
		}
	}
	return out
}

// Match reports whether s matches pattern. Linear-time greedy match with a
// single backtrack point (the last `*` seen).
func Match(pattern, s string) bool {
	p := lex(pattern)

	var px, sx int
	starPx, starSx := -1, 0

	for sx < len(s) {
		switch {
		case px < len(p) && p[px].star:
			starPx, starSx = px, sx
			px++
		case px < len(p) && !p[px].star && p[px].ch == s[sx]:
			px++
			sx++
		case starPx >= 0:
			px = starPx + 1
			starSx++
			sx = starSx
		default:
			return false
		}
	}

	for px < len(p) && p[px].star {
		px++
	}
	return px == len(p)
}
