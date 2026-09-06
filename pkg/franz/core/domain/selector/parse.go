package selector

import "strings"

// Parse parses a selector expression. An empty (or whitespace-only) string
// yields the everything-matching selector.
func Parse(s string) (Selector, error) {
	p := &parser{in: s}
	p.skipSpace()
	if p.eof() {
		return Selector{}, nil
	}

	var reqs []Requirement
	for {
		r, err := p.requirement()
		if err != nil {
			return Selector{}, err
		}
		reqs = append(reqs, r)

		p.skipSpace()
		if p.eof() {
			break
		}
		if p.peek() != ',' {
			return Selector{}, invalid("expected ',' at %q", p.rest())
		}
		p.next() // consume ','
		p.skipSpace()
		if p.eof() {
			return Selector{}, invalid("trailing ','")
		}
	}
	return Selector{reqs: reqs}, nil
}

// MustParse panics on error; for tests and constants.
func MustParse(s string) Selector {
	sel, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return sel
}

type parser struct {
	in  string
	pos int
}

func (p *parser) eof() bool    { return p.pos >= len(p.in) }
func (p *parser) peek() byte   { return p.in[p.pos] }
func (p *parser) next() byte   { b := p.in[p.pos]; p.pos++; return b }
func (p *parser) rest() string { return p.in[p.pos:] }

func (p *parser) skipSpace() {
	for !p.eof() && (p.in[p.pos] == ' ' || p.in[p.pos] == '\t') {
		p.pos++
	}
}

// requirement parses one clause.
func (p *parser) requirement() (Requirement, error) {
	p.skipSpace()
	if p.eof() {
		return Requirement{}, invalid("empty requirement")
	}

	// !key
	if p.peek() == '!' {
		p.next()
		key, err := p.key()
		if err != nil {
			return Requirement{}, err
		}
		return Requirement{Key: key, Op: OpNotExists}, nil
	}

	key, err := p.key()
	if err != nil {
		return Requirement{}, err
	}
	p.skipSpace()

	if p.eof() || p.peek() == ',' {
		return Requirement{Key: key, Op: OpExists}, nil
	}

	// != or =
	if p.peek() == '!' {
		p.next()
		if p.eof() || p.peek() != '=' {
			return Requirement{}, invalid("expected '=' after '!' for key %q", key)
		}
		p.next()
		v, err := p.value()
		if err != nil {
			return Requirement{}, err
		}
		return Requirement{Key: key, Op: OpNotEquals, Values: []string{v}}, nil
	}
	if p.peek() == '=' {
		p.next()
		v, err := p.value()
		if err != nil {
			return Requirement{}, err
		}
		return Requirement{Key: key, Op: OpEquals, Values: []string{v}}, nil
	}

	// IN ( ... ) or NOT IN ( ... )
	kw := p.keyword()
	switch strings.ToUpper(kw) {
	case "IN":
		vs, err := p.valueList()
		if err != nil {
			return Requirement{}, err
		}
		return Requirement{Key: key, Op: OpIn, Values: vs}, nil
	case "NOT":
		p.skipSpace()
		if strings.ToUpper(p.keyword()) != "IN" {
			return Requirement{}, invalid("expected 'IN' after 'NOT' for key %q", key)
		}
		vs, err := p.valueList()
		if err != nil {
			return Requirement{}, err
		}
		return Requirement{Key: key, Op: OpNotIn, Values: vs}, nil
	default:
		return Requirement{}, invalid("expected '=', '!=', 'IN' or 'NOT IN' after key %q, got %q", key, kw)
	}
}

// key reads a label key: letters, digits, '.', '-', '_', '/'. No wildcards.
func (p *parser) key() (string, error) {
	start := p.pos
	for !p.eof() {
		b := p.in[p.pos]
		if isKeyByte(b) {
			p.pos++
			continue
		}
		break
	}
	if p.pos == start {
		return "", invalid("expected a label key at %q", p.rest())
	}
	k := p.in[start:p.pos]
	if strings.Contains(k, "*") {
		return "", invalid("wildcards are not allowed in label keys (%q)", k)
	}
	return k, nil
}

// keyword reads a bare alpha word (IN / NOT).
func (p *parser) keyword() string {
	p.skipSpace()
	start := p.pos
	for !p.eof() {
		b := p.in[p.pos]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') {
			p.pos++
			continue
		}
		break
	}
	return p.in[start:p.pos]
}

// value reads a single value (bare token or quoted string).
func (p *parser) value() (string, error) {
	p.skipSpace()
	if p.eof() {
		return "", invalid("expected a value")
	}
	if p.peek() == '"' {
		return p.quoted()
	}
	start := p.pos
	for !p.eof() {
		b := p.in[p.pos]
		if b == ',' || b == ')' || b == '(' || b == ' ' || b == '\t' || b == '=' || b == '"' {
			break
		}
		p.pos++
	}
	if p.pos == start {
		return "", invalid("expected a value at %q", p.rest())
	}
	return p.in[start:p.pos], nil
}

func (p *parser) quoted() (string, error) {
	p.next() // opening quote
	var b strings.Builder
	for !p.eof() {
		c := p.next()
		if c == '"' {
			return b.String(), nil
		}
		if c == '\\' && !p.eof() {
			b.WriteByte(p.next())
			continue
		}
		b.WriteByte(c)
	}
	return "", invalid("unterminated quoted value")
}

// valueList reads `( v, v, v )`.
func (p *parser) valueList() ([]string, error) {
	p.skipSpace()
	if p.eof() || p.peek() != '(' {
		return nil, invalid("expected '(' at %q", p.rest())
	}
	p.next()

	var vs []string
	for {
		p.skipSpace()
		if p.eof() {
			return nil, invalid("unterminated value list")
		}
		if p.peek() == ')' {
			p.next()
			if len(vs) == 0 {
				return nil, invalid("empty value list")
			}
			return vs, nil
		}
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		vs = append(vs, v)
		p.skipSpace()
		if p.eof() {
			return nil, invalid("unterminated value list")
		}
		switch p.peek() {
		case ',':
			p.next()
		case ')':
			p.next()
			return vs, nil
		default:
			return nil, invalid("expected ',' or ')' in value list at %q", p.rest())
		}
	}
}

func isKeyByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	case b == '.' || b == '-' || b == '_' || b == '/':
		return true
	case b == '*':
		return true // caught and rejected in key()
	default:
		return false
	}
}

func quote(v string) string {
	if strings.ContainsAny(v, " ,()\"\t") {
		return `"` + strings.ReplaceAll(v, `"`, `\"`) + `"`
	}
	return v
}

func joinQuoted(vs []string) string {
	q := make([]string, len(vs))
	for i, v := range vs {
		q[i] = quote(v)
	}
	return strings.Join(q, ", ")
}
