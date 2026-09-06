// Package realm holds the Realm value object — the tenant / FRN scope every
// Franz resource lives under (003.1) — and the request-context plumbing that
// carries the caller's realm from the inbound adapter down to the repositories.
//
// Realm provisioning is out of scope (003.1). While API auth (003.2) is still a
// stub, one seeded realm (DefaultSlug / DefaultID) backs every request.
package realm

import (
	"context"

	"github.com/google/uuid"
)

// DefaultSlug / DefaultID identify the single seeded realm (migrations/V1__init.sql,
// ADR-API-005 D3).
const DefaultSlug = "default"

// DefaultID is the fixed UUID of the seeded default realm.
var DefaultID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

// Realm is a tenant boundary.
type Realm struct {
	ID   uuid.UUID
	Slug string
	Name string
}

// IsZero reports whether r is the zero value.
func (r Realm) IsZero() bool { return r == Realm{} }

type ctxKey struct{}

// NewContext returns a copy of ctx carrying r. The inbound adapter's auth
// interceptor calls this once per request (deliverable 02.10).
func NewContext(ctx context.Context, r Realm) context.Context {
	return context.WithValue(ctx, ctxKey{}, r)
}

// FromContext returns the realm attached to ctx, if any.
func FromContext(ctx context.Context) (Realm, bool) {
	r, ok := ctx.Value(ctxKey{}).(Realm)
	return r, ok
}

// MustFromContext returns the realm attached to ctx or panics. A missing realm
// means the auth interceptor did not run — a wiring bug, not a client error.
func MustFromContext(ctx context.Context) Realm {
	r, ok := FromContext(ctx)
	if !ok {
		panic("realm: no realm in context (auth interceptor not wired?)")
	}
	return r
}
