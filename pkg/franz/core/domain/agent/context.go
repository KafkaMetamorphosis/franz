package agent

import "context"

type ctxKey struct{}

// NewContext returns a copy of ctx carrying the authenticated agent. The agent
// auth interceptor (deliverable 05.2) calls this for ClusterProviderService
// requests.
func NewContext(ctx context.Context, a *Agent) context.Context {
	return context.WithValue(ctx, ctxKey{}, a)
}

// FromContext returns the authenticated agent attached to ctx, if any.
func FromContext(ctx context.Context) (*Agent, bool) {
	a, ok := ctx.Value(ctxKey{}).(*Agent)
	return a, ok
}

// MustFromContext returns the authenticated agent or panics — a missing agent on
// an agent-only RPC means the interceptor did not run (a wiring bug).
func MustFromContext(ctx context.Context) *Agent {
	a, ok := FromContext(ctx)
	if !ok {
		panic("agent: no authenticated agent in context (agent-auth interceptor not wired?)")
	}
	return a
}
