package grpcgateway

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/agent"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/token"
)

// clusterProviderMethodPrefix scopes the agent-auth interceptor to the
// agent-only ClusterProviderService; every other RPC passes through untouched
// (the console allow-all realm interceptor handles those).
const clusterProviderMethodPrefix = "/franz.v1.ClusterProviderService/"

// AgentAuthenticator resolves an `authorization: Bearer <token>` gRPC metadata
// header to the registered agent and puts it in the request context
// (004 ADR §2). Unknown, rotated, or deleted-agent tokens are UNAUTHENTICATED.
type AgentAuthenticator struct {
	agents out.AgentRepository
}

// NewAgentAuthenticator wires the interceptor to the agent repository.
func NewAgentAuthenticator(agents out.AgentRepository) *AgentAuthenticator {
	return &AgentAuthenticator{agents: agents}
}

func (a *AgentAuthenticator) authenticate(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	var raw string
	for _, v := range md.Get("authorization") {
		if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
			raw = strings.TrimSpace(v[7:])
			break
		}
	}
	if raw == "" {
		return ctx, status.Error(codes.Unauthenticated, "missing or malformed bearer token")
	}

	ag, err := a.agents.GetByTokenHash(ctx, token.Hash(raw))
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, "invalid agent token")
	}
	if ag.Status == agent.StatusDeleted {
		return ctx, status.Error(codes.Unauthenticated, "agent is deleted")
	}
	return agent.NewContext(ctx, ag), nil
}

// UnaryInterceptor authenticates ClusterProviderService unary calls.
func (a *AgentAuthenticator) UnaryInterceptor(
	ctx context.Context, req any,
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	if !strings.HasPrefix(info.FullMethod, clusterProviderMethodPrefix) {
		return handler(ctx, req)
	}
	ctx, err := a.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor authenticates the WatchClusterAssignments stream.
func (a *AgentAuthenticator) StreamInterceptor(
	srv any, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	if !strings.HasPrefix(info.FullMethod, clusterProviderMethodPrefix) {
		return handler(srv, ss)
	}
	ctx, err := a.authenticate(ss.Context())
	if err != nil {
		return err
	}
	return handler(srv, &realmStream{ServerStream: ss, ctx: ctx})
}

// WithAgentAuth installs the agent-auth interceptors (004 ADR §2). They run
// after the realm interceptor and only act on ClusterProviderService methods.
func WithAgentAuth(a *AgentAuthenticator) Option {
	return func(o *options) {
		o.unary = append(o.unary, a.UnaryInterceptor)
		o.stream = append(o.stream, a.StreamInterceptor)
	}
}
