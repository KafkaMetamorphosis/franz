package grpcgateway

import (
	"context"
	"net/http"

	"google.golang.org/grpc"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// RealmLookup is the slice of core/ports/out.RealmRepository the authenticator
// needs.
type RealmLookup interface {
	GetBySlug(ctx context.Context, slug string) (realm.Realm, error)
}

// Authenticator is the 003.2 seam. Today it authenticates nobody and resolves
// every request to the single seeded realm (ADR-API-005 D3); when API auth lands
// it will read the caller's identity and pick the realm from it. Every inbound
// path (gRPC unary, gRPC stream, gateway HTTP) runs through it, so downstream
// code can always call realm.MustFromContext.
type Authenticator struct {
	realms RealmLookup
}

// NewAuthenticator wires the authenticator to the realm repository.
func NewAuthenticator(realms RealmLookup) *Authenticator {
	return &Authenticator{realms: realms}
}

func (a *Authenticator) resolve(ctx context.Context) (context.Context, error) {
	if _, ok := realm.FromContext(ctx); ok {
		return ctx, nil
	}
	r, err := a.realms.GetBySlug(ctx, realm.DefaultSlug)
	if err != nil {
		return ctx, err
	}
	return realm.NewContext(ctx, r), nil
}

// UnaryInterceptor resolves the realm before the handler runs.
func (a *Authenticator) UnaryInterceptor(
	ctx context.Context, req any,
	_ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	ctx, err := a.resolve(ctx)
	if err != nil {
		return nil, ToError(err)
	}
	return handler(ctx, req)
}

// StreamInterceptor resolves the realm and hands the handler a stream whose
// Context carries it.
func (a *Authenticator) StreamInterceptor(
	srv any, ss grpc.ServerStream,
	_ *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx, err := a.resolve(ss.Context())
	if err != nil {
		return ToError(err)
	}
	return handler(srv, &realmStream{ServerStream: ss, ctx: ctx})
}

type realmStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *realmStream) Context() context.Context { return s.ctx }

// HTTPMiddleware resolves the realm for gateway (REST/JSON) requests, which reach
// the gRPC services in-process and so never hit the gRPC interceptors.
func (a *Authenticator) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := a.resolve(r.Context())
		if err != nil {
			http.Error(w, `{"error":"realm resolution failed"}`, http.StatusInternalServerError)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
