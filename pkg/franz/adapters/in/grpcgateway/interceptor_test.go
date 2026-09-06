package grpcgateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

type fakeRealms struct {
	r   realm.Realm
	err error
}

func (f fakeRealms) GetBySlug(context.Context, string) (realm.Realm, error) { return f.r, f.err }

func seeded() realm.Realm {
	return realm.Realm{ID: realm.DefaultID, Slug: realm.DefaultSlug, Name: "Default realm"}
}

func TestUnaryInterceptorInjectsRealm(t *testing.T) {
	a := NewAuthenticator(fakeRealms{r: seeded()})

	var got realm.Realm
	handler := func(ctx context.Context, _ any) (any, error) {
		got = realm.MustFromContext(ctx)
		return nil, nil
	}
	if _, err := a.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if got.ID != realm.DefaultID {
		t.Errorf("realm in context = %+v", got)
	}
}

func TestUnaryInterceptorPropagatesLookupError(t *testing.T) {
	a := NewAuthenticator(fakeRealms{err: errors.New("db down")})
	_, err := a.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(context.Context, any) (any, error) { return nil, nil })
	if err == nil {
		t.Fatal("expected error")
	}
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s fakeStream) Context() context.Context { return s.ctx }

func TestStreamInterceptorInjectsRealm(t *testing.T) {
	a := NewAuthenticator(fakeRealms{r: seeded()})
	var got realm.Realm
	err := a.StreamInterceptor(nil, fakeStream{ctx: context.Background()}, &grpc.StreamServerInfo{},
		func(_ any, ss grpc.ServerStream) error {
			got = realm.MustFromContext(ss.Context())
			return nil
		})
	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if got.Slug != realm.DefaultSlug {
		t.Errorf("realm = %+v", got)
	}
}

func TestHTTPMiddlewareInjectsRealm(t *testing.T) {
	a := NewAuthenticator(fakeRealms{r: seeded()})
	var got realm.Realm
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = realm.MustFromContext(r.Context())
	})
	rec := httptest.NewRecorder()
	a.HTTPMiddleware(next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/anything", nil))
	if got.ID != realm.DefaultID {
		t.Errorf("realm = %+v", got)
	}
}

func TestHTTPMiddlewareFailsClosed(t *testing.T) {
	a := NewAuthenticator(fakeRealms{err: errors.New("db down")})
	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	rec := httptest.NewRecorder()
	a.HTTPMiddleware(next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
	if called {
		t.Error("handler ran despite realm resolution failure")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d", rec.Code)
	}
}

func TestResolveIsIdempotent(t *testing.T) {
	// a realm already in context is not re-fetched
	a := NewAuthenticator(fakeRealms{err: errors.New("should not be called")})
	pre := realm.NewContext(context.Background(), realm.Realm{ID: uuid.New(), Slug: "pre"})
	ctx, err := a.resolve(pre)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if r := realm.MustFromContext(ctx); r.Slug != "pre" {
		t.Errorf("realm was replaced: %+v", r)
	}
}
