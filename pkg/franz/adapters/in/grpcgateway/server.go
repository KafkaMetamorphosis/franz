// Package grpcgateway is the driving adapter: it runs the gRPC server and the
// grpc-gateway REST/JSON mux, and maps inbound requests onto core/ports/in.
//
// Deliverable 01 wires only the servers and a /healthz probe — no services yet.
package grpcgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
)

// Server owns the gRPC listener and the HTTP server that fronts the gateway.
type Server struct {
	grpcPort int
	httpPort int
	log      *slog.Logger

	grpc *grpc.Server
	gw   *runtime.ServeMux
	http *http.Server
	lis  net.Listener
}

// New constructs the server. Register gRPC services on Grpc() and gateway
// handlers on Gateway() before calling Start.
func New(grpcPort, httpPort int, log *slog.Logger) *Server {
	gs := grpc.NewServer()
	gw := runtime.NewServeMux()

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	root.Handle("/", gw)

	return &Server{
		grpcPort: grpcPort,
		httpPort: httpPort,
		log:      log,
		grpc:     gs,
		gw:       gw,
		http:     &http.Server{Addr: fmt.Sprintf(":%d", httpPort), Handler: root},
	}
}

// Grpc returns the gRPC server for service registration.
func (s *Server) Grpc() *grpc.Server { return s.grpc }

// Gateway returns the grpc-gateway mux for handler registration.
func (s *Server) Gateway() *runtime.ServeMux { return s.gw }

// Start binds the listeners and serves in the background.
func (s *Server) Start(_ context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.grpcPort))
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	s.lis = lis

	go func() {
		if err := s.grpc.Serve(lis); err != nil {
			s.log.Error("grpc server stopped", "err", err)
		}
	}()
	go func() {
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server stopped", "err", err)
		}
	}()

	s.log.Info("listening", "grpc_port", s.grpcPort, "http_port", s.httpPort)
	return nil
}

// Stop gracefully drains both servers.
func (s *Server) Stop(ctx context.Context) error {
	s.grpc.GracefulStop()
	return s.http.Shutdown(ctx)
}
