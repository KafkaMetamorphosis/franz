// Command franz is the control-plane binary. It builds an fx.App that assembles
// config, the gRPC + grpc-gateway servers, and (from deliverable 02 on) the
// usecases and adapters.
package main

import (
	"context"
	"log/slog"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/in/grpcgateway"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/config"
	"github.com/KafkaMetamorphosis/franz/pkg/shared"
)

func main() {
	fx.New(
		fx.Provide(
			func() (config.Config, error) { return config.Load("config.yaml") },
			func(c config.Config) *slog.Logger { return shared.NewLogger(c.LogLevel) },
			func(c config.Config, log *slog.Logger) *grpcgateway.Server {
				return grpcgateway.New(c.GRPCPort, c.HTTPPort, log)
			},
		),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: log}
		}),
		fx.Invoke(registerServer),
	).Run()
}

func registerServer(lc fx.Lifecycle, s *grpcgateway.Server, log *slog.Logger, c config.Config) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("franz starting", "bootstrap_realm", c.BootstrapRealm)
			return s.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			log.Info("franz stopping")
			return s.Stop(ctx)
		},
	})
}
