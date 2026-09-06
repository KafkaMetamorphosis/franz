// Command franz is the control-plane binary. It builds an fx.App that assembles
// config, Postgres, the realm bootstrap, and the gRPC + grpc-gateway servers.
// Entity services are added from deliverable 03 on.
package main

import (
	"context"
	"log/slog"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/in/grpcgateway"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/stub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/config"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	"github.com/KafkaMetamorphosis/franz/pkg/shared"
)

func main() {
	fx.New(
		fx.Provide(
			func() (config.Config, error) { return config.Load("config.yaml") },
			func(c config.Config) *slog.Logger { return shared.NewLogger(c.LogLevel) },
			func(c config.Config) (frn.Codec, error) { return frn.NewCodec(c.ResourcePrefix) },
			newDB,
			fx.Annotate(postgres.NewRealmRepo, fx.As(new(out.RealmRepository))),
			fx.Annotate(postgres.NewClusterRepo, fx.As(new(out.ClusterRepository))),
			fx.Annotate(func() stub.NoTopicGuard { return stub.NoTopicGuard{} },
				fx.As(new(out.ClusterTopicGuard))),
			fx.Annotate(clusters.NewService, fx.As(new(in.KafkaClusterService))),
			func(r out.RealmRepository) *grpcgateway.Authenticator {
				return grpcgateway.NewAuthenticator(r)
			},
			newServer,
		),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: log}
		}),
		// Force the FRN codec early so an invalid resource_prefix fails the boot
		// before anything else starts.
		fx.Invoke(func(frn.Codec) {}),
		fx.Invoke(registerServer),
	).Run()
}

// newDB opens the pool, runs the embedded migrations on boot when
// db.auto_migrate is set, and closes the pool on shutdown.
func newDB(lc fx.Lifecycle, c config.Config, log *slog.Logger) (*postgres.DB, error) {
	db, err := postgres.New(context.Background(), c.DB.DSN())
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if !c.DB.AutoMigrate {
				return nil
			}
			log.Info("applying migrations")
			return db.Migrate(ctx)
		},
		OnStop: func(context.Context) error {
			db.Close()
			return nil
		},
	})
	return db, nil
}

// newServer builds the inbound adapter with the realm-resolving interceptors
// installed on every path (deliverable 02.10) and registers the entity services.
func newServer(
	c config.Config, log *slog.Logger,
	auth *grpcgateway.Authenticator, codec frn.Codec,
	clusterSvc in.KafkaClusterService,
) (*grpcgateway.Server, error) {
	s := grpcgateway.New(c.GRPCPort, c.HTTPPort, log, grpcgateway.WithAuthenticator(auth))
	if err := grpcgateway.RegisterKafkaClusterService(s, clusterSvc, codec); err != nil {
		return nil, err
	}
	return s, nil
}

func registerServer(lc fx.Lifecycle, s *grpcgateway.Server, log *slog.Logger, c config.Config, codec frn.Codec) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("franz starting",
				"bootstrap_realm", c.BootstrapRealm,
				"resource_prefix", codec.Prefix())
			return s.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			log.Info("franz stopping")
			return s.Stop(ctx)
		},
	})
}
