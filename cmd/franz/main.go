// Command franz is the control-plane binary. It builds an fx.App that assembles
// config, Postgres, the realm bootstrap, and the gRPC + grpc-gateway servers.
// Entity services are added from deliverable 03 on.
package main

import (
	"context"
	"log/slog"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/in/grpcgateway"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/postgres"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/out/stub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/config"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/agents"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/shared"
)

// providerEventRetention is the nightly-prune window for cluster_provider_event
// (004 ADR §4, matching the 003.14 telemetry default).
const providerEventRetention = 30 * 24 * time.Hour

func main() {
	fx.New(
		fx.Provide(
			func() (config.Config, error) { return config.Load("config.yaml") },
			func(c config.Config) *slog.Logger { return shared.NewLogger(c.LogLevel) },
			func(c config.Config) (frn.Codec, error) { return frn.NewCodec(c.ResourcePrefix) },
			newDB,
			streamhub.New,
			func(h *streamhub.Hub) out.AssignmentPublisher { return h },
			fx.Annotate(postgres.NewRealmRepo, fx.As(new(out.RealmRepository))),
			fx.Annotate(postgres.NewClusterRepo, fx.As(new(out.ClusterRepository))),
			fx.Annotate(postgres.NewAgentRepo, fx.As(new(out.AgentRepository))),
			fx.Annotate(postgres.NewProviderEventRepo,
				fx.As(new(out.ProviderEventRepository)),
				fx.As(new(out.ProviderStatusReader))),
			fx.Annotate(func() stub.NoTopicGuard { return stub.NoTopicGuard{} },
				fx.As(new(out.ClusterTopicGuard))),
			fx.Annotate(clusters.NewService, fx.As(new(in.KafkaClusterService))),
			fx.Annotate(agents.NewService, fx.As(new(in.AgentService))),
			fx.Annotate(provider.NewService, fx.As(new(in.ClusterProviderService))),
			func(r out.RealmRepository) *grpcgateway.Authenticator {
				return grpcgateway.NewAuthenticator(r)
			},
			func(r out.AgentRepository) *grpcgateway.AgentAuthenticator {
				return grpcgateway.NewAgentAuthenticator(r)
			},
			newServer,
		),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: log}
		}),
		// Force the FRN codec early so an invalid resource_prefix fails the boot
		// before anything else starts.
		fx.Invoke(func(frn.Codec) {}),
		fx.Invoke(startProviderEventPrune),
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
// (02.10) and the agent-auth interceptor (05.2), then registers the services.
func newServer(
	c config.Config, log *slog.Logger,
	auth *grpcgateway.Authenticator, agentAuth *grpcgateway.AgentAuthenticator,
	codec frn.Codec, hub *streamhub.Hub,
	clusterSvc in.KafkaClusterService, agentSvc in.AgentService,
	providerSvc in.ClusterProviderService,
) (*grpcgateway.Server, error) {
	s := grpcgateway.New(c.GRPCPort, c.HTTPPort, log,
		grpcgateway.WithAuthenticator(auth),
		grpcgateway.WithAgentAuth(agentAuth),
	)
	if err := grpcgateway.RegisterKafkaClusterService(s, clusterSvc, providerSvc, codec); err != nil {
		return nil, err
	}
	if err := grpcgateway.RegisterAgentService(s, agentSvc, codec); err != nil {
		return nil, err
	}
	grpcgateway.RegisterClusterProviderService(s, providerSvc, hub, codec)
	return s, nil
}

// startProviderEventPrune runs the nightly cluster_provider_event prune.
func startProviderEventPrune(lc fx.Lifecycle, log *slog.Logger, repo out.ProviderEventRepository) {
	stop := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				ticker := time.NewTicker(24 * time.Hour)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						n, err := repo.PruneOlderThan(context.Background(), time.Now().Add(-providerEventRetention))
						if err != nil {
							log.Warn("provider-event prune failed", "err", err)
							continue
						}
						if n > 0 {
							log.Info("pruned provider events", "count", n)
						}
					}
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error { close(stop); return nil },
	})
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
