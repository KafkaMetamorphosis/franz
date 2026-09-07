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
	"github.com/KafkaMetamorphosis/franz/pkg/franz/adapters/streamhub"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/config"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/agents"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/channels"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/clusters"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/resourceprovider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/telemetry"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/usecases/topics"
	"github.com/KafkaMetamorphosis/franz/pkg/shared"
)

const (
	// providerEventRetention is the nightly-prune window for
	// cluster_provider_event (004 ADR §4, matching the 003.14 telemetry default).
	providerEventRetention = 30 * 24 * time.Hour
	// indicatorSampleRetention is the 003.14 append-only sample window. Same
	// nightly job, same 30 days.
	indicatorSampleRetention = 30 * 24 * time.Hour
)

func main() {
	fx.New(
		fx.Provide(
			func() (config.Config, error) { return config.Load("config.yaml") },
			func(c config.Config) *slog.Logger { return shared.NewLogger(c.LogLevel) },
			func(c config.Config) (frn.Codec, error) { return frn.NewCodec(c.ResourcePrefix) },
			newDB,
			streamhub.New,
			func(h *streamhub.Hub) out.AssignmentPublisher { return h },
			func(h *streamhub.Hub) out.PartitionAssignmentPublisher { return h },
			fx.Annotate(postgres.NewRealmRepo, fx.As(new(out.RealmRepository))),
			fx.Annotate(postgres.NewClusterRepo, fx.As(new(out.ClusterRepository))),
			fx.Annotate(postgres.NewAgentRepo, fx.As(new(out.AgentRepository))),
			fx.Annotate(postgres.NewProviderEventRepo,
				fx.As(new(out.ProviderEventRepository)),
				fx.As(new(out.ProviderStatusReader))),
			fx.Annotate(postgres.NewTopicRepo,
				fx.As(new(out.TopicRepository)),
				fx.As(new(out.ClusterTopicGuard))),
			fx.Annotate(postgres.NewChannelRepo, fx.As(new(out.AsyncChannelRepository))),
			fx.Annotate(postgres.NewIndicatorSampleRepo, fx.As(new(out.IndicatorSampleRepository))),
			fx.Annotate(resourceprovider.NewNotifier, fx.As(new(out.PartitionNotifier))),
			// Provided concretely as well as behind the port: the entity services
			// take out.ShardPlacer, while the retry sweep drives Sweep directly.
			placement.NewService,
			func(p *placement.Service) out.ShardPlacer { return p },
			fx.Annotate(clusters.NewService, fx.As(new(in.KafkaClusterService))),
			fx.Annotate(agents.NewService, fx.As(new(in.AgentService))),
			fx.Annotate(provider.NewService, fx.As(new(in.ClusterProviderService))),
			fx.Annotate(topics.NewService, fx.As(new(in.KafkaTopicService))),
			fx.Annotate(channels.NewService, fx.As(new(in.AsyncChannelService))),
			fx.Annotate(resourceprovider.NewService, fx.As(new(in.ResourceProviderService))),
			fx.Annotate(telemetry.NewService, fx.As(new(in.TelemetryIngestService))),
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
		fx.Invoke(startIndicatorSamplePrune),
		fx.Invoke(startPlacementSweep),
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
	providerSvc in.ClusterProviderService, topicSvc in.KafkaTopicService,
	channelSvc in.AsyncChannelService, resourceSvc in.ResourceProviderService,
	telemetrySvc in.TelemetryIngestService,
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
	if err := grpcgateway.RegisterKafkaTopicService(s, topicSvc, codec); err != nil {
		return nil, err
	}
	if err := grpcgateway.RegisterAsyncChannelService(s, channelSvc, codec); err != nil {
		return nil, err
	}
	grpcgateway.RegisterClusterProviderService(s, providerSvc, hub, codec)
	grpcgateway.RegisterResourceProviderService(s, resourceSvc, hub, codec)
	grpcgateway.RegisterTelemetryService(s, telemetrySvc)
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

// startIndicatorSamplePrune runs the nightly indicator_sample prune (003.14).
func startIndicatorSamplePrune(lc fx.Lifecycle, log *slog.Logger, repo out.IndicatorSampleRepository) {
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
						n, err := repo.PruneOlderThan(context.Background(), time.Now().Add(-indicatorSampleRetention))
						if err != nil {
							log.Warn("indicator-sample prune failed", "err", err)
							continue
						}
						if n > 0 {
							log.Info("pruned indicator samples", "count", n)
						}
					}
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error { close(stop); return nil },
	})
}

// startPlacementSweep runs the placement retry sweep (003.7): every interval it
// re-runs selection for each ACTIVE channel whose live async-channel shard rows
// are fewer than its declared `channel_partitions`, so a channel materialises
// its shards as soon as a cluster is registered, resumed, or re-labelled into
// eligibility. A non-positive interval disables it.
func startPlacementSweep(
	lc fx.Lifecycle, log *slog.Logger, c config.Config, placer *placement.Service,
) {
	interval := c.Placement.SweepInterval
	if interval <= 0 {
		log.Info("placement sweep disabled", "sweep_interval", interval)
		return
	}
	stop := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			log.Info("placement sweep started", "sweep_interval", interval)
			go func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						n, err := placer.Sweep(context.Background())
						if err != nil {
							log.Warn("placement sweep failed", "err", err)
							continue
						}
						if n > 0 {
							log.Info("placement sweep materialised async-channel shards", "count", n)
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
