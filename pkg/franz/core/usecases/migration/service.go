// Package migration is the shard migration application service (003.13). It
// orchestrates existing primitives rather than a parallel mechanism:
// PROVISIONING creates an ordinary new shard (the same as placement's
// newShard) on the target cluster; cutover calls
// KafkaTopicService.SetConsumption(DISABLED) on the source, which already
// re-normalises traffic_share across the channel's siblings; RETIRING deletes
// the source shard through the normal delete path. shard_migration is
// bookkeeping and the Sweep driving it, not a second state machine.
package migration

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/migration"
	placementdomain "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
)

// The two 003.13 early-completion indicators Gregor Samsa publishes
// (pkg/gregorsamsa/telemetry). Duplicated as constants here rather than
// imported — the agent module and the control plane share no Go package, the
// same reason every other Gregor Samsa indicator name is a string literal at
// both ends (see governance's whitelist for the precedent).
const (
	indicatorTopicDrained           = "kafka.topic.drained"
	indicatorTopicConsumerConnected = "kafka.topic.consumer_connected"
)

// Service implements in.MigrationService.
type Service struct {
	migrations out.ShardMigrationRepository
	topics     out.TopicRepository
	clusters   out.ClusterRepository
	channels   out.AsyncChannelRepository
	realms     out.RealmRepository
	samples    out.IndicatorSampleRepository
	// topicSvc is the driving port, not the concrete usecase — reused only for
	// SetConsumption, which already does the traffic_share re-normalisation
	// cutover needs. Optional in tests that do not exercise cutover.
	topicSvc in.KafkaTopicService
	notifier out.PartitionNotifier
	log      *slog.Logger
	now      func() time.Time
}

var _ in.MigrationService = (*Service)(nil)

// NewService wires the service to its ports.
func NewService(
	migrations out.ShardMigrationRepository,
	topics out.TopicRepository,
	clusters out.ClusterRepository,
	channels out.AsyncChannelRepository,
	realms out.RealmRepository,
	samples out.IndicatorSampleRepository,
	topicSvc in.KafkaTopicService,
	notifier out.PartitionNotifier,
	log *slog.Logger,
) *Service {
	return &Service{
		migrations: migrations, topics: topics, clusters: clusters, channels: channels,
		realms: realms, samples: samples, topicSvc: topicSvc, notifier: notifier,
		log: log, now: time.Now,
	}
}

// MigrateKafkaTopic is 003.13 OQ1's resolved operator entry point.
func (s *Service) MigrateKafkaTopic(
	ctx context.Context, topicName, targetClusterName string,
) (*migration.ShardMigration, error) {
	r := realm.MustFromContext(ctx)
	source, err := s.topics.Get(ctx, r.ID, topicName)
	if err != nil {
		return nil, err
	}
	targetCluster, err := s.clusters.Get(ctx, r.ID, targetClusterName)
	if err != nil {
		return nil, err
	}
	return s.createMigration(ctx, r, source, targetCluster, migration.ReasonOperator)
}

// MigrateCluster starts moving every live shard off sourceCluster, each to an
// independently-selected target (the same weight-desc/name-asc candidate
// ordering normal placement uses, excluding sourceCluster itself). A shard
// with no eligible target, or one already migrating, is skipped — logged, not
// failed — so one bad shard cannot abort the rest of the drain.
func (s *Service) MigrateCluster(
	ctx context.Context, sourceClusterName, reason string,
) ([]*migration.ShardMigration, error) {
	r := realm.MustFromContext(ctx)
	if reason == "" {
		reason = migration.ReasonOperator
	}
	sourceCluster, err := s.clusters.Get(ctx, r.ID, sourceClusterName)
	if err != nil {
		return nil, err
	}
	shards, err := s.topics.ListByClusters(ctx, r.ID, []uuid.UUID{sourceCluster.ID})
	if err != nil {
		return nil, err
	}
	allClusters, err := s.clusters.ListAll(ctx, r.ID)
	if err != nil {
		return nil, err
	}

	var results []*migration.ShardMigration
	for _, shard := range shards {
		if shard.State == topic.StateDeleted {
			continue
		}
		ch, err := s.channels.Get(ctx, r.ID, shard.ChannelName)
		if err != nil {
			s.log.Warn("migrate cluster: resolve channel failed", "shard", shard.Name, "err", err)
			continue
		}
		rules, err := placementdomain.ParseChannelRules(ch.Labels)
		if err != nil {
			s.log.Warn("migrate cluster: channel rules invalid", "channel", ch.Name, "err", err)
			continue
		}
		target := pickTarget(rules, allClusters, sourceCluster.ID)
		if target == nil {
			s.log.Warn("migrate cluster: no eligible target",
				"shard", shard.Name, "source_cluster", sourceClusterName)
			continue
		}
		m, err := s.createMigration(ctx, r, shard, target, reason)
		if err != nil {
			if errs.KindOf(err) == errs.AlreadyExists {
				continue // already migrating — not a failure of this call
			}
			s.log.Warn("migrate cluster: could not start migration", "shard", shard.Name, "err", err)
			continue
		}
		results = append(results, m)
	}
	return results, nil
}

// pickTarget reuses the normal placement candidate ordering (weight desc, name
// asc), excluding the cluster being drained, requesting one shard's worth.
func pickTarget(rules placementdomain.ChannelRules, all []*cluster.Cluster, exclude uuid.UUID) *cluster.Cluster {
	candidates := make([]*cluster.Cluster, 0, len(all))
	for _, c := range all {
		if c.ID != exclude {
			candidates = append(candidates, c)
		}
	}
	plan := rules.Plan(candidates, 1)
	if !plan.Placed() {
		return nil
	}
	return plan.Clusters[0]
}

// createMigration validates, creates the target shard, and starts bookkeeping.
// It is the single path every trigger (operator, drain taint, cluster-delete
// force, misplaced-shard relocation, governance) goes through.
func (s *Service) createMigration(
	ctx context.Context, r realm.Realm, source *topic.KafkaTopic, targetCluster *cluster.Cluster, reason string,
) (*migration.ShardMigration, error) {
	if source.State == topic.StateDeleted {
		return nil, errs.Preconditionf("shard %q is deleted", source.Name)
	}
	if source.KafkaClusterID == nil {
		return nil, errs.Preconditionf("shard %q is not placed on any cluster", source.Name)
	}
	if *source.KafkaClusterID == targetCluster.ID {
		return nil, errs.Invalidf("shard %q is already on cluster %q", source.Name, targetCluster.Name)
	}
	if targetCluster.State != cluster.StateActive {
		return nil, errs.Preconditionf("target cluster %q is not ACTIVE", targetCluster.Name)
	}
	sourceCluster, err := s.clusters.Get(ctx, r.ID, source.ClusterName)
	if err != nil {
		return nil, err
	}
	sourceActive, err := s.migrations.ListActiveByCluster(ctx, r.ID, sourceCluster.ID)
	if err != nil {
		return nil, err
	}
	// 003.13 "one migration per shard at a time" — checked here, ahead of the
	// concurrency limit, so a caller retrying an already-migrating shard sees
	// the specific, actionable reason rather than "the cluster is busy" (the
	// DB's partial unique index enforces this regardless; this is the
	// friendlier error on the common path).
	for _, active := range sourceActive {
		if active.SourceTopicID == source.ID {
			return nil, errs.Existsf("shard %q already has an in-flight migration", source.Name)
		}
	}
	if int32(len(sourceActive)) >= sourceCluster.MigrationConcurrencyLimit() {
		return nil, errs.Exhaustedf(
			"cluster %q has reached its migration concurrency limit (%d)",
			sourceCluster.Name, sourceCluster.MigrationConcurrencyLimit())
	}
	if err := s.checkConcurrency(ctx, r.ID, targetCluster); err != nil {
		return nil, err
	}

	ch, err := s.channels.Get(ctx, r.ID, source.ChannelName)
	if err != nil {
		return nil, err
	}
	rules, err := placementdomain.ParseChannelRules(ch.Labels)
	if err != nil {
		return nil, err
	}
	if ok, why := rules.CanHost(targetCluster); !ok {
		return nil, errs.Preconditionf(
			"target cluster %q is not eligible for channel %q: %s", targetCluster.Name, ch.Name, why)
	}

	// The target is an ordinary new shard — same shape as the source, same
	// PlaceChannelShards transaction (locks the channel + siblings FOR UPDATE)
	// every other shard-create path uses, at the next never-used index (names
	// are never freed, so len(existing) — including DELETED rows — is always
	// free).
	var target *topic.KafkaTopic
	created, err := s.topics.PlaceChannelShards(ctx, r.ID, ch.ID,
		func(existing []*topic.KafkaTopic) (out.ShardPlan, error) {
			shard, err := topic.New(r, ch.ID, ch.Name, len(existing),
				targetCluster.Configuration, source.TopicConfiguration,
				source.Partitions, source.ReplicationFactor)
			if err != nil {
				return out.ShardPlan{}, err
			}
			if err := shard.PlaceOn(targetCluster.ID, targetCluster.Name); err != nil {
				return out.ShardPlan{}, err
			}
			shard.ChannelName = ch.Name
			target = shard
			return out.ShardPlan{Create: []*topic.KafkaTopic{shard}}, nil
		})
	if err != nil {
		return nil, err
	}
	s.notify(ctx, r.ID, created)

	m, err := migration.New(r.ID, ch.ID, source.ID, target.ID,
		*source.KafkaClusterID, targetCluster.ID, reason, s.now())
	if err != nil {
		return nil, err
	}
	if err := s.migrations.Create(ctx, m); err != nil {
		return nil, err
	}
	m.AsyncChannelName, m.SourceTopicName, m.TargetTopicName = ch.Name, source.Name, target.Name
	m.SourceClusterName, m.TargetClusterName = sourceCluster.Name, targetCluster.Name
	s.log.Info("shard migration started",
		"shard", source.Name, "source_cluster", sourceCluster.Name,
		"target_cluster", targetCluster.Name, "target_shard", target.Name, "reason", reason)
	return m, nil
}

// checkConcurrency enforces 003.13 OQ6's per-cluster limit against c's current
// non-terminal migrations (as either source or target).
func (s *Service) checkConcurrency(ctx context.Context, realmID uuid.UUID, c *cluster.Cluster) error {
	active, err := s.migrations.ListActiveByCluster(ctx, realmID, c.ID)
	if err != nil {
		return err
	}
	if int32(len(active)) >= c.MigrationConcurrencyLimit() {
		return errs.Exhaustedf(
			"cluster %q has reached its migration concurrency limit (%d)",
			c.Name, c.MigrationConcurrencyLimit())
	}
	return nil
}

func (s *Service) notify(ctx context.Context, realmID uuid.UUID, shards []*topic.KafkaTopic) {
	if s.notifier == nil || len(shards) == 0 {
		return
	}
	s.notifier.ShardsChanged(ctx, realmID, shards)
}

// GetShardMigration returns one migration by id, with its read-path name
// projections filled in.
func (s *Service) GetShardMigration(ctx context.Context, id uuid.UUID) (*migration.ShardMigration, error) {
	r := realm.MustFromContext(ctx)
	m, err := s.migrations.Get(ctx, r.ID, id)
	if err != nil {
		return nil, err
	}
	s.enrich(ctx, r.ID, m)
	return m, nil
}

// ListShardMigrations returns one channel's migration history, newest first,
// with read-path name projections filled in.
func (s *Service) ListShardMigrations(
	ctx context.Context, input in.ListShardMigrationsInput,
) (in.ShardMigrationPage, error) {
	r := realm.MustFromContext(ctx)
	channelID, err := s.topics.ResolveChannelID(ctx, r.ID, input.AsyncChannel)
	if err != nil {
		return in.ShardMigrationPage{}, err
	}
	queryKey := pagetoken.QueryKey("shard-migration", input.AsyncChannel)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ShardMigrationPage{}, err
	}
	page, err := s.migrations.ListByChannel(ctx, r.ID, channelID, pagetoken.ClampSize(input.PageSize), after)
	if err != nil {
		return in.ShardMigrationPage{}, err
	}
	for _, m := range page.Migrations {
		s.enrich(ctx, r.ID, m)
	}
	return in.ShardMigrationPage{
		Migrations:    page.Migrations,
		NextPageToken: pagetoken.Encode(page.LastCursor, queryKey),
	}, nil
}

// enrich fills m's read-path name projections from the two topic rows it
// references — each already carries its own channel and cluster name joined
// (topic.KafkaTopic's own ChannelName/ClusterName convention), so this needs
// no separate channel or cluster lookup. Best-effort: a lookup failure (a
// topic hard-deleted out from under a terminal migration, in principle) leaves
// the projection blank rather than failing the read.
func (s *Service) enrich(ctx context.Context, realmID uuid.UUID, m *migration.ShardMigration) {
	if source, err := s.topics.GetByID(ctx, realmID, m.SourceTopicID); err == nil {
		m.SourceTopicName = source.Name
		m.SourceClusterName = source.ClusterName
		m.AsyncChannelName = source.ChannelName
	}
	if target, err := s.topics.GetByID(ctx, realmID, m.TargetTopicID); err == nil {
		m.TargetTopicName = target.Name
		m.TargetClusterName = target.ClusterName
		if m.AsyncChannelName == "" {
			m.AsyncChannelName = target.ChannelName
		}
	}
}

// --- the sweep: advances every non-terminal migration one step -----------

// Sweep is the periodic driver (18.1's "idempotent, resumable" state machine):
// it re-evaluates every non-terminal migration against the real world
// (target's READY state, the two 003.13 early-completion indicators, the
// drain deadline) and advances what's ready. Realm-agnostic, like the
// placement retry sweep — it runs once for the whole process, not per realm.
func (s *Service) Sweep(ctx context.Context) (int, error) {
	migrations, err := s.migrations.ListNonTerminal(ctx)
	if err != nil {
		return 0, err
	}
	realms := map[uuid.UUID]realm.Realm{}
	advanced := 0
	for _, m := range migrations {
		r, ok := realms[m.RealmID]
		if !ok {
			r, err = s.realms.GetByID(ctx, m.RealmID)
			if err != nil {
				s.log.Warn("migration sweep: resolve realm failed", "realm_id", m.RealmID, "err", err)
				continue
			}
			realms[m.RealmID] = r
		}
		if s.advance(ctx, r, m) {
			advanced++
		}
	}
	return advanced, nil
}

// advance moves m forward at most one phase. Every step is idempotent and
// re-entrant: a crash between steps resumes cleanly from m.Phase on the next
// sweep, and no phase deletes data before the next confirms (18.9's own
// "Done when").
func (s *Service) advance(ctx context.Context, r realm.Realm, m *migration.ShardMigration) bool {
	switch m.Phase {
	case migration.PhaseProvisioning:
		return s.advanceProvisioning(ctx, r, m)
	case migration.PhaseCutover:
		return s.advanceCutover(ctx, r, m)
	case migration.PhaseDraining:
		return s.advanceDraining(ctx, r, m)
	case migration.PhaseRetiring:
		return s.advanceRetiring(ctx, r, m)
	default:
		return false
	}
}

func (s *Service) advanceProvisioning(ctx context.Context, r realm.Realm, m *migration.ShardMigration) bool {
	target, err := s.topics.GetByID(ctx, r.ID, m.TargetTopicID)
	if err != nil {
		s.fail(ctx, r, m, "load target shard: "+err.Error())
		return true
	}
	if target.State == topic.StateError {
		s.fail(ctx, r, m, "target shard failed to reconcile: "+target.LastReconcileMessage)
		return true
	}
	if target.State != topic.StateReady {
		return false // still waiting on the agent
	}
	_, err = s.migrations.Mutate(ctx, r.ID, m.ID, func(m *migration.ShardMigration) error {
		return m.AdvanceToCutover()
	})
	if err != nil {
		s.log.Warn("migration sweep: advance to cutover failed", "migration", m.ID, "err", err)
		return false
	}
	s.log.Info("shard migration: target ready, cutting over", "migration", m.ID)
	return true
}

// advanceCutover applies SetConsumption(DISABLED) to the source — the actual
// cutover mechanism (traffic_share re-normalisation across the channel's
// siblings, already built for re-shard) — then advances to DRAINING.
func (s *Service) advanceCutover(ctx context.Context, r realm.Realm, m *migration.ShardMigration) bool {
	source, err := s.topics.GetByID(ctx, r.ID, m.SourceTopicID)
	if err != nil {
		s.fail(ctx, r, m, "load source shard: "+err.Error())
		return true
	}
	if s.topicSvc != nil && source.Consumption != topic.ConsumptionDisabled {
		ctx = realm.NewContext(ctx, r)
		if _, err := s.topicSvc.SetConsumption(ctx, source.Name, topic.ConsumptionDisabled); err != nil {
			s.log.Warn("migration sweep: cutover SetConsumption failed", "migration", m.ID, "err", err)
			return false
		}
	}
	_, err = s.migrations.Mutate(ctx, r.ID, m.ID, func(m *migration.ShardMigration) error {
		return m.AdvanceToDraining(s.now())
	})
	if err != nil {
		s.log.Warn("migration sweep: advance to draining failed", "migration", m.ID, "err", err)
		return false
	}
	s.log.Info("shard migration: cutover complete, draining", "migration", m.ID)
	return true
}

func (s *Service) advanceDraining(ctx context.Context, r realm.Realm, m *migration.ShardMigration) bool {
	source, err := s.topics.GetByID(ctx, r.ID, m.SourceTopicID)
	if err != nil {
		s.fail(ctx, r, m, "load source shard: "+err.Error())
		return true
	}
	drained := s.latestBool(ctx, r.ID, indicatorTopicDrained, source.FRN.Path())
	connected := s.latestBool(ctx, r.ID, indicatorTopicConsumerConnected, source.FRN.Path())
	if !m.ReadyToRetire(drained, connected, s.now()) {
		return false
	}
	_, err = s.migrations.Mutate(ctx, r.ID, m.ID, func(m *migration.ShardMigration) error {
		return m.AdvanceToRetiring()
	})
	if err != nil {
		s.log.Warn("migration sweep: advance to retiring failed", "migration", m.ID, "err", err)
		return false
	}
	s.log.Info("shard migration: drain complete, retiring source",
		"migration", m.ID, "drained", drained, "consumer_connected", connected)
	return true
}

// advanceRetiring deletes the source shard through the normal delete path
// (topic.StateDeleted via MutateChannelShards) — the same path any other
// shard delete takes, which already tells the source cluster's agent to
// remove the real topic. No new agent protocol.
func (s *Service) advanceRetiring(ctx context.Context, r realm.Realm, m *migration.ShardMigration) bool {
	shards, err := s.topics.MutateChannelShards(ctx, r.ID, m.AsyncChannelID,
		func(shards []*topic.KafkaTopic) error {
			for _, sh := range shards {
				if sh.ID == m.SourceTopicID {
					if sh.State == topic.StateDeleted {
						return nil // already retired — idempotent resume
					}
					return sh.SetState(topic.StateDeleted)
				}
			}
			return errs.Internalf("source shard %s not found among its channel's shards", m.SourceTopicID)
		})
	if err != nil {
		s.log.Warn("migration sweep: retire source failed", "migration", m.ID, "err", err)
		return false
	}
	s.notify(ctx, r.ID, shards)

	_, err = s.migrations.Mutate(ctx, r.ID, m.ID, func(m *migration.ShardMigration) error {
		return m.Complete(s.now())
	})
	if err != nil {
		s.log.Warn("migration sweep: complete failed", "migration", m.ID, "err", err)
		return false
	}
	s.log.Info("shard migration done", "migration", m.ID)
	return true
}

func (s *Service) fail(ctx context.Context, r realm.Realm, m *migration.ShardMigration, reason string) {
	_, err := s.migrations.Mutate(ctx, r.ID, m.ID, func(m *migration.ShardMigration) error {
		return m.Fail(reason, s.now())
	})
	if err != nil {
		s.log.Warn("migration sweep: mark failed did not persist", "migration", m.ID, "err", err)
		return
	}
	s.log.Warn("shard migration failed", "migration", m.ID, "reason", reason)
}

// latestBool reads the newest sample of a boolean indicator for one resource.
// IndicatorSampleRepository.LatestPerResource returns every resource's latest
// sample for the indicator, not a single-resource lookup — Gregor Samsa
// publishes this indicator for every topic it manages, not just migrating
// ones, so the realm-wide list is filtered here rather than adding a new
// single-resource query method. false (conservative) if the indicator has
// never been sampled for this resource, or on any read error.
func (s *Service) latestBool(ctx context.Context, realmID uuid.UUID, indicatorName, resourceFRN string) bool {
	if s.samples == nil {
		return false
	}
	samples, err := s.samples.LatestPerResource(ctx, realmID, indicatorName, 0)
	if err != nil {
		s.log.Warn("migration sweep: read indicator failed", "indicator", indicatorName, "err", err)
		return false
	}
	for _, sample := range samples {
		if sample.ResourceFRN == resourceFRN {
			v, err := indicator.ParseValue(indicator.UnitBoolean, "value", sample.Value)
			return err == nil && v.Num != 0
		}
	}
	return false
}
