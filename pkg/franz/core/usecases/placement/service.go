// Package placement is the placement application service (003.7): it runs the
// domain selection algorithm for an Async Channel and materialises the result as
// `kafka_topic` rows.
//
// It is the sole producer of async-channel shard rows (ADR-API-009):
// CreateAsyncChannel writes only the channel row, and a shard row exists only
// once placement has a concrete cluster for it. A channel with no eligible
// cluster simply has no shard rows — there is no placeholder row with a NULL
// cluster.
//
// Every entry point is best-effort by design. Channel create always succeeds
// (003.7), so a placement failure is logged and left to the retry sweep rather
// than failing the caller's write.
package placement

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	placementdomain "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// Fallbacks for a chosen cluster that declares neither seed key in its
// `cluster_configuration`. A shard row must carry a real Kafka shape (003.6), so
// there is no "unset" to persist.
const (
	FallbackPartitions        int32 = 1
	FallbackReplicationFactor int32 = 1
)

// Service implements out.ShardPlacer.
type Service struct {
	channels out.AsyncChannelRepository
	clusters out.ClusterRepository
	topics   out.TopicRepository
	realms   out.RealmRepository
	// notifier pushes the resulting partition-assignment deltas to the in-scope
	// Resource Provider agents (005 ADR §1.3). Optional — nil in tests that do
	// not exercise the agent wire.
	notifier out.PartitionNotifier
	log      *slog.Logger
}

var _ out.ShardPlacer = (*Service)(nil)

// NewService wires the placement service to its ports.
func NewService(
	channels out.AsyncChannelRepository,
	clusters out.ClusterRepository,
	topics out.TopicRepository,
	realms out.RealmRepository,
	notifier out.PartitionNotifier,
	log *slog.Logger,
) *Service {
	return &Service{
		channels: channels, clusters: clusters, topics: topics,
		realms: realms, notifier: notifier, log: log,
	}
}

// PlaceChannel runs one placement pass for a single channel — the
// channel-create / channel-relabel trigger.
func (s *Service) PlaceChannel(ctx context.Context, realmID uuid.UUID, channelName string) {
	r, err := s.realms.GetByID(ctx, realmID)
	if err != nil {
		s.warn("placement: resolve realm", "channel", channelName, "err", err)
		return
	}
	c, err := s.channels.Get(ctx, realmID, channelName)
	if err != nil {
		s.warn("placement: load async channel", "channel", channelName, "err", err)
		return
	}
	if _, err := s.Place(ctx, r, c); err != nil {
		s.warn("placement: pass failed", "channel", channelName, "err", err)
	}
}

// PlaceRealm runs a pass for every ACTIVE channel in the realm — the
// cluster-change trigger, where any channel may have gained or lost a candidate
// or seen a placed async-channel shard become misplaced.
func (s *Service) PlaceRealm(ctx context.Context, realmID uuid.UUID) {
	r, err := s.realms.GetByID(ctx, realmID)
	if err != nil {
		s.warn("placement: resolve realm", "realm_id", realmID, "err", err)
		return
	}
	channels, err := s.channels.ListActive(ctx, realmID)
	if err != nil {
		s.warn("placement: list active channels", "realm_id", realmID, "err", err)
		return
	}
	for _, c := range channels {
		if _, err := s.Place(ctx, r, c); err != nil {
			s.warn("placement: pass failed", "channel", c.Name, "err", err)
		}
	}
}

// Sweep is the retry pass (003.7 "retry sweep"): every ACTIVE channel, in any
// realm, whose live async-channel shard rows are fewer than its declared
// `channel_partitions`. It returns how many shard rows it materialised.
func (s *Service) Sweep(ctx context.Context) (int, error) {
	channels, err := s.channels.ListUnderplaced(ctx)
	if err != nil {
		return 0, err
	}
	realms := map[uuid.UUID]realm.Realm{}
	created := 0
	for _, c := range channels {
		r, ok := realms[c.RealmID]
		if !ok {
			r, err = s.realms.GetByID(ctx, c.RealmID)
			if err != nil {
				s.warn("placement sweep: resolve realm", "realm_id", c.RealmID, "err", err)
				continue
			}
			realms[c.RealmID] = r
		}
		written, err := s.Place(ctx, r, c)
		if err != nil {
			s.warn("placement sweep: pass failed", "channel", c.Name, "err", err)
			continue
		}
		created += written
	}
	return created, nil
}

// Place runs one pass for one channel and returns how many async-channel shard
// rows it materialised. It is the single place shard rows are born.
//
// The pass does two things under one transaction: it re-evaluates every
// already-placed shard against the channel's current rules (setting or clearing
// the misplaced marker, never moving the shard — 003.7 "Re-placement"), and it
// creates a row for every shard index that has none and that selection could
// assign a cluster to.
func (s *Service) Place(
	ctx context.Context, r realm.Realm, c *channel.AsyncChannel,
) (int, error) {
	if c.State != channel.StateActive {
		return 0, nil // a paused or deleted channel is not placed
	}
	rules, err := placementdomain.ParseChannelRules(c.Labels)
	if err != nil {
		return 0, err
	}
	registered, err := s.clusters.ListAll(ctx, r.ID)
	if err != nil {
		return 0, err
	}
	byID := make(map[uuid.UUID]*cluster.Cluster, len(registered))
	for _, registeredCluster := range registered {
		byID[registeredCluster.ID] = registeredCluster
	}
	plan := rules.Plan(registered, int(c.ChannelPartitions))

	created := 0
	changed, err := s.topics.PlaceChannelShards(ctx, r.ID, c.ID,
		func(existing []*topic.KafkaTopic) (out.ShardPlan, error) {
			byName := make(map[string]*topic.KafkaTopic, len(existing))
			for _, shard := range existing {
				byName[shard.Name] = shard
			}

			var shardPlan out.ShardPlan
			for _, shard := range existing {
				if shard.State == topic.StateDeleted {
					continue
				}
				if remarkMisplaced(shard, rules, byID) {
					shardPlan.Update = append(shardPlan.Update, shard)
				}
			}
			for index := range int(c.ChannelPartitions) {
				if _, taken := byName[c.ShardName(index)]; taken {
					continue
				}
				host := plan.ByShardIndex[index]
				if host == nil {
					continue // nothing eligible yet; the retry sweep comes back
				}
				shard, err := newShard(r, c, index, host)
				if err != nil {
					return out.ShardPlan{}, err
				}
				shardPlan.Create = append(shardPlan.Create, shard)
			}
			created = len(shardPlan.Create)
			return shardPlan, nil
		})
	if err != nil {
		return 0, err
	}

	if created > 0 {
		s.info("placed async-channel shards",
			"channel", c.Name, "created", created, "clusters", clusterNames(plan.Clusters))
	}
	s.notify(ctx, r.ID, changed)
	return created, nil
}

// remarkMisplaced re-evaluates one placed shard against the channel's current
// rules and returns whether the marker changed. It never touches the shard's
// cluster — relocation is the migration flow (003.13).
func remarkMisplaced(
	shard *topic.KafkaTopic,
	rules placementdomain.ChannelRules,
	byID map[uuid.UUID]*cluster.Cluster,
) bool {
	var host *cluster.Cluster
	if shard.KafkaClusterID != nil {
		host = byID[*shard.KafkaClusterID] // absent ⇒ the cluster was deleted
	}
	if ok, reason := rules.CanHost(host); !ok {
		return shard.MarkMisplaced(reason)
	}
	return shard.ClearMisplaced()
}

// newShard materialises one async-channel shard on the chosen cluster, seeding
// `partitions` / `replication_factor` and the frozen config merge from that
// cluster's `cluster_configuration` (003.6, ADR-API-009).
func newShard(
	r realm.Realm, c *channel.AsyncChannel, index int, host *cluster.Cluster,
) (*topic.KafkaTopic, error) {
	shard, err := topic.New(r, c.ID, c.Name, index, host.Configuration, nil,
		topic.SeedPartitions(host.Configuration, FallbackPartitions),
		topic.SeedReplicationFactor(host.Configuration, FallbackReplicationFactor))
	if err != nil {
		return nil, err
	}
	if err := shard.PlaceOn(host.ID, host.Name); err != nil {
		return nil, err
	}
	shard.ChannelName = c.Name
	return shard, nil
}

// notify forwards the written rows to the Resource Provider agents that hold
// their clusters in scope (005 ADR §1.3, task 13.8). Best-effort: a disconnected
// agent picks the change up in the full set it gets on its next reconnect.
func (s *Service) notify(ctx context.Context, realmID uuid.UUID, shards []*topic.KafkaTopic) {
	if s.notifier == nil || len(shards) == 0 {
		return
	}
	s.notifier.ShardsChanged(ctx, realmID, shards)
}

func clusterNames(clusters []*cluster.Cluster) []string {
	names := make([]string, len(clusters))
	for i, c := range clusters {
		names[i] = c.Name
	}
	return names
}

func (s *Service) info(msg string, args ...any) {
	if s.log != nil {
		s.log.Info(msg, args...)
	}
}

func (s *Service) warn(msg string, args ...any) {
	if s.log != nil {
		s.log.Warn(msg, args...)
	}
}
