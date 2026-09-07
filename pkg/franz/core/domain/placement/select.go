package placement

import (
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
)

// Plan is the outcome of one selection pass for one Async Channel.
//
// It is a pure function of the channel's labels, the cluster set and the shard
// count: identical inputs always produce an identical Plan (003.7 invariant
// "placement is deterministic").
type Plan struct {
	// Clusters are the chosen clusters, ordered by `franz.affinity/weight`
	// descending then name ascending and capped at `franz.affinity/shard-size`.
	// Empty when nothing is eligible — then no async-channel shard is placed and
	// no `kafka_topic` row is created (ADR-API-009).
	Clusters []*cluster.Cluster

	// ByShardIndex maps async-channel shard index (0 … channelShards-1) to the
	// cluster it belongs on. Empty when Clusters is empty.
	ByShardIndex map[int]*cluster.Cluster
}

// Placed reports whether the plan assigns any async-channel shard at all.
func (p Plan) Placed() bool { return len(p.Clusters) > 0 }

// Select runs the 003.7 selection algorithm for one Async Channel:
//
//  1. candidates — every ACTIVE cluster whose labels satisfy
//     `franz.affinity/selector` (absent selector ⇒ no candidates),
//  2. drop candidates satisfying `franz.antiaffinity/selector`,
//  3. drop `drain`-tainted candidates and `no-creation`-tainted ones the channel
//     does not tolerate,
//  4. order by (weight desc, name asc) and take min(shard-size, |candidates|),
//  5. distribute the channelShards async-channel shards round-robin across them.
//
// A malformed reserved label on the channel is INVALID_ARGUMENT; a cluster whose
// reserved labels are malformed is simply not a candidate (fail closed).
func Select(
	channelLabels map[string]string, clusters []*cluster.Cluster, channelShards int,
) (Plan, error) {
	rules, err := ParseChannelRules(channelLabels)
	if err != nil {
		return Plan{}, err
	}
	return rules.Plan(clusters, channelShards), nil
}

// Plan runs steps 1–5 for already-parsed channel rules. The input order of
// clusters does not affect the result — candidates are re-ordered by
// (weight desc, name asc) before the cap is applied.
func (r ChannelRules) Plan(clusters []*cluster.Cluster, channelShards int) Plan {
	if channelShards < 1 {
		return Plan{}
	}
	candidates := r.Candidates(clusters)
	if len(candidates) == 0 {
		return Plan{}
	}

	spread := r.shardSize
	if spread > len(candidates) {
		spread = len(candidates)
	}
	chosen := candidates[:spread]

	// Round-robin. On an uneven split the earlier (higher-weight, then
	// lower-named) clusters take the remainder: with 5 channel shards over 2
	// clusters the first takes shards 0, 2 and 4 and the second takes 1 and 3
	// (003.7 OQ1).
	byShardIndex := make(map[int]*cluster.Cluster, channelShards)
	for index := range channelShards {
		byShardIndex[index] = chosen[index%spread]
	}
	return Plan{Clusters: chosen, ByShardIndex: byShardIndex}
}

// Candidates returns the clusters a new async-channel shard of this channel may
// be created on, ordered by (weight desc, name asc) — steps 1–4 without the
// shard-size cap. Exposed so callers can report "why is nothing placed".
func (r ChannelRules) Candidates(clusters []*cluster.Cluster) []*cluster.Cluster {
	if !r.hasAffinity {
		return nil // placement is opt-in (003.7 step 1)
	}
	candidates := make([]*cluster.Cluster, 0, len(clusters))
	for _, c := range clusters {
		if ok, _ := r.CanPlace(c); ok {
			candidates = append(candidates, c)
		}
	}
	sortCandidates(candidates)
	return candidates
}
