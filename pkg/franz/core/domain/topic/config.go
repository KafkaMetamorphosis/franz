package topic

import (
	"strconv"
	"strings"
)

// The `cluster_configuration` keys that seed a shard's dedicated fields rather
// than its config merge (ADR-API-010, 003.6 OQ1). Placement reads them off the
// chosen cluster to fill `partitions` / `replication_factor`; they are Franz
// vocabulary, not real Kafka topic-config keys, so Materialize drops them.
const (
	ConfigKeyPartitions        = "partitions"
	ConfigKeyReplicationFactor = "replication-factor"
)

// Materialize computes the reconciled topic configuration: the cluster's
// `cluster_configuration` overlaid by the shard's `topic_configuration`, with
// per-shard keys winning (003.3 / 003.6 "Config merge"). It returns a fresh map;
// `partitions` / `replication_factor` are dedicated fields and are NOT part of
// this merge, so the two keys that seed them are dropped from the cluster layer.
//
// The result is frozen onto the shard at create / desired-state-change time — a
// later edit to the cluster's configuration does not re-run it for existing
// shards (that would silently reconfigure live topics).
func Materialize(clusterConfig, topicConfig map[string]string) map[string]string {
	merged := make(map[string]string, len(clusterConfig)+len(topicConfig))
	for k, v := range clusterConfig {
		if k == ConfigKeyPartitions || k == ConfigKeyReplicationFactor {
			continue
		}
		merged[k] = v
	}
	for k, v := range topicConfig {
		merged[k] = v
	}
	return merged
}

// SeedPartitions reads the desired Kafka partition count for a new shard off the
// chosen cluster's `cluster_configuration`, falling back to fallback when the
// key is absent or unparseable (Franz never validates the map's values —
// ADR-API-010).
func SeedPartitions(clusterConfig map[string]string, fallback int32) int32 {
	return seedInt(clusterConfig, ConfigKeyPartitions, fallback)
}

// SeedReplicationFactor is SeedPartitions for `replication-factor`.
func SeedReplicationFactor(clusterConfig map[string]string, fallback int32) int32 {
	return seedInt(clusterConfig, ConfigKeyReplicationFactor, fallback)
}

func seedInt(config map[string]string, key string, fallback int32) int32 {
	n, err := strconv.Atoi(strings.TrimSpace(config[key]))
	if err != nil || n < 1 {
		return fallback
	}
	return int32(n)
}
