package topic

import (
	"strconv"
	"strings"
)

// Franz-vocabulary keys that may appear in a cluster's `cluster_configuration`
// (ADR-API-010) but are **not** real Kafka topic-config keys — a broker rejects
// `createTopics` / `alterConfigs` with `INVALID_CONFIG` if any of them is passed
// as topic config. `partitions` / `replication-factor` seed dedicated shard
// fields (003.6 OQ1); `kafka-version` is a cluster-substrate concern the Cluster
// Provider recipe reads. Materialize drops all of them from the merge.
const (
	ConfigKeyPartitions        = "partitions"
	ConfigKeyReplicationFactor = "replication-factor"
	ConfigKeyKafkaVersion      = "kafka-version"
)

// nonTopicConfigKeys is the set excluded from Materialize.
var nonTopicConfigKeys = map[string]bool{
	ConfigKeyPartitions:        true,
	ConfigKeyReplicationFactor: true,
	ConfigKeyKafkaVersion:      true,
}

// Materialize computes the reconciled topic configuration: the cluster's
// `cluster_configuration` overlaid by the shard's `topic_configuration`, with
// per-shard keys winning (003.3 / 003.6 "Config merge"). It returns a fresh map
// carrying only real Kafka topic-config keys — the Franz-vocabulary keys
// (`partitions`, `replication-factor`, `kafka-version`) are dropped from the
// cluster layer.
//
// The result is frozen onto the shard at create / desired-state-change time — a
// later edit to the cluster's configuration does not re-run it for existing
// shards (that would silently reconfigure live topics).
func Materialize(clusterConfig, topicConfig map[string]string) map[string]string {
	merged := make(map[string]string, len(clusterConfig)+len(topicConfig))
	for k, v := range clusterConfig {
		if nonTopicConfigKeys[k] {
			continue
		}
		merged[k] = v
	}
	for k, v := range topicConfig {
		if nonTopicConfigKeys[k] {
			continue
		}
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
