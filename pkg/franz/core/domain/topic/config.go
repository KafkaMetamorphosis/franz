package topic

// Materialize computes the reconciled topic configuration: the cluster's
// `cluster_configuration` overlaid by the shard's `topic_configuration`, with
// per-shard keys winning (003.3 / 003.6 "Config merge"). It returns a fresh map;
// `partitions` / `replication_factor` are dedicated fields and are NOT part of
// this merge.
//
// The result is frozen onto the shard at create / desired-state-change time — a
// later edit to the cluster's configuration does not re-run it for existing
// shards (that would silently reconfigure live topics).
func Materialize(clusterConfig, topicConfig map[string]string) map[string]string {
	merged := make(map[string]string, len(clusterConfig)+len(topicConfig))
	for k, v := range clusterConfig {
		merged[k] = v
	}
	for k, v := range topicConfig {
		merged[k] = v
	}
	return merged
}
