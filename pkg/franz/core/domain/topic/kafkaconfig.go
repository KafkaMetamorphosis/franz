package topic

// The topic-level configuration keys a Kafka broker accepts on `createTopics` /
// `alterConfigs` (Kafka 3.7). 003.8's write whitelist says a governance action
// may write `topic_configuration.<key>` only when `<key>` is "a real Kafka topic
// config key"; this set is that check.
//
// Franz never validates config *values* (ADR-API-010) — a value is forwarded
// verbatim and the broker is the authority. Only the key is policed, and only on
// the governance write path: an operator editing `topic_configuration` directly
// is not constrained by this list.
//
// 003.8 OQ5 asks whether a *curated subset* should be writable (excluding
// semantics-changing keys such as `cleanup.policy`). The position taken here is
// the un-curated set: a policy may write any real topic-config key. Curation is
// an authorization concern (who may create policies), not a config-key concern,
// and a partial list would silently reject legitimate automation.
var kafkaTopicConfigKeys = map[string]bool{
	"cleanup.policy":                          true,
	"compression.type":                        true,
	"delete.retention.ms":                     true,
	"file.delete.delay.ms":                    true,
	"flush.messages":                          true,
	"flush.ms":                                true,
	"follower.replication.throttled.replicas": true,
	"index.interval.bytes":                    true,
	"leader.replication.throttled.replicas":   true,
	"local.retention.bytes":                   true,
	"local.retention.ms":                      true,
	"max.compaction.lag.ms":                   true,
	"max.message.bytes":                       true,
	"message.format.version":                  true,
	"message.timestamp.after.max.ms":          true,
	"message.timestamp.before.max.ms":         true,
	"message.timestamp.difference.max.ms":     true,
	"message.timestamp.type":                  true,
	"min.cleanable.dirty.ratio":               true,
	"min.compaction.lag.ms":                   true,
	"min.insync.replicas":                     true,
	"preallocate":                             true,
	"remote.storage.enable":                   true,
	"retention.bytes":                         true,
	"retention.ms":                            true,
	"segment.bytes":                           true,
	"segment.index.bytes":                     true,
	"segment.jitter.ms":                       true,
	"segment.ms":                              true,
	"unclean.leader.election.enable":          true,
}

// IsKafkaConfigKey reports whether key is a topic-level Kafka configuration key.
func IsKafkaConfigKey(key string) bool { return kafkaTopicConfigKeys[key] }

// KafkaConfigKeys returns the recognised key set as a fresh map, for callers
// that want to enumerate it (tests, the console).
func KafkaConfigKeys() map[string]bool {
	out := make(map[string]bool, len(kafkaTopicConfigKeys))
	for k, v := range kafkaTopicConfigKeys {
		out[k] = v
	}
	return out
}
