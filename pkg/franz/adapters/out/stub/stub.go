// Package stub holds placeholder out-adapters for ports whose real
// implementation lands in a later deliverable. Each is a deliberate no-op with a
// comment naming the deliverable that replaces it.
package stub

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// NoTopicGuard reports zero live topics for every cluster. Replaced by a
// postgres-backed count in deliverable 09 (Kafka Topic), which creates the
// kafka_topic table this would otherwise query.
type NoTopicGuard struct{}

var _ out.ClusterTopicGuard = NoTopicGuard{}

// CountLiveTopics always returns 0.
func (NoTopicGuard) CountLiveTopics(context.Context, uuid.UUID) (int, error) { return 0, nil }

// NoPartitionNotifier drops every partition-assignment delta. For tests and
// wirings that do not run the Resource Provider contract; a real deployment
// wires resourceprovider.Notifier instead. Dropping a delta is safe by design —
// an agent full-resyncs on reconnect (005 ADR §1.3).
type NoPartitionNotifier struct{}

var _ out.PartitionNotifier = NoPartitionNotifier{}

func (NoPartitionNotifier) ShardsChanged(context.Context, uuid.UUID, []*topic.KafkaTopic) {}

func (NoPartitionNotifier) ClusterLabelsChanged(
	context.Context, uuid.UUID, string, map[string]string, map[string]string,
) {
}

func (NoPartitionNotifier) AgentSelectorChanged(
	context.Context, uuid.UUID, string, map[string]string, map[string]string,
) {
}
