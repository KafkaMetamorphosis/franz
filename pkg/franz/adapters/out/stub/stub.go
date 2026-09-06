// Package stub holds placeholder out-adapters for ports whose real
// implementation lands in a later deliverable. Each is a deliberate no-op with a
// comment naming the deliverable that replaces it.
package stub

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// NoTopicGuard reports zero live topics for every cluster. Replaced by a
// postgres-backed count in deliverable 09 (Kafka Topic), which creates the
// kafka_topic table this would otherwise query.
type NoTopicGuard struct{}

var _ out.ClusterTopicGuard = NoTopicGuard{}

// CountLiveTopics always returns 0.
func (NoTopicGuard) CountLiveTopics(context.Context, uuid.UUID) (int, error) { return 0, nil }
