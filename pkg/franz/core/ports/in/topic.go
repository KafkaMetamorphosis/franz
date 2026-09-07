package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
)

// ListTopicsInput parameterises a ListKafkaTopics call. Both filters are by
// resource name and optional; PageToken is opaque.
type ListTopicsInput struct {
	AsyncChannel string
	KafkaCluster string
	PageSize     int32
	PageToken    string
}

// TopicPage is a page of List results.
type TopicPage struct {
	Topics        []*topic.KafkaTopic
	NextPageToken string
	TotalSize     int32
}

// KafkaTopicService is the driving port for Kafka Topic reads + the one client
// mutation (003.6). Franz creates and deletes the entity itself — there is no
// Create / Update / Delete here. The realm is taken from the request context.
type KafkaTopicService interface {
	Get(ctx context.Context, name string) (*topic.KafkaTopic, error)
	List(ctx context.Context, in ListTopicsInput) (TopicPage, error)
	// SetConsumption drains (DISABLED) or restores (ENABLED) the shard and
	// re-normalises the owning channel's traffic-share split.
	SetConsumption(ctx context.Context, name string, c topic.Consumption) (*topic.KafkaTopic, error)
}
