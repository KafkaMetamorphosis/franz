package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Admin wraps Kafka admin operations
type Admin struct {
	conn *kafka.Conn
}

// NewAdmin creates a new Kafka admin client
func NewAdmin(brokers []string) (*Admin, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	// Connect to the first broker
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to kafka broker: %w", err)
	}

	return &Admin{
		conn: conn,
	}, nil
}

// Close closes the admin connection
func (a *Admin) Close() error {
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}

// GetTopics retrieves all topics from the Kafka cluster
func (a *Admin) GetTopics(ctx context.Context) ([]kafka.Partition, error) {
	partitions, err := a.conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}
	return partitions, nil
}

// GetBrokers retrieves broker information
func (a *Admin) GetBrokers(ctx context.Context) ([]kafka.Broker, error) {
	brokers, err := a.conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("failed to get brokers: %w", err)
	}
	return brokers, nil
}

// GetTopicMetadata retrieves metadata for a specific topic
func (a *Admin) GetTopicMetadata(ctx context.Context, topic string) (*kafka.Partition, error) {
	partitions, err := a.conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition for topic %s: %w", topic, err)
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	// Return the first partition (all partitions of a topic have similar metadata)
	return &partitions[0], nil
}

// GetClusterMetadata retrieves cluster metadata
func (a *Admin) GetClusterMetadata(ctx context.Context) (map[string]interface{}, error) {
	brokers, err := a.GetBrokers(ctx)
	if err != nil {
		return nil, err
	}

	topics, err := a.GetTopics(ctx)
	if err != nil {
		return nil, err
	}

	// Count unique topics
	topicMap := make(map[string]bool)
	for _, partition := range topics {
		topicMap[partition.Topic] = true
	}

	metadata := map[string]interface{}{
		"broker_count": len(brokers),
		"topic_count":  len(topicMap),
		"brokers":      brokers,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	}

	return metadata, nil
}
