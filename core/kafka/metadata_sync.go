package kafka

import (
	"context"
	"fmt"

	"github.com/franz-kafka/server/core/models"
	"github.com/franz-kafka/server/core/store"
	"github.com/segmentio/kafka-go"
)

// MetadataSync handles syncing metadata from Kafka clusters to the metadata store
type MetadataSync struct {
	metadataStore *store.MetadataStore
}

// NewMetadataSync creates a new metadata sync service
func NewMetadataSync(metadataStore *store.MetadataStore) *MetadataSync {
	return &MetadataSync{
		metadataStore: metadataStore,
	}
}

// SyncCluster fetches cluster metadata from a Kafka cluster and stores it
func (ms *MetadataSync) SyncCluster(ctx context.Context, clusterID, clusterName string, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers provided")
	}

	// Connect to the cluster
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer conn.Close()

	// Get brokers
	kafkaBrokers, err := conn.Brokers()
	if err != nil {
		return fmt.Errorf("failed to get brokers: %w", err)
	}

	// Get all partitions (topics)
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	// Count unique topics
	topicMap := make(map[string]bool)
	for _, partition := range partitions {
		topicMap[partition.Topic] = true
	}

	// Create cluster metadata
	clusterMetadata := &models.ClusterMetadata{
		ID:            clusterID,
		Name:          clusterName,
		BootstrapURLs: brokers,
		BrokerCount:   len(kafkaBrokers),
		TopicCount:    len(topicMap),
	}

	// Save to store
	if err := ms.metadataStore.SaveCluster(ctx, clusterMetadata); err != nil {
		return fmt.Errorf("failed to save cluster metadata: %w", err)
	}

	return nil
}

// SyncTopic fetches topic metadata from a Kafka cluster and stores it
func (ms *MetadataSync) SyncTopic(ctx context.Context, clusterID, topicName string, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers provided")
	}

	// Connect to the cluster
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer conn.Close()

	// Get topic partitions
	partitions, err := conn.ReadPartitions(topicName)
	if err != nil {
		return fmt.Errorf("failed to read partitions for topic %s: %w", topicName, err)
	}

	if len(partitions) == 0 {
		return fmt.Errorf("topic %s not found", topicName)
	}

	// Build partition metadata
	partitionMetadata := make([]models.PartitionMetadata, 0, len(partitions))
	replicationFactor := 0

	for _, partition := range partitions {
		pm := models.PartitionMetadata{
			ID:       partition.ID,
			Leader:   partition.Leader.ID,
			Replicas: make([]int, 0, len(partition.Replicas)),
			ISRs:     make([]int, 0, len(partition.Isr)),
		}

		for _, replica := range partition.Replicas {
			pm.Replicas = append(pm.Replicas, replica.ID)
		}

		for _, isr := range partition.Isr {
			pm.ISRs = append(pm.ISRs, isr.ID)
		}

		partitionMetadata = append(partitionMetadata, pm)

		// Set replication factor from first partition
		if replicationFactor == 0 {
			replicationFactor = len(partition.Replicas)
		}
	}

	// Get topic configs
	configs, err := ms.getTopicConfigs(conn, topicName)
	if err != nil {
		// Log error but continue without configs
		configs = make(map[string]string)
	}

	// Create topic metadata
	topicMetadata := &models.TopicMetadata{
		ClusterID:         clusterID,
		TopicName:         topicName,
		Partitions:        partitionMetadata,
		ReplicationFactor: replicationFactor,
		Configs:           configs,
	}

	// Save to store
	if err := ms.metadataStore.SaveTopic(ctx, topicMetadata); err != nil {
		return fmt.Errorf("failed to save topic metadata: %w", err)
	}

	return nil
}

// SyncAllTopics fetches metadata for all topics in a cluster and stores them
func (ms *MetadataSync) SyncAllTopics(ctx context.Context, clusterID string, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers provided")
	}

	// Connect to the cluster
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer conn.Close()

	// Get all partitions
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	// Get unique topic names
	topicMap := make(map[string]bool)
	for _, partition := range partitions {
		topicMap[partition.Topic] = true
	}

	// Sync each topic
	for topicName := range topicMap {
		if err := ms.SyncTopic(ctx, clusterID, topicName, brokers); err != nil {
			// Log error but continue with other topics
			fmt.Printf("Error syncing topic %s: %v\n", topicName, err)
			continue
		}
	}

	return nil
}

// getTopicConfigs attempts to retrieve topic configuration
// Note: kafka-go has limited support for topic configs, so this is a basic implementation
func (ms *MetadataSync) getTopicConfigs(conn *kafka.Conn, topicName string) (map[string]string, error) {
	// kafka-go doesn't have a direct API to get topic configs
	// For now, return an empty map
	// This can be enhanced later with admin API calls
	return make(map[string]string), nil
}
