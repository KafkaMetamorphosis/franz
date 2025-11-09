package kafka

import (
	"context"
	"fmt"
	"log"

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

	log.Printf("[MetadataSync] SyncCluster: Starting sync for cluster %s (%s) using brokers: %v", clusterID, clusterName, brokers)

	// Connect to the cluster
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		log.Printf("[MetadataSync] SyncCluster: ERROR connecting to broker %s: %v", brokers[0], err)
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer conn.Close()
	log.Printf("[MetadataSync] SyncCluster: Successfully connected to broker %s", brokers[0])

	// Get brokers
	kafkaBrokers, err := conn.Brokers()
	if err != nil {
		log.Printf("[MetadataSync] SyncCluster: ERROR getting brokers: %v", err)
		return fmt.Errorf("failed to get brokers: %w", err)
	}
	log.Printf("[MetadataSync] SyncCluster: Found %d brokers:", len(kafkaBrokers))
	for i, broker := range kafkaBrokers {
		log.Printf("[MetadataSync] SyncCluster:   Broker %d: ID=%d, Host=%s, Port=%d", i, broker.ID, broker.Host, broker.Port)
	}

	// Get all partitions (topics)
	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("[MetadataSync] SyncCluster: ERROR reading partitions: %v", err)
		return fmt.Errorf("failed to read partitions: %w", err)
	}
	log.Printf("[MetadataSync] SyncCluster: Found %d partitions total", len(partitions))

	// Count unique topics
	topicMap := make(map[string]bool)
	for _, partition := range partitions {
		topicMap[partition.Topic] = true
	}
	log.Printf("[MetadataSync] SyncCluster: Found %d unique topics:", len(topicMap))
	for topic := range topicMap {
		log.Printf("[MetadataSync] SyncCluster:   - %s", topic)
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
	log.Printf("[MetadataSync] SyncCluster: Saving cluster metadata to store")
	if err := ms.metadataStore.SaveCluster(ctx, clusterMetadata); err != nil {
		log.Printf("[MetadataSync] SyncCluster: ERROR saving cluster metadata: %v", err)
		return fmt.Errorf("failed to save cluster metadata: %w", err)
	}

	log.Printf("[MetadataSync] SyncCluster: Successfully synced cluster %s", clusterID)
	return nil
}

// SyncTopic fetches topic metadata from a Kafka cluster and stores it
func (ms *MetadataSync) SyncTopic(ctx context.Context, clusterID, topicName string, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers provided")
	}

	log.Printf("[MetadataSync] SyncTopic: Starting sync for topic %s from cluster %s using broker %s", topicName, clusterID, brokers[0])

	// Connect to the cluster
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		log.Printf("[MetadataSync] SyncTopic: ERROR connecting to broker %s: %v", brokers[0], err)
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer conn.Close()
	log.Printf("[MetadataSync] SyncTopic: Successfully connected to broker %s", brokers[0])

	// Get topic partitions
	partitions, err := conn.ReadPartitions(topicName)
	if err != nil {
		log.Printf("[MetadataSync] SyncTopic: ERROR reading partitions for topic %s: %v", topicName, err)
		return fmt.Errorf("failed to read partitions for topic %s: %w", topicName, err)
	}

	if len(partitions) == 0 {
		log.Printf("[MetadataSync] SyncTopic: WARNING - topic %s not found (0 partitions)", topicName)
		return fmt.Errorf("topic %s not found", topicName)
	}
	log.Printf("[MetadataSync] SyncTopic: Found %d partitions for topic %s", len(partitions), topicName)

	// Build partition metadata
	partitionMetadata := make([]models.PartitionMetadata, 0, len(partitions))
	replicationFactor := 0

	for i, partition := range partitions {
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

		log.Printf("[MetadataSync] SyncTopic:   Partition %d: ID=%d, Leader=%d, Replicas=%v, ISRs=%v",
			i, pm.ID, pm.Leader, pm.Replicas, pm.ISRs)

		partitionMetadata = append(partitionMetadata, pm)

		// Set replication factor from first partition
		if replicationFactor == 0 {
			replicationFactor = len(partition.Replicas)
		}
	}
	log.Printf("[MetadataSync] SyncTopic: Replication factor: %d", replicationFactor)

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
	log.Printf("[MetadataSync] Saving topic %s metadata to store", topicName)
	if err := ms.metadataStore.SaveTopic(ctx, topicMetadata); err != nil {
		return fmt.Errorf("failed to save topic metadata: %w", err)
	}

	log.Printf("[MetadataSync] Successfully synced topic %s", topicName)
	return nil
}

// SyncAllTopics fetches metadata for all topics in a cluster and stores them
func (ms *MetadataSync) SyncAllTopics(ctx context.Context, clusterID string, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers provided")
	}

	log.Printf("[MetadataSync] SyncAllTopics: Starting sync for all topics in cluster %s using broker %s", clusterID, brokers[0])

	// Connect to the cluster
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		log.Printf("[MetadataSync] SyncAllTopics: ERROR connecting: %v", err)
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer conn.Close()

	// Get all partitions
	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("[MetadataSync] SyncAllTopics: ERROR reading partitions: %v", err)
		return fmt.Errorf("failed to read partitions: %w", err)
	}
	log.Printf("[MetadataSync] SyncAllTopics: Found %d partitions total", len(partitions))

	// Get unique topic names
	topicMap := make(map[string]bool)
	for _, partition := range partitions {
		topicMap[partition.Topic] = true
	}
	log.Printf("[MetadataSync] SyncAllTopics: Found %d unique topics to sync", len(topicMap))

	// Sync each topic
	successCount := 0
	errorCount := 0
	for topicName := range topicMap {
		log.Printf("[MetadataSync] SyncAllTopics: Syncing topic %s...", topicName)
		if err := ms.SyncTopic(ctx, clusterID, topicName, brokers); err != nil {
			// Log error but continue with other topics
			log.Printf("[MetadataSync] SyncAllTopics: ERROR syncing topic %s: %v", topicName, err)
			errorCount++
			continue
		}
		successCount++
	}

	log.Printf("[MetadataSync] SyncAllTopics: Completed. Success: %d, Errors: %d", successCount, errorCount)
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
