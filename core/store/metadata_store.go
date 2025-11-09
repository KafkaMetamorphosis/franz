package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/franz-kafka/server/core/models"
	"github.com/segmentio/kafka-go"
)

// MetadataStore manages cluster and topic metadata in Kafka compact topics
type MetadataStore struct {
	brokers           []string
	clustersTopicName string
	topicsTopicName   string
	client            KafkaClient // Kafka client for operations

	// In-memory caches
	clusters map[string]*models.ClusterMetadata
	topics   map[string]*models.TopicMetadata
	mu       sync.RWMutex
}

// NewMetadataStore creates a new metadata store
func NewMetadataStore(brokers []string, clustersTopicName, topicsTopicName string) (*MetadataStore, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	log.Printf("[MetadataStore] Initializing with brokers: %v, clusters topic: %s, topics topic: %s",
		brokers, clustersTopicName, topicsTopicName)

	// Create real Kafka client with 10s timeout
	client, err := NewRealKafkaClient(brokers, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	store := &MetadataStore{
		brokers:           brokers,
		clustersTopicName: clustersTopicName,
		topicsTopicName:   topicsTopicName,
		client:            client,
		clusters:          make(map[string]*models.ClusterMetadata),
		topics:            make(map[string]*models.TopicMetadata),
	}

	// Initialize topics
	if err := store.initializeTopics(); err != nil {
		return nil, fmt.Errorf("failed to initialize topics: %w", err)
	}

	log.Printf("[MetadataStore] Topics initialized successfully")

	// Load existing data
	if err := store.loadData(); err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
	}

	log.Printf("[MetadataStore] Loaded %d clusters and %d topics from storage", len(store.clusters), len(store.topics))

	return store, nil
}

// initializeTopics creates the compact topics if they don't exist
func (s *MetadataStore) initializeTopics() error {
	// Create clusters topic
	err := s.client.CreateTopics(kafka.TopicConfig{
		Topic:             s.clustersTopicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "cleanup.policy", ConfigValue: "compact"},
			{ConfigName: "segment.ms", ConfigValue: "86400000"},
			{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
		},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create clusters topic: %w", err)
	}

	// Create topics topic
	err = s.client.CreateTopics(kafka.TopicConfig{
		Topic:             s.topicsTopicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "cleanup.policy", ConfigValue: "compact"},
			{ConfigName: "segment.ms", ConfigValue: "86400000"},
			{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
		},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create topics topic: %w", err)
	}

	return nil
}

// createTopicIfNotExists creates a topic with compact cleanup policy if it doesn't exist
func (s *MetadataStore) createTopicIfNotExists(conn *kafka.Conn, topicName string) error {
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "cleanup.policy",
					ConfigValue: "compact",
				},
				{
					ConfigName:  "segment.ms",
					ConfigValue: "86400000", // 1 day
				},
				{
					ConfigName:  "min.cleanable.dirty.ratio",
					ConfigValue: "0.01",
				},
			},
		},
	}

	err := conn.CreateTopics(topicConfigs...)
	if err != nil {
		// Check if error is "topic already exists"
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}

	return nil
}

// loadData loads existing metadata from compact topics
func (s *MetadataStore) loadData() error {
	ctx := context.Background()

	// Load clusters
	clusterMsgs, err := s.client.ReadMessages(ctx, s.clustersTopicName, 0)
	if err != nil {
		return fmt.Errorf("failed to read clusters: %w", err)
	}

	for _, msg := range clusterMsgs {
		if msg.Value == nil {
			s.mu.Lock()
			delete(s.clusters, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var cluster models.ClusterMetadata
		if err := json.Unmarshal(msg.Value, &cluster); err != nil {
			continue
		}

		s.mu.Lock()
		s.clusters[cluster.ID] = &cluster
		s.mu.Unlock()
	}

	// Load topics
	topicMsgs, err := s.client.ReadMessages(ctx, s.topicsTopicName, 0)
	if err != nil {
		return fmt.Errorf("failed to read topics: %w", err)
	}

	for _, msg := range topicMsgs {
		if msg.Value == nil {
			s.mu.Lock()
			delete(s.topics, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var topic models.TopicMetadata
		if err := json.Unmarshal(msg.Value, &topic); err != nil {
			continue
		}

		s.mu.Lock()
		s.topics[string(msg.Key)] = &topic
		s.mu.Unlock()
	}

	return nil
}

// loadClusters loads cluster metadata from the clusters topic
func (s *MetadataStore) loadClusters(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   s.brokers,
		Topic:     s.clustersTopicName,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Seek to beginning
	if err := reader.SetOffset(0); err != nil {
		return fmt.Errorf("failed to set offset: %w", err)
	}

	// Read all messages
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// If we've reached the end, that's okay
			if err == context.DeadlineExceeded || err == context.Canceled {
				break
			}
			// Check if there are no more messages
			break
		}

		// Skip tombstone messages (delete markers)
		if msg.Value == nil {
			s.mu.Lock()
			delete(s.clusters, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var cluster models.ClusterMetadata
		if err := json.Unmarshal(msg.Value, &cluster); err != nil {
			// Log error but continue
			continue
		}

		s.mu.Lock()
		s.clusters[cluster.ID] = &cluster
		s.mu.Unlock()
	}

	return nil
}

// loadTopics loads topic metadata from the topics topic
func (s *MetadataStore) loadTopics(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   s.brokers,
		Topic:     s.topicsTopicName,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Seek to beginning
	if err := reader.SetOffset(0); err != nil {
		return fmt.Errorf("failed to set offset: %w", err)
	}

	// Read all messages
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// If we've reached the end, that's okay
			break
		}

		// Skip tombstone messages (delete markers)
		if msg.Value == nil {
			s.mu.Lock()
			delete(s.topics, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var topic models.TopicMetadata
		if err := json.Unmarshal(msg.Value, &topic); err != nil {
			// Log error but continue
			continue
		}

		s.mu.Lock()
		s.topics[string(msg.Key)] = &topic
		s.mu.Unlock()
	}

	return nil
}

// SaveCluster saves cluster metadata to the compact topic with retries
func (s *MetadataStore) SaveCluster(ctx context.Context, cluster *models.ClusterMetadata) error {
	// Validate cluster data
	if err := s.validateCluster(cluster); err != nil {
		return fmt.Errorf("invalid cluster data: %w", err)
	}

	cluster.LastSyncTime = time.Now().UTC()

	data, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster: %w", err)
	}

	log.Printf("[MetadataStore] Saving cluster %s to topic %s", cluster.ID, s.clustersTopicName)

	// Retry logic with exponential backoff
	err = s.retryWithBackoff(ctx, func() error {
		return s.client.WriteMessage(ctx, s.clustersTopicName, []byte(cluster.ID), data)
	})

	if err != nil {
		log.Printf("[MetadataStore] ERROR writing cluster message: %v", err)
		return fmt.Errorf("failed to write cluster message: %w", err)
	}

	log.Printf("[MetadataStore] Successfully saved cluster %s", cluster.ID)

	// Update in-memory cache
	s.mu.Lock()
	s.clusters[cluster.ID] = cluster
	s.mu.Unlock()

	return nil
}

// SaveTopic saves topic metadata to the compact topic with retries
func (s *MetadataStore) SaveTopic(ctx context.Context, topic *models.TopicMetadata) error {
	// Validate topic data
	if err := s.validateTopic(topic); err != nil {
		return fmt.Errorf("invalid topic data: %w", err)
	}

	topic.LastSyncTime = time.Now().UTC()

	data, err := json.Marshal(topic)
	if err != nil {
		return fmt.Errorf("failed to marshal topic: %w", err)
	}

	key := fmt.Sprintf("%s:%s", topic.ClusterID, topic.TopicName)

	log.Printf("[MetadataStore] Saving topic %s to topic %s", key, s.topicsTopicName)

	// Retry logic with exponential backoff
	err = s.retryWithBackoff(ctx, func() error {
		return s.client.WriteMessage(ctx, s.topicsTopicName, []byte(key), data)
	})

	if err != nil {
		log.Printf("[MetadataStore] ERROR writing topic message: %v", err)
		return fmt.Errorf("failed to write topic message: %w", err)
	}

	log.Printf("[MetadataStore] Successfully saved topic %s", key)

	// Update in-memory cache
	s.mu.Lock()
	s.topics[key] = topic
	s.mu.Unlock()

	return nil
}

// GetCluster retrieves cluster metadata by ID
func (s *MetadataStore) GetCluster(clusterID string) (*models.ClusterMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, exists := s.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("cluster %s not found", clusterID)
	}

	return cluster, nil
}

// GetTopic retrieves topic metadata by cluster ID and topic name
func (s *MetadataStore) GetTopic(clusterID, topicName string) (*models.TopicMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", clusterID, topicName)
	topic, exists := s.topics[key]
	if !exists {
		return nil, fmt.Errorf("topic %s in cluster %s not found", topicName, clusterID)
	}

	return topic, nil
}

// ListClusters returns all stored clusters
func (s *MetadataStore) ListClusters() []*models.ClusterMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clusters := make([]*models.ClusterMetadata, 0, len(s.clusters))
	for _, cluster := range s.clusters {
		clusters = append(clusters, cluster)
	}

	return clusters
}

// ListTopics returns all stored topics, optionally filtered by cluster ID
func (s *MetadataStore) ListTopics(clusterID string) []*models.TopicMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*models.TopicMetadata, 0)
	for key, topic := range s.topics {
		if clusterID == "" || strings.HasPrefix(key, clusterID+":") {
			topics = append(topics, topic)
		}
	}

	return topics
}

// validateCluster validates cluster metadata before storing
func (s *MetadataStore) validateCluster(cluster *models.ClusterMetadata) error {
	if cluster == nil {
		return fmt.Errorf("cluster is nil")
	}
	if cluster.ID == "" {
		return fmt.Errorf("cluster ID is required")
	}
	if cluster.Name == "" {
		return fmt.Errorf("cluster name is required")
	}
	if len(cluster.BootstrapURLs) == 0 {
		return fmt.Errorf("at least one bootstrap URL is required")
	}
	return nil
}

// validateTopic validates topic metadata before storing
func (s *MetadataStore) validateTopic(topic *models.TopicMetadata) error {
	if topic == nil {
		return fmt.Errorf("topic is nil")
	}
	if topic.ClusterID == "" {
		return fmt.Errorf("cluster ID is required")
	}
	if topic.TopicName == "" {
		return fmt.Errorf("topic name is required")
	}
	return nil
}

// retryWithBackoff retries a function with exponential backoff
func (s *MetadataStore) retryWithBackoff(ctx context.Context, fn func() error) error {
	maxRetries := 3
	baseDelay := 100 * time.Millisecond
	maxDelay := 2 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Calculate exponential backoff delay
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			if delay > maxDelay {
				delay = maxDelay
			}

			log.Printf("[MetadataStore] Retry attempt %d after %v", attempt+1, delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("[MetadataStore] Attempt %d failed: %v", attempt+1, err)
	}

	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}
