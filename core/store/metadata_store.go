package store

import (
	"context"
	"encoding/json"
	"fmt"
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

	store := &MetadataStore{
		brokers:           brokers,
		clustersTopicName: clustersTopicName,
		topicsTopicName:   topicsTopicName,
		clusters:          make(map[string]*models.ClusterMetadata),
		topics:            make(map[string]*models.TopicMetadata),
	}

	// Initialize topics
	if err := store.initializeTopics(); err != nil {
		return nil, fmt.Errorf("failed to initialize topics: %w", err)
	}

	// Load existing data
	if err := store.loadData(); err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
	}

	return store, nil
}

// initializeTopics creates the compact topics if they don't exist
func (s *MetadataStore) initializeTopics() error {
	conn, err := kafka.Dial("tcp", s.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	// Create clusters topic
	if err := s.createTopicIfNotExists(controllerConn, s.clustersTopicName); err != nil {
		return fmt.Errorf("failed to create clusters topic: %w", err)
	}

	// Create topics topic
	if err := s.createTopicIfNotExists(controllerConn, s.topicsTopicName); err != nil {
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
	if err := s.loadClusters(ctx); err != nil {
		return fmt.Errorf("failed to load clusters: %w", err)
	}

	// Load topics
	if err := s.loadTopics(ctx); err != nil {
		return fmt.Errorf("failed to load topics: %w", err)
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

// SaveCluster saves cluster metadata to the compact topic
func (s *MetadataStore) SaveCluster(ctx context.Context, cluster *models.ClusterMetadata) error {
	cluster.LastSyncTime = time.Now().UTC()

	data, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster: %w", err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(s.brokers...),
		Topic:    s.clustersTopicName,
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(cluster.ID),
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("failed to write cluster message: %w", err)
	}

	// Update in-memory cache
	s.mu.Lock()
	s.clusters[cluster.ID] = cluster
	s.mu.Unlock()

	return nil
}

// SaveTopic saves topic metadata to the compact topic
func (s *MetadataStore) SaveTopic(ctx context.Context, topic *models.TopicMetadata) error {
	topic.LastSyncTime = time.Now().UTC()

	data, err := json.Marshal(topic)
	if err != nil {
		return fmt.Errorf("failed to marshal topic: %w", err)
	}

	key := fmt.Sprintf("%s:%s", topic.ClusterID, topic.TopicName)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(s.brokers...),
		Topic:    s.topicsTopicName,
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("failed to write topic message: %w", err)
	}

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
