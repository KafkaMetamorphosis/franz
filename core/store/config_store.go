package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/franz-kafka/server/core/models"
	"github.com/segmentio/kafka-go"
)

// ConfigStore manages cluster and topic definitions in Kafka compacted topics
type ConfigStore struct {
	brokers                []string
	clustersTopicName      string
	definitionsTopicName   string
	clusterTopicsTopicName string

	// In-memory caches
	clusters      map[string]*models.Cluster
	definitions   map[string]*models.TopicDefinition
	clusterTopics map[string]*models.ClusterTopic
	mu            sync.RWMutex

	// Kafka writer/reader
	writer *kafka.Writer
}

// NewConfigStore creates a new configuration store
func NewConfigStore(brokers []string, clustersTopicName, definitionsTopicName, clusterTopicsTopicName string) (*ConfigStore, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	log.Printf("[ConfigStore] Initializing with brokers: %v", brokers)
	log.Printf("[ConfigStore] Topics - Clusters: %s, Definitions: %s, ClusterTopics: %s",
		clustersTopicName, definitionsTopicName, clusterTopicsTopicName)

	store := &ConfigStore{
		brokers:                brokers,
		clustersTopicName:      clustersTopicName,
		definitionsTopicName:   definitionsTopicName,
		clusterTopicsTopicName: clusterTopicsTopicName,
		clusters:               make(map[string]*models.Cluster),
		definitions:            make(map[string]*models.TopicDefinition),
		clusterTopics:          make(map[string]*models.ClusterTopic),
	}

	// Initialize writer (we'll use different topics via the Topic field in messages)
	store.writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		WriteTimeout: 10 * time.Second,
	}

	// Initialize topics
	if err := store.initializeTopics(); err != nil {
		return nil, fmt.Errorf("failed to initialize topics: %w", err)
	}

	// Load existing data
	if err := store.loadData(); err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
	}

	log.Printf("[ConfigStore] Loaded %d clusters, %d topic definitions, %d cluster topics",
		len(store.clusters), len(store.definitions), len(store.clusterTopics))

	return store, nil
}

// initializeTopics creates the compact topics if they don't exist
func (s *ConfigStore) initializeTopics() error {
	conn, err := kafka.Dial("tcp", s.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	topicsToCreate := []kafka.TopicConfig{
		{
			Topic:             s.clustersTopicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "cleanup.policy", ConfigValue: "compact"},
				{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
			},
		},
		{
			Topic:             s.definitionsTopicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "cleanup.policy", ConfigValue: "compact"},
				{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
			},
		},
		{
			Topic:             s.clusterTopicsTopicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "cleanup.policy", ConfigValue: "compact"},
				{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
			},
		},
	}

	err = conn.CreateTopics(topicsToCreate...)
	if err != nil {
		// Ignore "already exists" errors
		log.Printf("[ConfigStore] Topic creation result: %v", err)
	}

	return nil
}

// loadData loads existing data from compacted topics
func (s *ConfigStore) loadData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Load clusters
	if err := s.loadClusters(ctx); err != nil {
		return fmt.Errorf("failed to load clusters: %w", err)
	}

	// Load topic definitions
	if err := s.loadDefinitions(ctx); err != nil {
		return fmt.Errorf("failed to load definitions: %w", err)
	}

	// Load cluster topics
	if err := s.loadClusterTopics(ctx); err != nil {
		return fmt.Errorf("failed to load cluster topics: %w", err)
	}

	return nil
}

// loadClusters reads clusters from the clusters topic
func (s *ConfigStore) loadClusters(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   s.brokers,
		Topic:     s.clustersTopicName,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   500 * time.Millisecond,
	})
	defer reader.Close()

	if err := reader.SetOffset(0); err != nil {
		return err
	}

	for {
		msgCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := reader.ReadMessage(msgCtx)
		cancel()

		if err != nil {
			break
		}

		if msg.Value == nil {
			// Tombstone - delete
			s.mu.Lock()
			delete(s.clusters, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var cluster models.Cluster
		if err := json.Unmarshal(msg.Value, &cluster); err != nil {
			log.Printf("[ConfigStore] Failed to unmarshal cluster: %v", err)
			continue
		}

		s.mu.Lock()
		s.clusters[cluster.Name] = &cluster
		s.mu.Unlock()
	}

	return nil
}

// loadDefinitions reads topic definitions from the definitions topic
func (s *ConfigStore) loadDefinitions(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   s.brokers,
		Topic:     s.definitionsTopicName,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   500 * time.Millisecond,
	})
	defer reader.Close()

	if err := reader.SetOffset(0); err != nil {
		return err
	}

	for {
		msgCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := reader.ReadMessage(msgCtx)
		cancel()

		if err != nil {
			break
		}

		if msg.Value == nil {
			// Tombstone - delete
			s.mu.Lock()
			delete(s.definitions, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var def models.TopicDefinition
		if err := json.Unmarshal(msg.Value, &def); err != nil {
			log.Printf("[ConfigStore] Failed to unmarshal topic definition: %v", err)
			continue
		}

		s.mu.Lock()
		s.definitions[def.Name] = &def
		s.mu.Unlock()
	}

	return nil
}

// loadClusterTopics reads cluster topic mappings from the cluster topics topic
func (s *ConfigStore) loadClusterTopics(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   s.brokers,
		Topic:     s.clusterTopicsTopicName,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   500 * time.Millisecond,
	})
	defer reader.Close()

	if err := reader.SetOffset(0); err != nil {
		return err
	}

	for {
		msgCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := reader.ReadMessage(msgCtx)
		cancel()

		if err != nil {
			break
		}

		if msg.Value == nil {
			// Tombstone - delete
			s.mu.Lock()
			delete(s.clusterTopics, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var ct models.ClusterTopic
		if err := json.Unmarshal(msg.Value, &ct); err != nil {
			log.Printf("[ConfigStore] Failed to unmarshal cluster topic: %v", err)
			continue
		}

		s.mu.Lock()
		s.clusterTopics[ct.Key()] = &ct
		s.mu.Unlock()
	}

	return nil
}

// writeMessage writes a message to a specific topic
func (s *ConfigStore) writeMessage(ctx context.Context, topic string, key, value []byte) error {
	return s.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

// Close closes the store
func (s *ConfigStore) Close() error {
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}
