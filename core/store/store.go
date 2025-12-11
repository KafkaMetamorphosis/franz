package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/franz-kafka/server/core/config"
	"github.com/franz-kafka/server/core/models"
	"github.com/segmentio/kafka-go"
)

type Store struct {
	storeConfig config.StoreConfig

	// In-memory caches
	clusterClaims map[string]*models.ClusterClaim
	definitions   map[string]*models.TopicDefinition
	clusterTopics map[string]*models.ClusterTopic

	mu sync.RWMutex

	// Kafka writer/reader
	writer *kafka.Writer
}

func NewStore(storeConfig config.StoreConfig) (*Store, error) {
	if len(storeConfig.BootstrapURLs) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	log.Printf("[Store] Store Kafka cluster bootstrap url: %v", storeConfig.BootstrapURLs)

	store := &Store{
		storeConfig:   storeConfig,
		clusterClaims: make(map[string]*models.ClusterClaim),
		definitions:   make(map[string]*models.TopicDefinition),
		clusterTopics: make(map[string]*models.ClusterTopic),
	}

	store.writer = &kafka.Writer{
		Addr:         kafka.TCP(storeConfig.BootstrapURLs...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		WriteTimeout: 10 * time.Second,
	}

	if err := store.initializeTopics(); err != nil {
		return nil, fmt.Errorf("failed to initialize topics: %w", err)
	}

	if err := store.loadData(); err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
	}

	log.Printf("[Store] Loaded %d clusters, %d topic definitions, %d cluster topics claims",
		len(store.clusterClaims), len(store.definitions), len(store.clusterTopics))

	return store, nil
}

// initializeTopics creates the compact topics if they don't exist
func (s *Store) initializeTopics() error {
	log.Printf("[Store] Initializing compacted topics: %s, %s and %s", s.storeConfig.ClusterClaimsTopicConfig.Topic, s.storeConfig.TopicsClaimConfig.Topic, s.storeConfig.DefinitionsTopicConfig.Topic)
	conn, err := kafka.Dial("tcp", s.storeConfig.BootstrapURLs[0])
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	topicsToCreate := []kafka.TopicConfig{
		{
			Topic:             s.storeConfig.ClusterClaimsTopicConfig.Topic,
			NumPartitions:     s.storeConfig.ClusterClaimsTopicConfig.Partitions,
			ReplicationFactor: s.storeConfig.ClusterClaimsTopicConfig.ReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "cleanup.policy", ConfigValue: "compact"},
				{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
				{ConfigName: "delete.retention.ms", ConfigValue: "5"},
			},
		},
		{
			Topic:             s.storeConfig.DefinitionsTopicConfig.Topic,
			NumPartitions:     s.storeConfig.DefinitionsTopicConfig.Partitions,
			ReplicationFactor: s.storeConfig.DefinitionsTopicConfig.ReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "cleanup.policy", ConfigValue: "compact"},
				{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
				{ConfigName: "delete.retention.ms", ConfigValue: "5"},
			},
		},
		{
			Topic:             s.storeConfig.DefinitionsTopicConfig.Topic,
			NumPartitions:     s.storeConfig.DefinitionsTopicConfig.Partitions,
			ReplicationFactor: s.storeConfig.DefinitionsTopicConfig.ReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "cleanup.policy", ConfigValue: "compact"},
				{ConfigName: "min.cleanable.dirty.ratio", ConfigValue: "0.01"},
				{ConfigName: "delete.retention.ms", ConfigValue: "5"},
			},
		},
	}

	err = conn.CreateTopics(topicsToCreate...)
	if err != nil {
		log.Printf("[Store] Topic creation result: %v", err)
	}

	return nil
}

func (s *Store) loadData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.loadClusterClaims(ctx); err != nil {
		return fmt.Errorf("failed to load clusters: %w", err)
	}

	if err := s.loadDefinitions(ctx); err != nil {
		return fmt.Errorf("failed to load definitions: %w", err)
	}

	if err := s.loadClusterTopics(ctx); err != nil {
		return fmt.Errorf("failed to load cluster topics: %w", err)
	}

	return nil
}

func (s *Store) loadDefinitions(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.storeConfig.BootstrapURLs,
		Topic:    s.storeConfig.DefinitionsTopicConfig.Topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
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
			log.Printf("[Store] Failed to unmarshal topic definition: %v", err)
			continue
		}

		s.mu.Lock()
		s.definitions[def.Name] = &def
		s.mu.Unlock()
	}

	return nil
}

func (s *Store) loadClusterTopics(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.storeConfig.BootstrapURLs,
		Topic:    s.storeConfig.TopicsClaimConfig.Topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
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
			s.mu.Lock()
			delete(s.clusterTopics, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var ct models.ClusterTopic
		if err := json.Unmarshal(msg.Value, &ct); err != nil {
			log.Printf("[Store] Failed to unmarshal cluster topic: %v", err)
			continue
		}

		s.mu.Lock()
		s.clusterTopics[ct.Key()] = &ct
		s.mu.Unlock()
	}

	return nil
}

func (s *Store) writeMessage(ctx context.Context, topic string, key, value []byte) error {
	log.Printf("[Store] Writing message to topic: %s, key: %s, value: %s", topic, string(key), string(value))

	return s.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

func (s *Store) Close() error {
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}
