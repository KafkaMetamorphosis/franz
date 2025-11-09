package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/franz-kafka/server/core/models"
)

// SaveClusterTopic saves a cluster topic mapping
func (s *ConfigStore) SaveClusterTopic(ctx context.Context, ct *models.ClusterTopic) error {
	if err := ct.Validate(); err != nil {
		return err
	}

	// Set timestamps
	now := time.Now().UTC()
	if ct.CreatedAt.IsZero() {
		ct.CreatedAt = now
	}
	ct.UpdatedAt = now

	// Marshal to JSON
	data, err := json.Marshal(ct)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster topic: %w", err)
	}

	// Write to Kafka (key is cluster:topic)
	key := ct.Key()
	if err := s.writeMessage(ctx, s.clusterTopicsTopicName, []byte(key), data); err != nil {
		return fmt.Errorf("failed to write cluster topic: %w", err)
	}

	// Update cache
	s.mu.Lock()
	s.clusterTopics[key] = ct
	s.mu.Unlock()

	return nil
}

// GetClusterTopic retrieves a cluster topic mapping
func (s *ConfigStore) GetClusterTopic(clusterName, topicName string) (*models.ClusterTopic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := clusterName + ":" + topicName
	ct, exists := s.clusterTopics[key]
	if !exists {
		return nil, models.ErrClusterTopicNotFound
	}

	return ct, nil
}

// ListClusterTopics returns all cluster topic mappings, optionally filtered by cluster
func (s *ConfigStore) ListClusterTopics(clusterName string) []*models.ClusterTopic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*models.ClusterTopic, 0)
	for _, ct := range s.clusterTopics {
		if clusterName == "" || ct.ClusterName == clusterName {
			topics = append(topics, ct)
		}
	}

	return topics
}

// DeleteClusterTopic deletes a cluster topic mapping (writes tombstone)
func (s *ConfigStore) DeleteClusterTopic(ctx context.Context, clusterName, topicName string) error {
	key := clusterName + ":" + topicName

	// Write tombstone (nil value)
	if err := s.writeMessage(ctx, s.clusterTopicsTopicName, []byte(key), nil); err != nil {
		return fmt.Errorf("failed to delete cluster topic: %w", err)
	}

	// Remove from cache
	s.mu.Lock()
	delete(s.clusterTopics, key)
	s.mu.Unlock()

	return nil
}
