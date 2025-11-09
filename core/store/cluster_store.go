package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/franz-kafka/server/core/models"
)

// SaveCluster saves a cluster definition
func (s *ConfigStore) SaveCluster(ctx context.Context, cluster *models.Cluster) error {
	if err := cluster.Validate(); err != nil {
		return err
	}

	// Set timestamps
	now := time.Now().UTC()
	if cluster.CreatedAt.IsZero() {
		cluster.CreatedAt = now
	}
	cluster.UpdatedAt = now

	// Marshal to JSON
	data, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster: %w", err)
	}

	// Write to Kafka
	if err := s.writeMessage(ctx, s.clustersTopicName, []byte(cluster.Name), data); err != nil {
		return fmt.Errorf("failed to write cluster: %w", err)
	}

	// Update cache
	s.mu.Lock()
	s.clusters[cluster.Name] = cluster
	s.mu.Unlock()

	return nil
}

// GetCluster retrieves a cluster by name
func (s *ConfigStore) GetCluster(name string) (*models.Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, exists := s.clusters[name]
	if !exists {
		return nil, models.ErrClusterNotFound
	}

	return cluster, nil
}

// ListClusters returns all clusters
func (s *ConfigStore) ListClusters() []*models.Cluster {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clusters := make([]*models.Cluster, 0, len(s.clusters))
	for _, cluster := range s.clusters {
		clusters = append(clusters, cluster)
	}

	return clusters
}

// DeleteCluster deletes a cluster (writes tombstone)
func (s *ConfigStore) DeleteCluster(ctx context.Context, name string) error {
	// Write tombstone (nil value)
	if err := s.writeMessage(ctx, s.clustersTopicName, []byte(name), nil); err != nil {
		return fmt.Errorf("failed to delete cluster: %w", err)
	}

	// Remove from cache
	s.mu.Lock()
	delete(s.clusters, name)
	s.mu.Unlock()

	return nil
}
