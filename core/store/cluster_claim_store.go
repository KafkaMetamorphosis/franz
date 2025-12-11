package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/franz-kafka/server/core/models"
	"github.com/segmentio/kafka-go"
)

func (s *Store) SaveClusterClaim(ctx context.Context, clusterClaim *models.ClusterClaim) (*models.ClusterClaim, error) {
	if err := clusterClaim.Validate(); err != nil {
		return nil, err
	}

	log.Printf("[ClusterClaimStore] Saving cluster: %s", clusterClaim.Metadata.Name)

	now := time.Now()
	clusterClaim.CreatedAt = now
	clusterClaim.UpdatedAt = now

	data, err := json.Marshal(clusterClaim)

	if err != nil {
		return nil, fmt.Errorf("failed to marshal cluster: %w", err)
	}

	log.Printf("[ClusterClaimStore] Cluster data: %v", string(data))

	if err := s.writeMessage(ctx, s.storeConfig.ClusterClaimsTopicConfig.Topic, []byte(clusterClaim.Metadata.Name), data); err != nil {
		return nil, fmt.Errorf("failed to write kafka topic: %w", err)
	}

	s.mu.Lock()
	s.clusterClaims[clusterClaim.Metadata.Name] = clusterClaim
	s.mu.Unlock()

	return clusterClaim, nil
}

// GetCluster retrieves a cluster by name
func (s *Store) GetCluster(name string) (*models.ClusterClaim, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, exists := s.clusterClaims[name]
	if !exists {
		return nil, models.ErrClusterNotFound
	}

	return cluster, nil
}

// ListClusters returns all clusters
func (s *Store) ListClusters() []*models.ClusterClaim {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clusters := make([]*models.ClusterClaim, 0, len(s.clusterClaims))
	for _, cluster := range s.clusterClaims {
		clusters = append(clusters, cluster)
	}

	return clusters
}

// DeleteClusterClaim deletes a cluster (writes tombstone)
func (s *Store) DeleteClusterClaim(ctx context.Context, name string) error {
	// Write tombstone (nil value)
	if err := s.writeMessage(ctx, s.storeConfig.ClusterClaimsTopicConfig.Topic, []byte(name), nil); err != nil {
		return fmt.Errorf("failed to delete cluster: %w", err)
	}

	// Remove from cache
	s.mu.Lock()
	delete(s.clusterClaims, name)
	s.mu.Unlock()

	return nil
}

func (s *Store) loadClusterClaims(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.storeConfig.BootstrapURLs,
		Topic:    s.storeConfig.ClusterClaimsTopicConfig.Topic,
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
			delete(s.clusterClaims, string(msg.Key))
			s.mu.Unlock()
			continue
		}

		var cluster models.ClusterClaim
		if err := json.Unmarshal(msg.Value, &cluster); err != nil {
			log.Printf("[Store] Failed to unmarshal cluster: %v", err)
			continue
		}

		s.mu.Lock()
		s.clusterClaims[cluster.Metadata.Name] = &cluster
		s.mu.Unlock()
	}

	return nil
}
