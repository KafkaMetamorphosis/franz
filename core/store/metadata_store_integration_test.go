//go:build integration
// +build integration

package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/franz-kafka/server/core/models"
)

// Run these tests with: go test -tags=integration ./core/store -v

func TestIntegration_FullWriteReadCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// Write cluster metadata
	cluster := &models.ClusterMetadata{
		ID:            "test-cluster-1",
		Name:          "Test Cluster 1",
		BootstrapURLs: []string{broker},
		BrokerCount:   1,
		TopicCount:    0,
	}

	err := store.SaveCluster(ctx, cluster)
	if err != nil {
		t.Fatalf("Failed to save cluster: %v", err)
	}

	// Write topic metadata
	topic := &models.TopicMetadata{
		ClusterID:         "test-cluster-1",
		TopicName:         "test-topic-1",
		ReplicationFactor: 1,
		Partitions: []models.PartitionMetadata{
			{ID: 0, Leader: 0, Replicas: []int{0}, ISRs: []int{0}},
		},
	}

	err = store.SaveTopic(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to save topic: %v", err)
	}

	// Read cluster back
	retrievedCluster, err := store.GetCluster("test-cluster-1")
	if err != nil {
		t.Fatalf("Failed to get cluster: %v", err)
	}

	if retrievedCluster.ID != cluster.ID {
		t.Errorf("Expected cluster ID %s, got %s", cluster.ID, retrievedCluster.ID)
	}

	// Read topic back
	retrievedTopic, err := store.GetTopic("test-cluster-1", "test-topic-1")
	if err != nil {
		t.Fatalf("Failed to get topic: %v", err)
	}

	if retrievedTopic.TopicName != topic.TopicName {
		t.Errorf("Expected topic name %s, got %s", topic.TopicName, retrievedTopic.TopicName)
	}
}

func TestIntegration_DataPersistenceAcrossRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	// First store instance - write data
	{
		store1, cleanup1 := SetupTestMetadataStore(t, ctx, broker)
		defer cleanup1()

		cluster := &models.ClusterMetadata{
			ID:            "persistent-cluster",
			Name:          "Persistent Cluster",
			BootstrapURLs: []string{broker},
			BrokerCount:   1,
			TopicCount:    1,
		}

		err := store1.SaveCluster(ctx, cluster)
		if err != nil {
			t.Fatalf("Failed to save cluster: %v", err)
		}
	}

	// Give Kafka time to persist
	time.Sleep(2 * time.Second)

	// Second store instance - read data
	{
		store2, cleanup2 := SetupTestMetadataStore(t, ctx, broker)
		defer cleanup2()

		cluster, err := store2.GetCluster("persistent-cluster")
		if err != nil {
			t.Fatalf("Failed to get cluster from new store instance: %v", err)
		}

		if cluster.ID != "persistent-cluster" {
			t.Errorf("Expected cluster ID persistent-cluster, got %s", cluster.ID)
		}
	}
}

func TestIntegration_TombstoneHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// Write cluster
	cluster := &models.ClusterMetadata{
		ID:            "delete-me",
		Name:          "Delete Me",
		BootstrapURLs: []string{broker},
		BrokerCount:   1,
		TopicCount:    0,
	}

	err := store.SaveCluster(ctx, cluster)
	if err != nil {
		t.Fatalf("Failed to save cluster: %v", err)
	}

	// Verify it exists
	_, err = store.GetCluster("delete-me")
	if err != nil {
		t.Fatalf("Cluster should exist: %v", err)
	}

	// Send tombstone (delete marker) by writing nil value
	err = store.client.WriteMessage(ctx, store.clustersTopicName, []byte("delete-me"), nil)
	if err != nil {
		t.Fatalf("Failed to write tombstone: %v", err)
	}

	// Reload data
	store2, cleanup2 := SetupTestMetadataStore(t, ctx, broker)
	defer cleanup2()

	// Verify cluster is deleted
	_, err = store2.GetCluster("delete-me")
	if err == nil {
		t.Error("Expected error for deleted cluster, got nil")
	}
}

func TestIntegration_ConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// Write multiple clusters concurrently
	done := make(chan error, 5)

	for i := 0; i < 5; i++ {
		go func(id int) {
			cluster := &models.ClusterMetadata{
				ID:            fmt.Sprintf("concurrent-cluster-%d", id),
				Name:          fmt.Sprintf("Concurrent Cluster %d", id),
				BootstrapURLs: []string{broker},
				BrokerCount:   1,
				TopicCount:    0,
			}

			done <- store.SaveCluster(ctx, cluster)
		}(i)
	}

	// Wait for all writes to complete
	for i := 0; i < 5; i++ {
		if err := <-done; err != nil {
			t.Errorf("Concurrent write %d failed: %v", i, err)
		}
	}

	// Verify all clusters were saved
	clusters := store.ListClusters()
	if len(clusters) != 5 {
		t.Errorf("Expected 5 clusters, got %d", len(clusters))
	}
}

func TestIntegration_LargePayload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// Create topic with many partitions
	partitions := make([]models.PartitionMetadata, 100)
	for i := 0; i < 100; i++ {
		partitions[i] = models.PartitionMetadata{
			ID:       i,
			Leader:   i % 3,
			Replicas: []int{i % 3, (i + 1) % 3, (i + 2) % 3},
			ISRs:     []int{i % 3, (i + 1) % 3},
		}
	}

	topic := &models.TopicMetadata{
		ClusterID:         "large-cluster",
		TopicName:         "large-topic",
		ReplicationFactor: 3,
		Partitions:        partitions,
	}

	err := store.SaveTopic(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to save large topic: %v", err)
	}

	// Retrieve and verify
	retrieved, err := store.GetTopic("large-cluster", "large-topic")
	if err != nil {
		t.Fatalf("Failed to get large topic: %v", err)
	}

	if len(retrieved.Partitions) != 100 {
		t.Errorf("Expected 100 partitions, got %d", len(retrieved.Partitions))
	}
}

func TestIntegration_RetryOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// This should retry and eventually succeed
	cluster := &models.ClusterMetadata{
		ID:            "retry-cluster",
		Name:          "Retry Cluster",
		BootstrapURLs: []string{broker},
		BrokerCount:   1,
		TopicCount:    0,
	}

	err := store.SaveCluster(ctx, cluster)
	if err != nil {
		t.Fatalf("Failed to save cluster with retries: %v", err)
	}

	// Verify it was saved
	retrieved, err := store.GetCluster("retry-cluster")
	if err != nil {
		t.Fatalf("Failed to retrieve cluster: %v", err)
	}

	if retrieved.ID != "retry-cluster" {
		t.Errorf("Expected cluster ID retry-cluster, got %s", retrieved.ID)
	}
}

func TestIntegration_CompactTopicCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// Write same cluster multiple times with different data
	for i := 0; i < 5; i++ {
		cluster := &models.ClusterMetadata{
			ID:            "compact-test",
			Name:          fmt.Sprintf("Version %d", i),
			BootstrapURLs: []string{broker},
			BrokerCount:   i,
			TopicCount:    i * 2,
		}

		err := store.SaveCluster(ctx, cluster)
		if err != nil {
			t.Fatalf("Failed to save cluster version %d: %v", i, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Reload and verify we get the latest version
	store2, cleanup2 := SetupTestMetadataStore(t, ctx, broker)
	defer cleanup2()

	cluster, err := store2.GetCluster("compact-test")
	if err != nil {
		t.Fatalf("Failed to get cluster: %v", err)
	}

	// Should have the latest values
	if cluster.BrokerCount != 4 {
		t.Errorf("Expected broker count 4, got %d", cluster.BrokerCount)
	}

	if cluster.TopicCount != 8 {
		t.Errorf("Expected topic count 8, got %d", cluster.TopicCount)
	}
}
