//go:build integration
// +build integration

package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/franz-kafka/server/core/models"
	"github.com/segmentio/kafka-go"
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

func TestIntegration_HostnameResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// This test reproduces the issue where Kafka returns internal hostnames
	// like "kafka-fleet:9092" that need to be resolved to actual addresses

	// Create a topic with multiple partitions
	topic := &models.TopicMetadata{
		ClusterID:         "hostname-test-cluster",
		TopicName:         "hostname-test-topic",
		ReplicationFactor: 1,
		Partitions: []models.PartitionMetadata{
			{ID: 0, Leader: 0, Replicas: []int{0}, ISRs: []int{0}},
			{ID: 1, Leader: 0, Replicas: []int{0}, ISRs: []int{0}},
			{ID: 2, Leader: 0, Replicas: []int{0}, ISRs: []int{0}},
		},
	}

	// This should work even if Kafka returns internal hostnames
	err := store.SaveTopic(ctx, topic)
	if err != nil {
		t.Fatalf("Failed to save topic (hostname resolution failed): %v", err)
	}

	// Verify the topic was saved and can be retrieved
	retrieved, err := store.GetTopic("hostname-test-cluster", "hostname-test-topic")
	if err != nil {
		t.Fatalf("Failed to retrieve topic after save: %v", err)
	}

	if retrieved.TopicName != topic.TopicName {
		t.Errorf("Expected topic name %s, got %s", topic.TopicName, retrieved.TopicName)
	}

	if len(retrieved.Partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(retrieved.Partitions))
	}

	// Try writing directly with the client to test low-level hostname handling
	testKey := []byte("test-key")
	testValue := []byte(`{"test": "data"}`)

	err = store.client.WriteMessage(ctx, store.topicsTopicName, testKey, testValue)
	if err != nil {
		t.Fatalf("Direct write failed (hostname resolution issue): %v", err)
	}

	t.Log("Hostname resolution test passed - able to write to Kafka successfully")
}

func TestIntegration_SyncTopicFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	// Create a real metadata store
	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// First, create an actual topic in Kafka to sync
	client, err := NewRealKafkaClient([]string{broker}, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Create a test topic with multiple partitions
	testTopicName := "sync-flow-test-topic"
	err = client.CreateTopics(kafka.TopicConfig{
		Topic:             testTopicName,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("Failed to create test topic: %v", err)
	}

	// Give Kafka a moment to create the topic
	time.Sleep(2 * time.Second)

	// Now simulate the SyncTopic flow that was failing in production
	// Step 1: Read topic metadata from Kafka (like SyncTopic does)
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(testTopicName)
	if err != nil {
		t.Fatalf("Failed to read partitions: %v", err)
	}

	if len(partitions) == 0 {
		t.Fatalf("Expected partitions for topic %s, got 0", testTopicName)
	}

	t.Logf("Successfully read %d partitions from Kafka", len(partitions))

	// Step 2: Build partition metadata (like SyncTopic does)
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

		if replicationFactor == 0 {
			replicationFactor = len(partition.Replicas)
		}
	}

	// Step 3: Create topic metadata (like SyncTopic does)
	topicMetadata := &models.TopicMetadata{
		ClusterID:         "sync-test-cluster",
		TopicName:         testTopicName,
		Partitions:        partitionMetadata,
		ReplicationFactor: replicationFactor,
		Configs:           make(map[string]string),
	}

	// Step 4: Save to store (THIS IS WHERE IT WAS FAILING)
	// This should work now with our hostname resolution fixes
	t.Log("Attempting to save topic metadata to store (previously failing step)...")
	err = store.SaveTopic(ctx, topicMetadata)
	if err != nil {
		t.Fatalf("Failed to save topic metadata (the bug we're fixing): %v", err)
	}

	t.Log("Successfully saved topic metadata!")

	// Step 5: Verify the topic was saved and can be retrieved
	retrieved, err := store.GetTopic("sync-test-cluster", testTopicName)
	if err != nil {
		t.Fatalf("Failed to retrieve saved topic: %v", err)
	}

	if retrieved.TopicName != testTopicName {
		t.Errorf("Expected topic name %s, got %s", testTopicName, retrieved.TopicName)
	}

	if len(retrieved.Partitions) != len(partitionMetadata) {
		t.Errorf("Expected %d partitions, got %d", len(partitionMetadata), len(retrieved.Partitions))
	}

	t.Logf("Full SyncTopic flow test passed! Topic %s synced successfully", testTopicName)
}

func TestIntegration_MultipleClustersSync(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	broker, cleanup := SetupTestKafka(t, ctx)
	defer cleanup()

	store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
	defer storeCleanup()

	// Simulate syncing multiple clusters (common in production)
	clusters := []struct {
		id   string
		name string
	}{
		{"cluster-1", "Production Cluster 1"},
		{"cluster-2", "Production Cluster 2"},
		{"cluster-3", "Staging Cluster"},
	}

	for _, cluster := range clusters {
		clusterMeta := &models.ClusterMetadata{
			ID:            cluster.id,
			Name:          cluster.name,
			BootstrapURLs: []string{broker},
			BrokerCount:   1,
			TopicCount:    0,
		}

		err := store.SaveCluster(ctx, clusterMeta)
		if err != nil {
			t.Fatalf("Failed to save cluster %s: %v", cluster.id, err)
		}
	}

	// Verify all clusters were saved
	allClusters := store.ListClusters()
	if len(allClusters) != len(clusters) {
		t.Errorf("Expected %d clusters, got %d", len(clusters), len(allClusters))
	}

	// Now sync topics for each cluster
	for _, cluster := range clusters {
		topic := &models.TopicMetadata{
			ClusterID:         cluster.id,
			TopicName:         fmt.Sprintf("topic-in-%s", cluster.id),
			ReplicationFactor: 1,
			Partitions: []models.PartitionMetadata{
				{ID: 0, Leader: 0, Replicas: []int{0}, ISRs: []int{0}},
			},
		}

		err := store.SaveTopic(ctx, topic)
		if err != nil {
			t.Fatalf("Failed to save topic for cluster %s: %v", cluster.id, err)
		}
	}

	// Verify all topics were saved
	allTopics := store.ListTopics("")
	if len(allTopics) != len(clusters) {
		t.Errorf("Expected %d topics, got %d", len(clusters), len(allTopics))
	}

	t.Log("Multi-cluster sync test passed!")
}
