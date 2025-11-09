package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/franz-kafka/server/core/models"
	"github.com/segmentio/kafka-go"
)

func TestNewMetadataStore_EmptyBrokers(t *testing.T) {
	mockClient := NewMockKafkaClient()

	_, err := NewMetadataStoreWithClient([]string{}, "clusters", "topics", mockClient)
	if err == nil {
		t.Error("Expected error for empty brokers, got nil")
	}
}

func TestNewMetadataStore_Success(t *testing.T) {
	mockClient := NewMockKafkaClient()

	store, err := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if store == nil {
		t.Fatal("Expected store to be created, got nil")
	}

	// Verify topics were created
	if mockClient.CreateCalls != 2 {
		t.Errorf("Expected 2 create calls, got %d", mockClient.CreateCalls)
	}
}

func TestSaveCluster_Success(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	cluster := &models.ClusterMetadata{
		ID:            "test-cluster",
		Name:          "Test Cluster",
		BootstrapURLs: []string{"localhost:9092"},
		BrokerCount:   3,
		TopicCount:    5,
	}

	ctx := context.Background()
	err := store.SaveCluster(ctx, cluster)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the message was written
	if mockClient.WriteCalls != 1 {
		t.Errorf("Expected 1 write call, got %d", mockClient.WriteCalls)
	}

	// Verify in-memory cache
	retrieved, err := store.GetCluster("test-cluster")
	if err != nil {
		t.Fatalf("Expected no error retrieving cluster, got %v", err)
	}

	if retrieved.ID != cluster.ID {
		t.Errorf("Expected cluster ID %s, got %s", cluster.ID, retrieved.ID)
	}
}

func TestSaveCluster_WriteError(t *testing.T) {
	mockClient := NewMockKafkaClient()
	mockClient.WriteError = fmt.Errorf("write error")

	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	cluster := &models.ClusterMetadata{
		ID:   "test-cluster",
		Name: "Test Cluster",
	}

	ctx := context.Background()
	err := store.SaveCluster(ctx, cluster)
	if err == nil {
		t.Error("Expected error on write failure, got nil")
	}
}

func TestSaveTopic_Success(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	topic := &models.TopicMetadata{
		ClusterID:         "test-cluster",
		TopicName:         "test-topic",
		ReplicationFactor: 3,
		Partitions: []models.PartitionMetadata{
			{ID: 0, Leader: 1, Replicas: []int{1, 2, 3}, ISRs: []int{1, 2, 3}},
		},
	}

	ctx := context.Background()
	err := store.SaveTopic(ctx, topic)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the message was written
	if mockClient.WriteCalls != 1 {
		t.Errorf("Expected 1 write call, got %d", mockClient.WriteCalls)
	}

	// Verify in-memory cache
	retrieved, err := store.GetTopic("test-cluster", "test-topic")
	if err != nil {
		t.Fatalf("Expected no error retrieving topic, got %v", err)
	}

	if retrieved.TopicName != topic.TopicName {
		t.Errorf("Expected topic name %s, got %s", topic.TopicName, retrieved.TopicName)
	}
}

func TestGetCluster_NotFound(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	_, err := store.GetCluster("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent cluster, got nil")
	}
}

func TestGetTopic_NotFound(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	_, err := store.GetTopic("cluster", "nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent topic, got nil")
	}
}

func TestListClusters_Empty(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	clusters := store.ListClusters()
	if len(clusters) != 0 {
		t.Errorf("Expected 0 clusters, got %d", len(clusters))
	}
}

func TestListClusters_Multiple(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	ctx := context.Background()

	// Add multiple clusters with valid data
	for i := 0; i < 3; i++ {
		cluster := &models.ClusterMetadata{
			ID:            fmt.Sprintf("cluster-%d", i),
			Name:          fmt.Sprintf("Cluster %d", i),
			BootstrapURLs: []string{"localhost:9092"},
		}
		store.SaveCluster(ctx, cluster)
	}

	clusters := store.ListClusters()
	if len(clusters) != 3 {
		t.Errorf("Expected 3 clusters, got %d", len(clusters))
	}
}

func TestListTopics_Empty(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	topics := store.ListTopics("")
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics, got %d", len(topics))
	}
}

func TestListTopics_FilteredByCluster(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	ctx := context.Background()

	// Add topics for different clusters
	store.SaveTopic(ctx, &models.TopicMetadata{
		ClusterID: "cluster-1",
		TopicName: "topic-a",
	})
	store.SaveTopic(ctx, &models.TopicMetadata{
		ClusterID: "cluster-1",
		TopicName: "topic-b",
	})
	store.SaveTopic(ctx, &models.TopicMetadata{
		ClusterID: "cluster-2",
		TopicName: "topic-c",
	})

	// Get all topics
	allTopics := store.ListTopics("")
	if len(allTopics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(allTopics))
	}

	// Get cluster-1 topics only
	cluster1Topics := store.ListTopics("cluster-1")
	if len(cluster1Topics) != 2 {
		t.Errorf("Expected 2 topics for cluster-1, got %d", len(cluster1Topics))
	}
}

func TestConcurrentAccess(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	ctx := context.Background()
	var wg sync.WaitGroup

	// Concurrent writes with valid data
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cluster := &models.ClusterMetadata{
				ID:            fmt.Sprintf("cluster-%d", id),
				Name:          fmt.Sprintf("Cluster %d", id),
				BootstrapURLs: []string{"localhost:9092"},
			}
			store.SaveCluster(ctx, cluster)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.ListClusters()
		}()
	}

	wg.Wait()

	// Verify all clusters were saved
	clusters := store.ListClusters()
	if len(clusters) != 10 {
		t.Errorf("Expected 10 clusters after concurrent writes, got %d", len(clusters))
	}
}

func TestLoadData_WithExistingData(t *testing.T) {
	mockClient := NewMockKafkaClient()

	// Prepare existing data in mock
	cluster1 := &models.ClusterMetadata{
		ID:   "cluster-1",
		Name: "Cluster 1",
	}
	cluster1Data, _ := json.Marshal(cluster1)

	mockClient.Messages["clusters"] = []kafka.Message{
		{Key: []byte("cluster-1"), Value: cluster1Data},
	}

	topic1 := &models.TopicMetadata{
		ClusterID: "cluster-1",
		TopicName: "topic-1",
	}
	topic1Data, _ := json.Marshal(topic1)

	mockClient.Messages["topics"] = []kafka.Message{
		{Key: []byte("cluster-1:topic-1"), Value: topic1Data},
	}

	// Create store (will load data)
	store, err := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify data was loaded
	clusters := store.ListClusters()
	if len(clusters) != 1 {
		t.Errorf("Expected 1 cluster after loading, got %d", len(clusters))
	}

	topics := store.ListTopics("")
	if len(topics) != 1 {
		t.Errorf("Expected 1 topic after loading, got %d", len(topics))
	}
}

func TestLoadData_TombstoneHandling(t *testing.T) {
	mockClient := NewMockKafkaClient()

	// Add a cluster
	cluster1 := &models.ClusterMetadata{
		ID:   "cluster-1",
		Name: "Cluster 1",
	}
	cluster1Data, _ := json.Marshal(cluster1)

	// Tombstone (delete marker) - nil value
	mockClient.Messages["clusters"] = []kafka.Message{
		{Key: []byte("cluster-1"), Value: cluster1Data},
		{Key: []byte("cluster-1"), Value: nil}, // Tombstone
	}

	store, err := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify cluster was deleted by tombstone
	clusters := store.ListClusters()
	if len(clusters) != 0 {
		t.Errorf("Expected 0 clusters after tombstone, got %d", len(clusters))
	}
}

func TestLastSyncTime_Updated(t *testing.T) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)

	cluster := &models.ClusterMetadata{
		ID:            "test-cluster",
		Name:          "Test Cluster",
		BootstrapURLs: []string{"localhost:9092"},
	}

	ctx := context.Background()
	before := time.Now()

	err := store.SaveCluster(ctx, cluster)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	after := time.Now()

	// Verify LastSyncTime was set
	if cluster.LastSyncTime.Before(before) || cluster.LastSyncTime.After(after) {
		t.Errorf("LastSyncTime not set correctly: %v", cluster.LastSyncTime)
	}
}

// Helper function for tests - creates store with mock client
func NewMetadataStoreWithClient(brokers []string, clustersTopicName, topicsTopicName string, client KafkaClient) (*MetadataStore, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	store := &MetadataStore{
		brokers:           brokers,
		clustersTopicName: clustersTopicName,
		topicsTopicName:   topicsTopicName,
		clusters:          make(map[string]*models.ClusterMetadata),
		topics:            make(map[string]*models.TopicMetadata),
		client:            client,
	}

	// Initialize topics
	if err := store.initializeTopicsWithClient(client); err != nil {
		return nil, fmt.Errorf("failed to initialize topics: %w", err)
	}

	// Load existing data
	if err := store.loadDataWithClient(client); err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
	}

	return store, nil
}

func (s *MetadataStore) initializeTopicsWithClient(client KafkaClient) error {
	// Create clusters topic
	err := client.CreateTopics(kafka.TopicConfig{
		Topic:             s.clustersTopicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "cleanup.policy", ConfigValue: "compact"},
		},
	})
	if err != nil && !isTopicExistsError(err) {
		return fmt.Errorf("failed to create clusters topic: %w", err)
	}

	// Create topics topic
	err = client.CreateTopics(kafka.TopicConfig{
		Topic:             s.topicsTopicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "cleanup.policy", ConfigValue: "compact"},
		},
	})
	if err != nil && !isTopicExistsError(err) {
		return fmt.Errorf("failed to create topics topic: %w", err)
	}

	return nil
}

func (s *MetadataStore) loadDataWithClient(client KafkaClient) error {
	ctx := context.Background()

	// Load clusters
	clusterMsgs, err := client.ReadMessages(ctx, s.clustersTopicName, 0)
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
	topicMsgs, err := client.ReadMessages(ctx, s.topicsTopicName, 0)
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

func isTopicExistsError(err error) bool {
	return err != nil && err.Error() == "already exists"
}
