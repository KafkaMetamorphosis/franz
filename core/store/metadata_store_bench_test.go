package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/franz-kafka/server/core/models"
)

// Run benchmarks with: go test -bench=. ./core/store -benchmem

func BenchmarkSaveCluster(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	cluster := &models.ClusterMetadata{
		ID:            "bench-cluster",
		Name:          "Benchmark Cluster",
		BootstrapURLs: []string{"localhost:9092"},
		BrokerCount:   3,
		TopicCount:    10,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster.ID = fmt.Sprintf("bench-cluster-%d", i)
		store.SaveCluster(ctx, cluster)
	}
}

func BenchmarkSaveTopic(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	topic := &models.TopicMetadata{
		ClusterID:         "bench-cluster",
		TopicName:         "bench-topic",
		ReplicationFactor: 3,
		Partitions: []models.PartitionMetadata{
			{ID: 0, Leader: 0, Replicas: []int{0, 1, 2}, ISRs: []int{0, 1, 2}},
			{ID: 1, Leader: 1, Replicas: []int{1, 2, 0}, ISRs: []int{1, 2, 0}},
			{ID: 2, Leader: 2, Replicas: []int{2, 0, 1}, ISRs: []int{2, 0, 1}},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.TopicName = fmt.Sprintf("bench-topic-%d", i)
		store.SaveTopic(ctx, topic)
	}
}

func BenchmarkGetCluster(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	// Prepare test data
	cluster := &models.ClusterMetadata{
		ID:            "bench-cluster",
		Name:          "Benchmark Cluster",
		BootstrapURLs: []string{"localhost:9092"},
		BrokerCount:   3,
		TopicCount:    10,
	}
	store.SaveCluster(ctx, cluster)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetCluster("bench-cluster")
	}
}

func BenchmarkGetTopic(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	// Prepare test data
	topic := &models.TopicMetadata{
		ClusterID:         "bench-cluster",
		TopicName:         "bench-topic",
		ReplicationFactor: 3,
		Partitions: []models.PartitionMetadata{
			{ID: 0, Leader: 0, Replicas: []int{0, 1, 2}, ISRs: []int{0, 1, 2}},
		},
	}
	store.SaveTopic(ctx, topic)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetTopic("bench-cluster", "bench-topic")
	}
}

func BenchmarkListClusters(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	// Prepare test data - 100 clusters
	for i := 0; i < 100; i++ {
		cluster := &models.ClusterMetadata{
			ID:            fmt.Sprintf("bench-cluster-%d", i),
			Name:          fmt.Sprintf("Benchmark Cluster %d", i),
			BootstrapURLs: []string{"localhost:9092"},
			BrokerCount:   3,
			TopicCount:    10,
		}
		store.SaveCluster(ctx, cluster)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.ListClusters()
	}
}

func BenchmarkListTopics(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	// Prepare test data - 100 topics
	for i := 0; i < 100; i++ {
		topic := &models.TopicMetadata{
			ClusterID:         "bench-cluster",
			TopicName:         fmt.Sprintf("bench-topic-%d", i),
			ReplicationFactor: 3,
			Partitions: []models.PartitionMetadata{
				{ID: 0, Leader: 0, Replicas: []int{0, 1, 2}, ISRs: []int{0, 1, 2}},
			},
		}
		store.SaveTopic(ctx, topic)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.ListTopics("")
	}
}

func BenchmarkListTopicsFiltered(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	// Prepare test data - 100 topics across 5 clusters
	for clusterID := 0; clusterID < 5; clusterID++ {
		for topicID := 0; topicID < 20; topicID++ {
			topic := &models.TopicMetadata{
				ClusterID:         fmt.Sprintf("cluster-%d", clusterID),
				TopicName:         fmt.Sprintf("topic-%d", topicID),
				ReplicationFactor: 3,
				Partitions: []models.PartitionMetadata{
					{ID: 0, Leader: 0, Replicas: []int{0, 1, 2}, ISRs: []int{0, 1, 2}},
				},
			}
			store.SaveTopic(ctx, topic)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.ListTopics("cluster-2")
	}
}

func BenchmarkConcurrentReads(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	// Prepare test data
	for i := 0; i < 10; i++ {
		cluster := &models.ClusterMetadata{
			ID:            fmt.Sprintf("cluster-%d", i),
			Name:          fmt.Sprintf("Cluster %d", i),
			BootstrapURLs: []string{"localhost:9092"},
			BrokerCount:   3,
			TopicCount:    10,
		}
		store.SaveCluster(ctx, cluster)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			store.GetCluster(fmt.Sprintf("cluster-%d", i%10))
			i++
		}
	})
}

func BenchmarkConcurrentWrites(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cluster := &models.ClusterMetadata{
				ID:            fmt.Sprintf("parallel-cluster-%d", i),
				Name:          fmt.Sprintf("Parallel Cluster %d", i),
				BootstrapURLs: []string{"localhost:9092"},
				BrokerCount:   3,
				TopicCount:    10,
			}
			store.SaveCluster(ctx, cluster)
			i++
		}
	})
}

func BenchmarkLargeTopicPayload(b *testing.B) {
	mockClient := NewMockKafkaClient()
	store, _ := NewMetadataStoreWithClient([]string{"localhost:9092"}, "clusters", "topics", mockClient)
	ctx := context.Background()

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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.TopicName = fmt.Sprintf("large-topic-%d", i)
		store.SaveTopic(ctx, topic)
	}
}
