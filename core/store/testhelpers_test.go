package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

// SetupTestKafka starts a Kafka container for integration tests
func SetupTestKafka(t *testing.T, ctx context.Context) (string, func()) {
	t.Helper()

	// Start Kafka container
	kafkaContainer, err := kafka.Run(ctx,
		"apache/kafka:latest",
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		t.Fatalf("Failed to start Kafka container: %v", err)
	}

	// Get bootstrap servers
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		kafkaContainer.Terminate(ctx)
		t.Fatalf("Failed to get Kafka brokers: %v", err)
	}

	if len(brokers) == 0 {
		kafkaContainer.Terminate(ctx)
		t.Fatal("No Kafka brokers returned")
	}

	broker := brokers[0]

	// Wait for Kafka to be ready
	if err := waitForKafka(broker, 30*time.Second); err != nil {
		kafkaContainer.Terminate(ctx)
		t.Fatalf("Kafka not ready: %v", err)
	}

	// Cleanup function
	cleanup := func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Kafka container: %v", err)
		}
	}

	return broker, cleanup
}

// waitForKafka waits for Kafka to be ready
func waitForKafka(broker string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		client, err := NewRealKafkaClient([]string{broker}, 5*time.Second)
		if err == nil {
			client.Close()
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("Kafka not ready after %v", timeout)
}

// SetupTestMetadataStore creates a metadata store for testing
func SetupTestMetadataStore(t *testing.T, ctx context.Context, broker string) (*MetadataStore, func()) {
	t.Helper()

	store, err := NewMetadataStore(
		[]string{broker},
		"test-clusters",
		"test-topics",
	)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}

	cleanup := func() {
		if store.client != nil {
			store.client.Close()
		}
	}

	return store, cleanup
}
