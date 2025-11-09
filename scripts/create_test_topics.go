package main

import (
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Define test topics to create on each cluster
	testTopics := []string{
		"test-topic-1",
		"test-topic-2",
		"orders",
		"users",
		"events",
	}

	// Define clusters
	clusters := map[string]string{
		"kafka-fleet": "localhost:19092",
		"kafka-2":     "localhost:29092",
		"kafka-3":     "localhost:39092",
	}

	// Create topics on each cluster
	for clusterName, broker := range clusters {
		log.Printf("\n=== Creating topics on %s (%s) ===", clusterName, broker)

		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Printf("ERROR: Failed to connect to %s: %v", clusterName, err)
			continue
		}
		defer conn.Close()

		controller, err := conn.Controller()
		if err != nil {
			log.Printf("ERROR: Failed to get controller for %s: %v", clusterName, err)
			continue
		}

		// Try to connect to controller, fallback to original broker
		controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
		if err != nil {
			log.Printf("WARNING: Could not connect to controller, using original broker")
			controllerConn = conn
		} else {
			defer controllerConn.Close()
		}

		// Create each topic
		for _, topicName := range testTopics {
			topicConfigs := []kafka.TopicConfig{
				{
					Topic:             topicName,
					NumPartitions:     3,
					ReplicationFactor: 1,
				},
			}

			err = controllerConn.CreateTopics(topicConfigs...)
			if err != nil {
				log.Printf("  - Topic %s: %v", topicName, err)
			} else {
				log.Printf("  ✓ Created topic: %s (3 partitions, RF=1)", topicName)
			}

			// Small delay between topic creations
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Verify topics were created
	log.Printf("\n=== Verifying topics ===")
	time.Sleep(1 * time.Second) // Wait for topic creation to propagate

	for clusterName, broker := range clusters {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			log.Printf("ERROR: Failed to connect to %s: %v", clusterName, err)
			continue
		}
		defer conn.Close()

		partitions, err := conn.ReadPartitions()
		if err != nil {
			log.Printf("ERROR: Failed to read partitions from %s: %v", clusterName, err)
			continue
		}

		topicMap := make(map[string]int)
		for _, p := range partitions {
			topicMap[p.Topic]++
		}

		log.Printf("\n%s (%s) - %d topics:", clusterName, broker, len(topicMap))
		for topic, partCount := range topicMap {
			log.Printf("  - %s (%d partitions)", topic, partCount)
		}
	}

	log.Printf("\n=== Done! ===")
}
