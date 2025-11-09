package config

import (
	"os"
	"strings"
)

// Config holds the application configuration
type Config struct {
	Server       ServerConfig
	Kafka        KafkaConfig
	PrimaryKafka PrimaryKafkaConfig
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Port string
	Host string
}

// KafkaConfig holds Kafka broker configuration
type KafkaConfig struct {
	Brokers []string
}

// PrimaryKafkaConfig holds the primary Kafka DB configuration
type PrimaryKafkaConfig struct {
	Brokers           []string
	ClustersTopicName string
	TopicsTopicName   string
}

// Load reads configuration from environment variables
func Load() *Config {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	host := os.Getenv("SERVER_HOST")
	if host == "" {
		host = "0.0.0.0"
	}

	// Support both simple and legacy environment variable names
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = os.Getenv("KAFKA_FLEET_BOOTSTRAP_URLS")
	}
	if brokersEnv == "" {
		brokersEnv = "localhost:19092"
	}

	// Split comma-separated brokers
	brokers := []string{}
	for _, broker := range strings.Split(brokersEnv, ",") {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	// Primary Kafka DB configuration
	// Support both simple and legacy environment variable names
	primaryBrokersEnv := os.Getenv("PRIMARY_KAFKA_BROKERS")
	if primaryBrokersEnv == "" {
		primaryBrokersEnv = os.Getenv("KAFKA_PRIMARY_DB_BOOTSTRAP_URLS")
	}
	if primaryBrokersEnv == "" {
		primaryBrokersEnv = "localhost:19092"
	}

	primaryBrokers := []string{}
	for _, broker := range strings.Split(primaryBrokersEnv, ",") {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			primaryBrokers = append(primaryBrokers, broker)
		}
	}

	// Support both simple and legacy environment variable names for topic names
	clustersTopicName := os.Getenv("CLUSTERS_TOPIC_NAME")
	if clustersTopicName == "" {
		clustersTopicName = os.Getenv("KAFKA_METADATA_CLUSTERS_TOPIC")
	}
	if clustersTopicName == "" {
		clustersTopicName = "franz.metadata.clusters"
	}

	topicsTopicName := os.Getenv("TOPICS_TOPIC_NAME")
	if topicsTopicName == "" {
		topicsTopicName = os.Getenv("KAFKA_METADATA_TOPICS_TOPIC")
	}
	if topicsTopicName == "" {
		topicsTopicName = "franz.metadata.topics"
	}

	return &Config{
		Server: ServerConfig{
			Port: port,
			Host: host,
		},
		Kafka: KafkaConfig{
			Brokers: brokers,
		},
		PrimaryKafka: PrimaryKafkaConfig{
			Brokers:           primaryBrokers,
			ClustersTopicName: clustersTopicName,
			TopicsTopicName:   topicsTopicName,
		},
	}
}
