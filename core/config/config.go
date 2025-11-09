package config

import (
	"os"
	"strings"
)

// Config holds the application configuration
type Config struct {
	Server  ServerConfig
	Storage StorageConfig
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Port string
	Host string
}

// StorageConfig holds Kafka storage configuration
// Franz uses Kafka compacted topics to store cluster and topic definitions
type StorageConfig struct {
	Brokers                []string
	ClustersTopicName      string
	DefinitionsTopicName   string
	ClusterTopicsTopicName string
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

	// Storage Kafka brokers (where Franz stores configuration data)
	storageBrokersEnv := os.Getenv("STORAGE_KAFKA_BROKERS")
	if storageBrokersEnv == "" {
		// Fallback to legacy names
		storageBrokersEnv = os.Getenv("PRIMARY_KAFKA_BROKERS")
	}
	if storageBrokersEnv == "" {
		storageBrokersEnv = os.Getenv("KAFKA_BROKERS")
	}
	if storageBrokersEnv == "" {
		storageBrokersEnv = "localhost:19092"
	}

	storageBrokers := []string{}
	for _, broker := range strings.Split(storageBrokersEnv, ",") {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			storageBrokers = append(storageBrokers, broker)
		}
	}

	// Storage topic names for the three compacted topics
	clustersTopicName := os.Getenv("STORAGE_CLUSTERS_TOPIC")
	if clustersTopicName == "" {
		clustersTopicName = "franz.clusters.store"
	}

	definitionsTopicName := os.Getenv("STORAGE_DEFINITIONS_TOPIC")
	if definitionsTopicName == "" {
		definitionsTopicName = "franz.topic.definitions.store"
	}

	clusterTopicsTopicName := os.Getenv("STORAGE_CLUSTER_TOPICS_TOPIC")
	if clusterTopicsTopicName == "" {
		clusterTopicsTopicName = "franz.cluster.topics.store"
	}

	return &Config{
		Server: ServerConfig{
			Port: port,
			Host: host,
		},
		Storage: StorageConfig{
			Brokers:                storageBrokers,
			ClustersTopicName:      clustersTopicName,
			DefinitionsTopicName:   definitionsTopicName,
			ClusterTopicsTopicName: clusterTopicsTopicName,
		},
	}
}
