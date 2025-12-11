package config

import (
	"os"
	"strconv"
)

type StoreTopicConfig struct {
	Topic             string
	Partitions        int
	ReplicationFactor int
}

type StoreConfig struct {
	BootstrapURLs            []string
	ClusterClaimsTopicConfig StoreTopicConfig
	DefinitionsTopicConfig   StoreTopicConfig
	TopicsClaimConfig        StoreTopicConfig
}

type HttpServerConfig struct {
	Port string
	Host string
}

type Config struct {
	Server  HttpServerConfig
	Storage StoreConfig
}

func getStringEnvOr(key, fallback string) string {
	if envVar, exists := os.LookupEnv(key); exists {
		return envVar
	}
	return fallback
}

func getIntEnvOr(key string, fallback int) int {
	if envVar, exists := os.LookupEnv(key); exists {
		if intVar, err := strconv.Atoi(envVar); err != nil {
			return intVar
		}
	}
	return fallback
}

func Load() *Config {
	port := getStringEnvOr("FRANZ_SERVER_PORT", "8080")
	host := getStringEnvOr("FRANZ_SERVER_HOST", "0.0.0.0")

	storageBootstrapUrls := []string{getStringEnvOr("FRANZ_STORAGE_KAFKA_BOOTSTRAP_URL", "kafka-fleet:19092")} // if you don't have kafka-fleet yet, config your /etc/hosts with "127.0.0.1 kafka-fleet"

	clustersTopicName := getStringEnvOr("FRANZ_STORAGE_CLUSTERS_NAME", "franz.cluster_claims.store")
	clustersTopicPartitions := getIntEnvOr("FRANZ_STORAGE_CLUSTERS_PARTITIONS", 1)
	clustersTopicReplicationFactor := getIntEnvOr("FRANZ_STORAGE_CLUSTERS_REPLICATION_FACTOR", 1)

	definitionsTopicName := getStringEnvOr("FRANZ_STORAGE_DEFINITIONS_NAME", "franz.topic.definitions.store")
	definitionsTopicPartitions := getIntEnvOr("FRANZ_STORAGE_DEFINITIONS_PARTITIONS", 1)
	definitionsTopicReplicationFactor := getIntEnvOr("FRANZ_STORAGE_DEFINITIONS_REPLICATION_FACTOR", 1)

	topicClaimsTopicName := getStringEnvOr("FRANZ_STORAGE_TOPIC_CLAIMS_NAME", "franz.cluster.topic.claims.store")
	topicClaimsTopicPartitions := getIntEnvOr("FRANZ_STORAGE_TOPIC_CLAIMS_PARTITIONS", 1)
	topicClaimsTopicReplicationFactor := getIntEnvOr("FRANZ_STORAGE_TOPIC_CLAIMS_REPLICATION_FACTOR", 1)

	return &Config{
		Server: HttpServerConfig{
			Port: port,
			Host: host,
		},
		Storage: StoreConfig{
			BootstrapURLs: storageBootstrapUrls,
			ClusterClaimsTopicConfig: StoreTopicConfig{
				Topic:             clustersTopicName,
				Partitions:        clustersTopicPartitions,
				ReplicationFactor: clustersTopicReplicationFactor,
			},
			DefinitionsTopicConfig: StoreTopicConfig{
				Topic:             definitionsTopicName,
				Partitions:        definitionsTopicPartitions,
				ReplicationFactor: definitionsTopicReplicationFactor,
			},
			TopicsClaimConfig: StoreTopicConfig{
				Topic:             topicClaimsTopicName,
				Partitions:        topicClaimsTopicPartitions,
				ReplicationFactor: topicClaimsTopicReplicationFactor,
			},
		},
	}
}
