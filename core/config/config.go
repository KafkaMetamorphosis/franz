package config

import (
	"os"
	"strings"
)

// Config holds the application configuration
type Config struct {
	Server ServerConfig
	Kafka  KafkaConfig
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

	brokersEnv := os.Getenv("KAFKA_FLEET_BOOTSTRAP_URLS")
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

	return &Config{
		Server: ServerConfig{
			Port: port,
			Host: host,
		},
		Kafka: KafkaConfig{
			Brokers: brokers,
		},
	}
}
