// Package localkafka is the local-kafka-docker-agent (deliverable 07, ADR
// 004-local-kafka-docker-agent): a Cluster Provider agent that turns a Kafka
// Cluster registration into a running KRaft broker in Docker on the local
// machine, keeps it converged, and reports status. It is a deliberately simple
// agent — plain packages, no hexagonal layering, no fx (002-monorepo-structure).
package localkafka

import (
	"fmt"
	"os"
	"time"
)

// Config is the agent's runtime configuration, all from the environment.
type Config struct {
	// Endpoint is the Franz gRPC address, e.g. "localhost:9090".
	Endpoint string
	// Token is the agent's bearer token from CreateAgent.
	Token string
	// DockerHost overrides the Docker Engine socket (default: the SDK's env /
	// platform default, usually unix:///var/run/docker.sock).
	DockerHost string
	// AgentName is the registered agent's name — used for the
	// franz.managed-by container label. Optional; falls back to "local-kafka-agent".
	AgentName string
	// KafkaVersionDefault is the apache/kafka tag used when a cluster does not
	// set franz.provisioning/kafka-version.
	KafkaVersionDefault string
	// ReconnectBackoff caps the stream reconnect backoff.
	ReconnectBackoffMax time.Duration
	// ResyncInterval forces a periodic reconcile even without a stream delta, so
	// drift (a container someone stopped by hand) is corrected.
	ResyncInterval time.Duration
}

// LoadConfig reads the agent config from the environment. The agent's
// registration is assumed to already exist (seeded — see local/seed/, or
// created in the console); FRANZ_TOKEN is its bearer token.
func LoadConfig() (Config, error) {
	c := Config{
		Endpoint:            env("FRANZ_ENDPOINT", "localhost:9090"),
		Token:               os.Getenv("FRANZ_TOKEN"),
		DockerHost:          os.Getenv("DOCKER_HOST"),
		AgentName:           env("FRANZ_AGENT_NAME", "local-kafka-agent"),
		KafkaVersionDefault: env("FRANZ_KAFKA_VERSION", "3.7.0"),
		ReconnectBackoffMax: 30 * time.Second,
		ResyncInterval:      60 * time.Second,
	}
	if c.Token == "" {
		return Config{}, fmt.Errorf("FRANZ_TOKEN is required (the agent's bearer token)")
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
