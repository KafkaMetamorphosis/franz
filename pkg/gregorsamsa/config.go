// Package gregorsamsa is the Resource Provider agent (deliverable 12, ADR
// 005-gregor-samsa): it turns the intent declared on an Async Channel in Franz
// into real Kafka topics on every cluster in its label scope, and streams
// structural telemetry about those topics and clusters back to the control
// plane. It is a deliberately simple agent — plain packages, no hexagonal
// layering, no fx (002-monorepo-structure).
package gregorsamsa

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the agent's runtime configuration, all from the environment.
type Config struct {
	// Endpoint is the Franz gRPC address, e.g. "localhost:9090".
	Endpoint string
	// Token is the agent's bearer token from CreateAgent. Its registration
	// carries the franz.placement-selector/* labels that decide this instance's
	// scope; the agent itself evaluates nothing.
	Token string
	// AgentName is the registered agent's name, for logs and telemetry
	// attribution. Franz resolves the real identity from the token.
	AgentName string

	// ReconnectBackoffMin / Max bound the stream reconnect backoff
	// (005 ADR §1.6: 5s → 120s).
	ReconnectBackoffMin time.Duration
	ReconnectBackoffMax time.Duration
	// Debounce is how long to let a burst of stream messages settle before
	// reconciling, so the full set on open costs one pass.
	Debounce time.Duration
	// TelemetryInterval is the full-sweep cadence (005 ADR §2.2, default 60s).
	// Zero disables the telemetry loop entirely.
	TelemetryInterval time.Duration
	// ReportTimeout bounds one ReportPartitionReconciliation call.
	ReportTimeout time.Duration
}

// LoadConfig reads the agent config from the environment. The agent's
// registration is assumed to already exist (seeded — see local/seed/, or created
// in the console); FRANZ_TOKEN is its bearer token.
func LoadConfig() (Config, error) {
	c := Config{
		Endpoint:            env("FRANZ_ENDPOINT", "localhost:9090"),
		Token:               os.Getenv("FRANZ_TOKEN"),
		AgentName:           env("FRANZ_AGENT_NAME", "gregor-samsa"),
		ReconnectBackoffMin: 5 * time.Second,
		ReconnectBackoffMax: 120 * time.Second,
		Debounce:            500 * time.Millisecond,
		TelemetryInterval:   60 * time.Second,
		ReportTimeout:       10 * time.Second,
	}
	if c.Token == "" {
		return Config{}, fmt.Errorf("FRANZ_TOKEN is required (the agent's bearer token)")
	}

	interval, err := envDuration("FRANZ_TELEMETRY_INTERVAL", c.TelemetryInterval)
	if err != nil {
		return Config{}, err
	}
	c.TelemetryInterval = interval

	backoffMax, err := envDuration("FRANZ_RECONNECT_BACKOFF_MAX", c.ReconnectBackoffMax)
	if err != nil {
		return Config{}, err
	}
	c.ReconnectBackoffMax = backoffMax
	if c.ReconnectBackoffMin > c.ReconnectBackoffMax {
		c.ReconnectBackoffMin = c.ReconnectBackoffMax
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envDuration reads a Go duration ("30s", "2m") or a bare number of seconds.
func envDuration(key string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a duration (e.g. \"60s\") or a number of seconds", key, raw)
	}
	return time.Duration(seconds) * time.Second, nil
}
