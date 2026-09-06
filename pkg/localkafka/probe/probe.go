// Package probe decides READY vs DEGRADED for a provisioned broker (ADR 004
// OQ2): a franz-go Ping (ApiVersions round-trip) against the advertised
// bootstrap address.
package probe

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Result is the outcome of a readiness probe.
type Result struct {
	Ready   bool
	Message string
}

// Broker probes bootstrapURL. Ready is true when the broker answers a Kafka
// protocol request (ApiVersions) within the timeout.
func Broker(ctx context.Context, bootstrapURL string) Result {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapURL),
		kgo.DialTimeout(3*time.Second),
	)
	if err != nil {
		return Result{Message: fmt.Sprintf("client init: %v", err)}
	}
	defer cl.Close()

	if err := cl.Ping(ctx); err != nil {
		return Result{Message: fmt.Sprintf("broker not answering: %v", err)}
	}
	return Result{Ready: true, Message: "broker answering on " + bootstrapURL}
}
