// Command gregorsamsa is the Resource Provider agent (ADR 005-gregor-samsa). It
// watches the async channel partitions Franz has label-scoped to it, reconciles
// the real Kafka topics to match, reports each outcome, and sweeps structural
// telemetry back to the control plane.
//
// Configuration is environment-only — see pkg/gregorsamsa.LoadConfig.
package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
	"github.com/KafkaMetamorphosis/franz/pkg/shared"
)

func main() {
	log := shared.NewLogger(os.Getenv("LOG_LEVEL"))

	cfg, err := gregorsamsa.LoadConfig()
	if err != nil {
		log.Error("configuration", "err", err)
		os.Exit(1)
	}

	agent, err := gregorsamsa.NewAgent(cfg, log, kafkaadmin.NewKadm)
	if err != nil {
		log.Error("startup", "err", err)
		os.Exit(1)
	}
	defer agent.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("agent stopped", "err", err)
		os.Exit(1)
	}
	log.Info("gregor samsa stopped")
}
