// Command local-kafka-agent is the Cluster Provider agent for Feature 1: it
// connects to Franz, watches its cluster assignments, and brings an
// apache/kafka KRaft broker up in Docker on the local machine per the
// local-docker recipe (ADR 004-local-kafka-docker-agent).
//
//	FRANZ_ENDPOINT   Franz gRPC address           (default localhost:9090)
//	FRANZ_TOKEN      bearer token from CreateAgent (required)
//	FRANZ_AGENT_NAME registered agent name         (default local-kafka-agent)
//	FRANZ_KAFKA_VERSION  apache/kafka tag default  (default 3.7.0)
//	DOCKER_HOST      Docker Engine socket          (default: SDK/platform default)
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := localkafka.LoadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}

	agent, err := localkafka.NewAgent(cfg, log)
	if err != nil {
		log.Error("startup", "err", err)
		os.Exit(1)
	}
	defer agent.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := agent.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
	log.Info("agent stopped")
}
