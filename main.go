package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/franz-kafka/server/core/config"
	"github.com/franz-kafka/server/core/handlers"
	"github.com/franz-kafka/server/core/kafka"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Initialize Kafka admin client
	admin, err := kafka.NewAdmin(cfg.Kafka.Brokers)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka admin client: %v", err)
	}
	defer admin.Close()

	// Initialize handlers
	h := handlers.NewHandler(admin)

	// Setup routes
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.HealthCheck)
	mux.HandleFunc("/api/topics", h.GetTopics)
	mux.HandleFunc("/api/topics/", h.GetTopic)
	mux.HandleFunc("/api/brokers", h.GetBrokers)
	mux.HandleFunc("/api/cluster", h.GetClusterMetadata)

	// Create HTTP server
	addr := fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Server starting on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
