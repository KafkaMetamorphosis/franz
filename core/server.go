package core

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
	"github.com/franz-kafka/server/core/store"
	"github.com/gorilla/mux"
)

func StartServer() {
	config := config.Load()

	kafkaAdmin, err := kafka.NewAdmin(config.Storage.BootstrapURLs)

	if err != nil {
		log.Fatalf("Error starting Kafka Admi %v", err)
	}

	storage, err := store.NewStore(config.Storage)

	log.Printf("[Server] Storage Kafka ConfigStore: %v", config.Storage)

	if err != nil {
		log.Fatalf("[Server] Failed to initialize configuration store: %v", err)
	}
	defer storage.Close()

	handler := handlers.NewHandler(kafkaAdmin, storage, config)

	routes := NewRoutes(handler)

	httpServerReliableStart(config.Server.Host, config.Server.Port, routes.Router)
}

func httpServerReliableStart(address, port string, router *mux.Router) {
	addr := fmt.Sprintf("%s:%s", address, port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second, //TODO: transform in config
		WriteTimeout: 15 * time.Second, //TODO: transform in config
		IdleTimeout:  60 * time.Second, //TODO: transform in config
	}

	go func() {
		log.Printf("Starting HTTP Server on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// // Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down HTTP server...")

	// // Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
}
