package main

import (
	"log"

	"github.com/franz-kafka/server/core"
)

func main() {
	log.Println("Starting Franz server ..")
	core.StartServer()
	// Initialize configuration store
	// configStore, err := store.NewStore(config.Storage)

	// log.Printf("[Main] Storage Kafka ConfigStore: %v", configStore)

	// if err != nil {
	// 	log.Fatalf("[Main] Failed to initialize configuration store: %v", err)
	// }
	// defer configStore.Close()

	// // brokers := []string{cfg.Storage.Brokers}
	// // admin, err := kafka.NewAdmin(brokers)

	// // Initialize handlers
	// configHandler := handlers.NewConfigHandler(configStore)
	// liveQueryHandler := handlers.NewLiveQueryHandler(configStore)

	// // Setup routes
	// mux := http.NewServeMux()

	// // Health endpoint
	// mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
	// 	w.Header().Set("Content-Type", "application/json")
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte(`{"status":"healthy"}`))
	// })

	// // Cluster management endpoints
	// mux.HandleFunc("/api/clusters", func(w http.ResponseWriter, r *http.Request) {
	// 	switch r.Method {
	// 	case http.MethodGet:
	// 		configHandler.ListClusters(w, r)
	// 	case http.MethodPost:
	// 		configHandler.CreateCluster(w, r)
	// 	default:
	// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	// 	}
	// })
	// mux.HandleFunc("/api/cluster/", func(w http.ResponseWriter, r *http.Request) {
	// 	// Check if it's a sub-route
	// 	if contains(r.URL.Path, "/topics") {
	// 		switch r.Method {
	// 		case http.MethodGet:
	// 			configHandler.ListClusterTopics(w, r)
	// 		case http.MethodPost:
	// 			configHandler.AddTopicToCluster(w, r)
	// 		default:
	// 			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	// 		}
	// 	} else if contains(r.URL.Path, "/topic/") {
	// 		switch r.Method {
	// 		case http.MethodGet:
	// 			configHandler.GetClusterTopic(w, r)
	// 		case http.MethodPut:
	// 			configHandler.UpdateClusterTopic(w, r)
	// 		case http.MethodDelete:
	// 			configHandler.RemoveTopicFromCluster(w, r)
	// 		default:
	// 			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	// 		}
	// 	} else if contains(r.URL.Path, "/live_topics") {
	// 		liveQueryHandler.GetLiveTopics(w, r)
	// 	} else if contains(r.URL.Path, "/live_topic/") {
	// 		liveQueryHandler.GetLiveTopic(w, r)
	// 	} else if contains(r.URL.Path, "/live_brokers") {
	// 		liveQueryHandler.GetLiveBrokers(w, r)
	// 	} else {
	// 		// It's a cluster CRUD operation
	// 		switch r.Method {
	// 		case http.MethodGet:
	// 			configHandler.GetCluster(w, r)
	// 		case http.MethodPut:
	// 			configHandler.UpdateCluster(w, r)
	// 		case http.MethodDelete:
	// 			configHandler.DeleteCluster(w, r)
	// 		default:
	// 			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	// 		}
	// 	}
	// })

	// // Topic definition endpoints
	// mux.HandleFunc("/api/topic_definitions", func(w http.ResponseWriter, r *http.Request) {
	// 	switch r.Method {
	// 	case http.MethodGet:
	// 		configHandler.ListTopicDefinitions(w, r)
	// 	case http.MethodPost:
	// 		configHandler.CreateTopicDefinition(w, r)
	// 	default:
	// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	// 	}
	// })
	// mux.HandleFunc("/api/topic_definition/", func(w http.ResponseWriter, r *http.Request) {
	// 	switch r.Method {
	// 	case http.MethodGet:
	// 		configHandler.GetTopicDefinition(w, r)
	// 	case http.MethodPut:
	// 		configHandler.UpdateTopicDefinition(w, r)
	// 	case http.MethodDelete:
	// 		configHandler.DeleteTopicDefinition(w, r)
	// 	default:
	// 		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	// 	}
	// })

	// // Create HTTP server
	// addr := fmt.Sprintf("%s:%s", config.Server.Host, config.Server.Port)
	// srv := &http.Server{
	// 	Addr:         addr,
	// 	Handler:      mux,
	// 	ReadTimeout:  15 * time.Second,
	// 	WriteTimeout: 15 * time.Second,
	// 	IdleTimeout:  60 * time.Second,
	// }

	// // Start server in a goroutine
	// go func() {
	// 	log.Printf("Server starting on %s", addr)
	// 	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
	// 		log.Fatalf("Server failed to start: %v", err)
	// 	}
	// }()

	// // Wait for interrupt signal to gracefully shutdown the server
	// quit := make(chan os.Signal, 1)
	// signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// <-quit

	// log.Println("Shutting down server...")

	// // Graceful shutdown with timeout
	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()

	// if err := srv.Shutdown(ctx); err != nil {
	// 	log.Fatalf("Server forced to shutdown: %v", err)
	// }

	// log.Println("Server exited")
}

// contains checks if a string contains a substring
// func contains(s, substr string) bool {
// 	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || findSubstring(s, substr))
// }

// func findSubstring(s, substr string) bool {
// 	for i := 0; i <= len(s)-len(substr); i++ {
// 		if s[i:i+len(substr)] == substr {
// 			return true
// 		}
// 	}
// 	return false
// }
