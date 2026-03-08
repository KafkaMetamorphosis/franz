package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/franz-kafka/server/core/config"
	"github.com/franz-kafka/server/core/kafka"
	"github.com/franz-kafka/server/core/store"
)

type Handler struct {
	admin  *kafka.Admin
	config *config.Config
	store  *store.Store
}

func NewHandler(admin *kafka.Admin, store *store.Store, config *config.Config) *Handler {
	return &Handler{
		admin:  admin,
		config: config,
		store:  store,
	}
}

func (h *Handler) GetTopics(w http.ResponseWriter, r *http.Request) {
	partitions, err := h.admin.GetTopics(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to retrieve topics", err)
		return
	}

	// Extract unique topics
	topicMap := make(map[string][]map[string]interface{})
	for _, partition := range partitions {
		if _, exists := topicMap[partition.Topic]; !exists {
			topicMap[partition.Topic] = []map[string]interface{}{}
		}
		topicMap[partition.Topic] = append(topicMap[partition.Topic], map[string]interface{}{
			"id":     partition.ID,
			"leader": partition.Leader,
		})
	}

	response := map[string]interface{}{
		"topics": topicMap,
		"count":  len(topicMap),
	}

	writeJSON(w, http.StatusOK, response)
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError writes an error response
func writeError(w http.ResponseWriter, status int, message string, err error) {
	response := map[string]interface{}{
		"error":  message,
		"status": status,
	}
	if err != nil {
		response["details"] = err.Error()
	}
	writeJSON(w, status, response)
}
