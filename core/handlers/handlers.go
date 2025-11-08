package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/franz-kafka/server/core/kafka"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	admin *kafka.Admin
}

// NewHandler creates a new handler instance
func NewHandler(admin *kafka.Admin) *Handler {
	return &Handler{
		admin: admin,
	}
}

// HealthCheck handles health check requests
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]string{
		"status": "healthy",
	}
	writeJSON(w, http.StatusOK, response)
}

// GetTopics handles requests to list all topics
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

// GetTopic handles requests to get a specific topic's metadata
func (h *Handler) GetTopic(w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimPrefix(r.URL.Path, "/api/topics/")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "Topic name is required", nil)
		return
	}

	partition, err := h.admin.GetTopicMetadata(r.Context(), topic)
	if err != nil {
		writeError(w, http.StatusNotFound, "Topic not found", err)
		return
	}

	response := map[string]interface{}{
		"topic":     partition.Topic,
		"partition": partition.ID,
		"leader":    partition.Leader,
	}

	writeJSON(w, http.StatusOK, response)
}

// GetBrokers handles requests to get broker information
func (h *Handler) GetBrokers(w http.ResponseWriter, r *http.Request) {
	brokers, err := h.admin.GetBrokers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to retrieve brokers", err)
		return
	}

	brokerList := make([]map[string]interface{}, 0, len(brokers))
	for _, broker := range brokers {
		brokerList = append(brokerList, map[string]interface{}{
			"id":   broker.ID,
			"host": broker.Host,
			"port": broker.Port,
		})
	}

	response := map[string]interface{}{
		"brokers": brokerList,
		"count":   len(brokerList),
	}

	writeJSON(w, http.StatusOK, response)
}

// GetClusterMetadata handles requests to get cluster metadata
func (h *Handler) GetClusterMetadata(w http.ResponseWriter, r *http.Request) {
	metadata, err := h.admin.GetClusterMetadata(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to retrieve cluster metadata", err)
		return
	}

	writeJSON(w, http.StatusOK, metadata)
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
