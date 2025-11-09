package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/franz-kafka/server/core/kafka"
	"github.com/franz-kafka/server/core/store"
)

// MetadataHandler handles metadata-related HTTP requests
type MetadataHandler struct {
	metadataStore *store.MetadataStore
	metadataSync  *kafka.MetadataSync
}

// NewMetadataHandler creates a new metadata handler instance
func NewMetadataHandler(metadataStore *store.MetadataStore, metadataSync *kafka.MetadataSync) *MetadataHandler {
	return &MetadataHandler{
		metadataStore: metadataStore,
		metadataSync:  metadataSync,
	}
}

// SyncClusterRequest represents the request body for syncing a cluster
type SyncClusterRequest struct {
	ClusterID   string   `json:"cluster_id"`
	ClusterName string   `json:"cluster_name"`
	Brokers     []string `json:"brokers"`
}

// SyncTopicRequest represents the request body for syncing topics
type SyncTopicRequest struct {
	ClusterID string   `json:"cluster_id"`
	TopicName string   `json:"topic_name,omitempty"` // If empty, sync all topics
	Brokers   []string `json:"brokers"`
}

// SyncCluster handles POST /api/metadata/clusters/sync
func (h *MetadataHandler) SyncCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body", err)
		return
	}
	defer r.Body.Close()

	var req SyncClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	if req.ClusterID == "" {
		writeError(w, http.StatusBadRequest, "cluster_id is required", nil)
		return
	}

	if req.ClusterName == "" {
		writeError(w, http.StatusBadRequest, "cluster_name is required", nil)
		return
	}

	if len(req.Brokers) == 0 {
		writeError(w, http.StatusBadRequest, "brokers are required", nil)
		return
	}

	if err := h.metadataSync.SyncCluster(r.Context(), req.ClusterID, req.ClusterName, req.Brokers); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to sync cluster", err)
		return
	}

	response := map[string]interface{}{
		"message":    "Cluster synced successfully",
		"cluster_id": req.ClusterID,
	}
	writeJSON(w, http.StatusOK, response)
}

// SyncTopics handles POST /api/metadata/topics/sync
func (h *MetadataHandler) SyncTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body", err)
		return
	}
	defer r.Body.Close()

	var req SyncTopicRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	if req.ClusterID == "" {
		writeError(w, http.StatusBadRequest, "cluster_id is required", nil)
		return
	}

	if len(req.Brokers) == 0 {
		writeError(w, http.StatusBadRequest, "brokers are required", nil)
		return
	}

	// Sync single topic or all topics
	if req.TopicName != "" {
		if err := h.metadataSync.SyncTopic(r.Context(), req.ClusterID, req.TopicName, req.Brokers); err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to sync topic", err)
			return
		}
		response := map[string]interface{}{
			"message":    "Topic synced successfully",
			"cluster_id": req.ClusterID,
			"topic_name": req.TopicName,
		}
		writeJSON(w, http.StatusOK, response)
	} else {
		if err := h.metadataSync.SyncAllTopics(r.Context(), req.ClusterID, req.Brokers); err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to sync topics", err)
			return
		}
		response := map[string]interface{}{
			"message":    "All topics synced successfully",
			"cluster_id": req.ClusterID,
		}
		writeJSON(w, http.StatusOK, response)
	}
}

// ListClusters handles GET /api/metadata/clusters
func (h *MetadataHandler) ListClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	clusters := h.metadataStore.ListClusters()

	response := map[string]interface{}{
		"clusters": clusters,
		"count":    len(clusters),
	}

	writeJSON(w, http.StatusOK, response)
}

// GetCluster handles GET /api/metadata/clusters/{id}
func (h *MetadataHandler) GetCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster ID from path
	clusterID := strings.TrimPrefix(r.URL.Path, "/api/metadata/clusters/")
	if clusterID == "" || clusterID == "/api/metadata/clusters/" {
		writeError(w, http.StatusBadRequest, "Cluster ID is required", nil)
		return
	}

	cluster, err := h.metadataStore.GetCluster(clusterID)
	if err != nil {
		writeError(w, http.StatusNotFound, "Cluster not found", err)
		return
	}

	writeJSON(w, http.StatusOK, cluster)
}

// ListTopics handles GET /api/metadata/topics
func (h *MetadataHandler) ListTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Optional filter by cluster_id
	clusterID := r.URL.Query().Get("cluster_id")

	topics := h.metadataStore.ListTopics(clusterID)

	response := map[string]interface{}{
		"topics": topics,
		"count":  len(topics),
	}

	if clusterID != "" {
		response["cluster_id"] = clusterID
	}

	writeJSON(w, http.StatusOK, response)
}

// GetTopic handles GET /api/metadata/topics/{cluster_id}/{topic}
func (h *MetadataHandler) GetTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster ID and topic name from path
	path := strings.TrimPrefix(r.URL.Path, "/api/metadata/topics/")
	parts := strings.SplitN(path, "/", 2)

	if len(parts) != 2 {
		writeError(w, http.StatusBadRequest, "Both cluster_id and topic_name are required", nil)
		return
	}

	clusterID := parts[0]
	topicName := parts[1]

	if clusterID == "" || topicName == "" {
		writeError(w, http.StatusBadRequest, "Both cluster_id and topic_name are required", nil)
		return
	}

	topic, err := h.metadataStore.GetTopic(clusterID, topicName)
	if err != nil {
		writeError(w, http.StatusNotFound, "Topic not found", err)
		return
	}

	writeJSON(w, http.StatusOK, topic)
}
