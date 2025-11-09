package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/franz-kafka/server/core/models"
)

// AddTopicToCluster handles POST /api/cluster/{cluster_name}/topics
func (h *ConfigHandler) AddTopicToCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster name from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/topics")
	if len(parts) != 2 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}
	clusterName := parts[0]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body", err)
		return
	}
	defer r.Body.Close()

	var ct models.ClusterTopic
	if err := json.Unmarshal(body, &ct); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON", err)
		return
	}

	// Ensure cluster name matches the URL
	ct.ClusterName = clusterName

	if err := h.configStore.SaveClusterTopic(r.Context(), &ct); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to add topic to cluster", err)
		return
	}

	writeJSON(w, http.StatusCreated, ct)
}

// ListClusterTopics handles GET /api/cluster/{cluster_name}/topics
func (h *ConfigHandler) ListClusterTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster name from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/topics")
	if len(parts) != 2 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}
	clusterName := parts[0]

	topics := h.configStore.ListClusterTopics(clusterName)

	response := map[string]interface{}{
		"cluster": clusterName,
		"topics":  topics,
		"count":   len(topics),
	}

	writeJSON(w, http.StatusOK, response)
}

// GetClusterTopic handles GET /api/cluster/{cluster_name}/topic/{topic_name}
func (h *ConfigHandler) GetClusterTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster and topic names from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/topic/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name and topic name are required", nil)
		return
	}
	clusterName := parts[0]
	topicName := parts[1]

	ct, err := h.configStore.GetClusterTopic(clusterName, topicName)
	if err != nil {
		if err == models.ErrClusterTopicNotFound {
			writeError(w, http.StatusNotFound, "Cluster topic not found", err)
		} else {
			writeError(w, http.StatusInternalServerError, "Failed to get cluster topic", err)
		}
		return
	}

	writeJSON(w, http.StatusOK, ct)
}

// UpdateClusterTopic handles PUT /api/cluster/{cluster_name}/topic/{topic_name}
func (h *ConfigHandler) UpdateClusterTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster and topic names from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/topic/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name and topic name are required", nil)
		return
	}
	clusterName := parts[0]
	topicName := parts[1]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body", err)
		return
	}
	defer r.Body.Close()

	var ct models.ClusterTopic
	if err := json.Unmarshal(body, &ct); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON", err)
		return
	}

	// Ensure names match the URL
	ct.ClusterName = clusterName
	ct.TopicName = topicName

	if err := h.configStore.SaveClusterTopic(r.Context(), &ct); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to update cluster topic", err)
		return
	}

	writeJSON(w, http.StatusOK, ct)
}

// RemoveTopicFromCluster handles DELETE /api/cluster/{cluster_name}/topic/{topic_name}
func (h *ConfigHandler) RemoveTopicFromCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster and topic names from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/topic/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name and topic name are required", nil)
		return
	}
	clusterName := parts[0]
	topicName := parts[1]

	if err := h.configStore.DeleteClusterTopic(r.Context(), clusterName, topicName); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to remove topic from cluster", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Topic removed from cluster successfully",
		"cluster": clusterName,
		"topic":   topicName,
	})
}
