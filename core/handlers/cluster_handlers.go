package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/franz-kafka/server/core/models"
	"github.com/franz-kafka/server/core/store"
)

// ConfigHandler handles configuration-related HTTP requests
type ConfigHandler struct {
	configStore *store.ConfigStore
}

// NewConfigHandler creates a new config handler instance
func NewConfigHandler(configStore *store.ConfigStore) *ConfigHandler {
	return &ConfigHandler{
		configStore: configStore,
	}
}

// CreateCluster handles POST /api/clusters
func (h *ConfigHandler) CreateCluster(w http.ResponseWriter, r *http.Request) {
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

	var cluster models.Cluster
	if err := json.Unmarshal(body, &cluster); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON", err)
		return
	}

	if err := h.configStore.SaveCluster(r.Context(), &cluster); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to save cluster", err)
		return
	}

	writeJSON(w, http.StatusCreated, cluster)
}

// ListClusters handles GET /api/clusters
func (h *ConfigHandler) ListClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	clusters := h.configStore.ListClusters()

	response := map[string]interface{}{
		"clusters": clusters,
		"count":    len(clusters),
	}

	writeJSON(w, http.StatusOK, response)
}

// GetCluster handles GET /api/cluster/{name}
func (h *ConfigHandler) GetCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster name from path
	name := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	if name == "" || name == "/api/cluster/" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}

	cluster, err := h.configStore.GetCluster(name)
	if err != nil {
		if err == models.ErrClusterNotFound {
			writeError(w, http.StatusNotFound, "Cluster not found", err)
		} else {
			writeError(w, http.StatusInternalServerError, "Failed to get cluster", err)
		}
		return
	}

	writeJSON(w, http.StatusOK, cluster)
}

// UpdateCluster handles PUT /api/cluster/{name}
func (h *ConfigHandler) UpdateCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster name from path
	name := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	if name == "" || name == "/api/cluster/" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body", err)
		return
	}
	defer r.Body.Close()

	var cluster models.Cluster
	if err := json.Unmarshal(body, &cluster); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON", err)
		return
	}

	// Ensure the name in the URL matches the body
	cluster.Name = name

	if err := h.configStore.SaveCluster(r.Context(), &cluster); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to update cluster", err)
		return
	}

	writeJSON(w, http.StatusOK, cluster)
}

// DeleteCluster handles DELETE /api/cluster/{name}
func (h *ConfigHandler) DeleteCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster name from path
	name := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	if name == "" || name == "/api/cluster/" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}

	if err := h.configStore.DeleteCluster(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to delete cluster", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Cluster deleted successfully",
		"name":    name,
	})
}
