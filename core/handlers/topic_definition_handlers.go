package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/franz-kafka/server/core/models"
)

// CreateTopicDefinition handles POST /api/topic_definitions
func (h *ConfigHandler) CreateTopicDefinition(w http.ResponseWriter, r *http.Request) {
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

	var def models.TopicDefinition
	if err := json.Unmarshal(body, &def); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON", err)
		return
	}

	if err := h.configStore.SaveTopicDefinition(r.Context(), &def); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to save topic definition", err)
		return
	}

	writeJSON(w, http.StatusCreated, def)
}

// ListTopicDefinitions handles GET /api/topic_definitions
func (h *ConfigHandler) ListTopicDefinitions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	definitions := h.configStore.ListTopicDefinitions()

	response := map[string]interface{}{
		"definitions": definitions,
		"count":       len(definitions),
	}

	writeJSON(w, http.StatusOK, response)
}

// GetTopicDefinition handles GET /api/topic_definition/{name}
func (h *ConfigHandler) GetTopicDefinition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract definition name from path
	name := strings.TrimPrefix(r.URL.Path, "/api/topic_definition/")
	if name == "" || name == "/api/topic_definition/" {
		writeError(w, http.StatusBadRequest, "Topic definition name is required", nil)
		return
	}

	def, err := h.configStore.GetTopicDefinition(name)
	if err != nil {
		if err == models.ErrTopicDefinitionNotFound {
			writeError(w, http.StatusNotFound, "Topic definition not found", err)
		} else {
			writeError(w, http.StatusInternalServerError, "Failed to get topic definition", err)
		}
		return
	}

	writeJSON(w, http.StatusOK, def)
}

// UpdateTopicDefinition handles PUT /api/topic_definition/{name}
func (h *ConfigHandler) UpdateTopicDefinition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract definition name from path
	name := strings.TrimPrefix(r.URL.Path, "/api/topic_definition/")
	if name == "" || name == "/api/topic_definition/" {
		writeError(w, http.StatusBadRequest, "Topic definition name is required", nil)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body", err)
		return
	}
	defer r.Body.Close()

	var def models.TopicDefinition
	if err := json.Unmarshal(body, &def); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON", err)
		return
	}

	// Ensure the name in the URL matches the body
	def.Name = name

	if err := h.configStore.SaveTopicDefinition(r.Context(), &def); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to update topic definition", err)
		return
	}

	writeJSON(w, http.StatusOK, def)
}

// DeleteTopicDefinition handles DELETE /api/topic_definition/{name}
func (h *ConfigHandler) DeleteTopicDefinition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract definition name from path
	name := strings.TrimPrefix(r.URL.Path, "/api/topic_definition/")
	if name == "" || name == "/api/topic_definition/" {
		writeError(w, http.StatusBadRequest, "Topic definition name is required", nil)
		return
	}

	if err := h.configStore.DeleteTopicDefinition(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to delete topic definition", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Topic definition deleted successfully",
		"name":    name,
	})
}
