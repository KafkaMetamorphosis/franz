package handlers

import "net/http"

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]string{"status": "healthy"}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) ConfigDump(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.config)
}
