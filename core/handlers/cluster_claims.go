package handlers

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/franz-kafka/server/core/adapters"
	"github.com/franz-kafka/server/core/models"
	"github.com/franz-kafka/server/core/wire/in"
	"github.com/franz-kafka/server/core/wire/out"
	"github.com/gorilla/mux"
)

func (h *Handler) ListClusterClaims(w http.ResponseWriter, r *http.Request) {
	clusterClaims := h.store.ListClusters()

	response := out.Paginated{
		Total: len(clusterClaims),
		Items: adapters.ClusterClaimsToWire(clusterClaims),
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) CreateClusterClaim(w http.ResponseWriter, r *http.Request) {
	log.Printf("[handler/clusters] Received request to create cluster")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Failed to read request body", err)
		return
	}

	log.Printf("[handler/clusters] Request body: %v", string(body))
	defer r.Body.Close()

	var clusterClaimWire in.ClusterClaim
	if err := json.Unmarshal(body, &clusterClaimWire); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid cluster claim received", err)
		return
	}

	clusterClaimModel, err := h.store.SaveClusterClaim(r.Context(), adapters.ClusterClaimFromWire(clusterClaimWire))

	if err != nil {
		log.Printf("[handler/clusters] Failed to save cluster: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to save cluster", err)
		return
	}

	writeJSON(w, http.StatusCreated, adapters.ClusterClaimToWire(clusterClaimModel))
}

func (h *Handler) GetClusterClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	urlParameters := mux.Vars(r)
	name := urlParameters["cluster_name"]
	if name == "" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}

	clusterClaimModel, err := h.store.GetCluster(name)
	if err != nil {
		if err == models.ErrClusterNotFound {
			writeError(w, http.StatusNotFound, "Cluster not found", err)
		} else {
			writeError(w, http.StatusInternalServerError, "Failed to get cluster", err)
		}
		return
	}

	writeJSON(w, http.StatusOK, adapters.ClusterClaimToWire(clusterClaimModel))
}

func (h *Handler) UpdateClusterClaim(w http.ResponseWriter, r *http.Request) {
	urlParameters := mux.Vars(r)
	name := urlParameters["cluster_name"]
	if name == "" {
		writeError(w, http.StatusBadRequest, "cluster name is required", nil)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body", err)
		return
	}
	defer r.Body.Close()

	var clusterClaimWire in.ClusterClaim
	if err := json.Unmarshal(body, &clusterClaimWire); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON from update cluster claim", err)
		return
	}

	if clusterClaimWire.Metadata.Name != name {
		writeError(w, http.StatusBadRequest, "cluster name cannot be updated", err)
		return
	}

	log.Printf("[handlers/cluster_claims] Updating cluster claim '%s'", name)

	clusterClaimModel, err := h.store.SaveClusterClaim(r.Context(), adapters.ClusterClaimFromWire(clusterClaimWire))

	if err != nil {
		log.Printf("[handler/clusters] Failed to update cluster: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to update cluster", err)
		return
	}

	writeJSON(w, http.StatusOK, adapters.ClusterClaimToWire(clusterClaimModel))
}

func (h *Handler) DeleteClusterClaim(w http.ResponseWriter, r *http.Request) {
	urlParameters := mux.Vars(r)
	name := urlParameters["cluster_name"]
	if name == "" {
		writeError(w, http.StatusBadRequest, "cluster name is required", nil)
		return
	}

	log.Printf("[handlers/cluster_claims] Deleting cluster claim '%s'", name)

	if err := h.store.DeleteClusterClaim(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete cluster", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": "cluster deleted successfully",
		"name":    name,
	})
}
