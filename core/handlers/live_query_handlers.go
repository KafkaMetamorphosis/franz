package handlers

import (
	"net/http"
	"strings"

	"github.com/franz-kafka/server/core/kafka"
	"github.com/franz-kafka/server/core/store"
)

// LiveQueryHandler handles live queries to actual Kafka clusters
type LiveQueryHandler struct {
	configStore *store.ConfigStore
}

// NewLiveQueryHandler creates a new live query handler
func NewLiveQueryHandler(configStore *store.ConfigStore) *LiveQueryHandler {
	return &LiveQueryHandler{
		configStore: configStore,
	}
}

// getAdminForCluster creates an admin client for the specified cluster
func (h *LiveQueryHandler) getAdminForCluster(clusterName string) (*kafka.Admin, error) {
	cluster, err := h.configStore.GetCluster(clusterName)
	if err != nil {
		return nil, err
	}

	// Parse bootstrap URL to create admin client
	brokers := []string{cluster.BootstrapURL}
	return kafka.NewAdmin(brokers), nil
}

// GetLiveTopics handles GET /api/cluster/{cluster_name}/live/topics
func (h *LiveQueryHandler) GetLiveTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster name from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/live/topics")
	if len(parts) != 2 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}
	clusterName := parts[0]

	admin, err := h.getAdminForCluster(clusterName)
	if err != nil {
		writeError(w, http.StatusNotFound, "Cluster not found or unable to connect", err)
		return
	}

	partitions, err := admin.GetTopics(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to retrieve topics from Kafka", err)
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
		"cluster": clusterName,
		"topics":  topicMap,
		"count":   len(topicMap),
	}

	writeJSON(w, http.StatusOK, response)
}

// GetLiveTopic handles GET /api/cluster/{cluster_name}/live/topic/{topic_name}
func (h *LiveQueryHandler) GetLiveTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster and topic names from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/live/topic/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name and topic name are required", nil)
		return
	}
	clusterName := parts[0]
	topicName := parts[1]

	admin, err := h.getAdminForCluster(clusterName)
	if err != nil {
		writeError(w, http.StatusNotFound, "Cluster not found or unable to connect", err)
		return
	}

	partition, err := admin.GetTopicMetadata(r.Context(), topicName)
	if err != nil {
		writeError(w, http.StatusNotFound, "Topic not found in Kafka", err)
		return
	}

	response := map[string]interface{}{
		"cluster":   clusterName,
		"topic":     partition.Topic,
		"partition": partition.ID,
		"leader":    partition.Leader,
	}

	writeJSON(w, http.StatusOK, response)
}

// GetLiveBrokers handles GET /api/cluster/{cluster_name}/live/brokers
func (h *LiveQueryHandler) GetLiveBrokers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}

	// Extract cluster name from path
	path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
	parts := strings.Split(path, "/live/brokers")
	if len(parts) != 2 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "Cluster name is required", nil)
		return
	}
	clusterName := parts[0]

	admin, err := h.getAdminForCluster(clusterName)
	if err != nil {
		writeError(w, http.StatusNotFound, "Cluster not found or unable to connect", err)
		return
	}

	brokers, err := admin.GetBrokers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to retrieve brokers from Kafka", err)
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
		"cluster": clusterName,
		"brokers": brokerList,
		"count":   len(brokerList),
	}

	writeJSON(w, http.StatusOK, response)
}
