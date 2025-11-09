package models

import "time"

// ClusterTopic represents a topic assignment to a cluster
// This links a topic (by name) to a cluster and optionally to a topic definition/template
// Stored in franz.cluster.topics.store compacted topic
type ClusterTopic struct {
	ClusterName   string            `json:"cluster_name"`
	TopicName     string            `json:"topic_name"`
	TopicTemplate string            `json:"topic_template,omitempty"` // Reference to TopicDefinition
	Metadata      map[string]string `json:"metadata,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

// Validate checks if the cluster topic assignment is valid
func (ct *ClusterTopic) Validate() error {
	if ct.ClusterName == "" {
		return ErrClusterTopicClusterRequired
	}
	if ct.TopicName == "" {
		return ErrClusterTopicNameRequired
	}
	return nil
}

// Key returns the compound key for this cluster topic
func (ct *ClusterTopic) Key() string {
	return ct.ClusterName + ":" + ct.TopicName
}
