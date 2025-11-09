package models

import "time"

// Cluster represents a Kafka cluster configuration/definition
// This is stored in franz.clusters.store compacted topic
type Cluster struct {
	Name         string            `json:"name"`
	BootstrapURL string            `json:"bootstrap_url"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// Validate checks if the cluster definition is valid
func (c *Cluster) Validate() error {
	if c.Name == "" {
		return ErrClusterNameRequired
	}
	if c.BootstrapURL == "" {
		return ErrClusterBootstrapURLRequired
	}
	return nil
}
