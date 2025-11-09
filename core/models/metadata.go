package models

import "time"

// ClusterMetadata represents metadata about a Kafka cluster
type ClusterMetadata struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	BootstrapURLs []string  `json:"bootstrap_urls"`
	BrokerCount   int       `json:"broker_count"`
	TopicCount    int       `json:"topic_count"`
	LastSyncTime  time.Time `json:"last_sync_time"`
}

// PartitionMetadata represents detailed information about a topic partition
type PartitionMetadata struct {
	ID       int   `json:"id"`
	Leader   int   `json:"leader"`
	Replicas []int `json:"replicas"`
	ISRs     []int `json:"isrs"`
}

// TopicMetadata represents metadata about a Kafka topic
type TopicMetadata struct {
	ClusterID         string              `json:"cluster_id"`
	TopicName         string              `json:"topic_name"`
	Partitions        []PartitionMetadata `json:"partitions"`
	ReplicationFactor int                 `json:"replication_factor"`
	Configs           map[string]string   `json:"configs,omitempty"`
	LastSyncTime      time.Time           `json:"last_sync_time"`
}
