package models

import "time"

// TopicDefinition represents a topic template/definition
// This is stored in franz.topic.definitions.store compacted topic
type TopicDefinition struct {
	Name      string            `json:"name"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// Validate checks if the topic definition is valid
func (td *TopicDefinition) Validate() error {
	if td.Name == "" {
		return ErrTopicDefinitionNameRequired
	}
	return nil
}
