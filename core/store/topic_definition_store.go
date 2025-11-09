package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/franz-kafka/server/core/models"
)

// SaveTopicDefinition saves a topic definition/template
func (s *ConfigStore) SaveTopicDefinition(ctx context.Context, def *models.TopicDefinition) error {
	if err := def.Validate(); err != nil {
		return err
	}

	// Set timestamps
	now := time.Now().UTC()
	if def.CreatedAt.IsZero() {
		def.CreatedAt = now
	}
	def.UpdatedAt = now

	// Marshal to JSON
	data, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("failed to marshal topic definition: %w", err)
	}

	// Write to Kafka
	if err := s.writeMessage(ctx, s.definitionsTopicName, []byte(def.Name), data); err != nil {
		return fmt.Errorf("failed to write topic definition: %w", err)
	}

	// Update cache
	s.mu.Lock()
	s.definitions[def.Name] = def
	s.mu.Unlock()

	return nil
}

// GetTopicDefinition retrieves a topic definition by name
func (s *ConfigStore) GetTopicDefinition(name string) (*models.TopicDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	def, exists := s.definitions[name]
	if !exists {
		return nil, models.ErrTopicDefinitionNotFound
	}

	return def, nil
}

// ListTopicDefinitions returns all topic definitions
func (s *ConfigStore) ListTopicDefinitions() []*models.TopicDefinition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	defs := make([]*models.TopicDefinition, 0, len(s.definitions))
	for _, def := range s.definitions {
		defs = append(defs, def)
	}

	return defs
}

// DeleteTopicDefinition deletes a topic definition (writes tombstone)
func (s *ConfigStore) DeleteTopicDefinition(ctx context.Context, name string) error {
	// Write tombstone (nil value)
	if err := s.writeMessage(ctx, s.definitionsTopicName, []byte(name), nil); err != nil {
		return fmt.Errorf("failed to delete topic definition: %w", err)
	}

	// Remove from cache
	s.mu.Lock()
	delete(s.definitions, name)
	s.mu.Unlock()

	return nil
}
