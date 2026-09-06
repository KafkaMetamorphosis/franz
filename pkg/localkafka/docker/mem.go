package docker

import (
	"context"
	"fmt"
	"sync"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/recipe"
)

// MemDriver is an in-memory Driver for tests — it records the operations the
// reconciler performs without touching Docker.
type MemDriver struct {
	mu sync.Mutex

	AgentName string
	// containers keyed by name
	containers map[string]*memContainer
	volumes    map[string]bool
	images     map[string]bool

	// Ops is an ordered log of "verb name" strings for assertions.
	Ops []string
	// FailImagePull, when set, makes EnsureImage return it.
	FailImagePull error
}

type memContainer struct {
	id         string
	frn        string
	recipeHash string
	running    bool
	volume     string
}

// NewMemDriver returns an empty driver.
func NewMemDriver(agentName string) *MemDriver {
	return &MemDriver{
		AgentName:  agentName,
		containers: map[string]*memContainer{},
		volumes:    map[string]bool{},
		images:     map[string]bool{},
	}
}

func (m *MemDriver) record(op string) { m.Ops = append(m.Ops, op) }

// Seed adds a pre-existing container (e.g. to model a restart).
func (m *MemDriver) Seed(name, frn, hash string, running bool, volume string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.containers[name] = &memContainer{id: "id-" + name, frn: frn, recipeHash: hash, running: running, volume: volume}
	if volume != "" {
		m.volumes[volume] = true
	}
}

// Running reports whether the named container exists and is running.
func (m *MemDriver) Running(name string) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.containers[name]
	if !ok {
		return false, false
	}
	return c.running, true
}

// VolumeExists reports whether a data volume is present.
func (m *MemDriver) VolumeExists(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.volumes[name]
}

func (m *MemDriver) List(_ context.Context, agentName string) ([]Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if agentName != m.AgentName {
		return nil, nil
	}
	out := make([]Container, 0, len(m.containers))
	for name, c := range m.containers {
		out = append(out, Container{ID: c.id, Name: name, ClusterFRN: c.frn, RecipeHash: c.recipeHash, Running: c.running})
	}
	return out, nil
}

func (m *MemDriver) EnsureImage(_ context.Context, ref string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailImagePull != nil {
		return m.FailImagePull
	}
	if !m.images[ref] {
		m.images[ref] = true
		m.record("pull " + ref)
	}
	return nil
}

func (m *MemDriver) Create(_ context.Context, spec recipe.Spec) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.containers[spec.ContainerName]; exists {
		return "", fmt.Errorf("container %s already exists", spec.ContainerName)
	}
	m.containers[spec.ContainerName] = &memContainer{
		id:         "id-" + spec.ContainerName,
		frn:        spec.Labels[recipe.LabelCluster],
		recipeHash: spec.Labels[recipe.LabelRecipeHash],
		running:    false,
		volume:     spec.VolumeName,
	}
	m.volumes[spec.VolumeName] = true
	m.record("create " + spec.ContainerName)
	return "id-" + spec.ContainerName, nil
}

func (m *MemDriver) Start(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, c := range m.containers {
		if c.id == id {
			c.running = true
			m.record("start " + name)
			return nil
		}
	}
	return fmt.Errorf("no container %s", id)
}

func (m *MemDriver) Stop(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, c := range m.containers {
		if c.id == id {
			c.running = false
			m.record("stop " + name)
			return nil
		}
	}
	return nil // idempotent
}

func (m *MemDriver) Remove(_ context.Context, id, volumeName string, removeVolume bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, c := range m.containers {
		if c.id == id {
			delete(m.containers, name)
			m.record("remove " + name)
		}
	}
	if removeVolume && volumeName != "" {
		delete(m.volumes, volumeName)
		m.record("rmvol " + volumeName)
	}
	return nil
}
