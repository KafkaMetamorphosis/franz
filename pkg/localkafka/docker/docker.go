// Package docker is the agent's Docker driver: a small interface (Driver) plus
// an Engine-API implementation and an in-memory fake for tests (ADR 004 §6).
// The agent keeps no local state — container labels are the store.
package docker

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/recipe"
)

// Container is the agent's view of one managed container.
type Container struct {
	ID         string
	Name       string
	ClusterFRN string // franz.cluster label
	RecipeHash string // franz.recipe-hash label
	Running    bool
}

// Driver is the minimal Docker surface the reconciler needs. All methods are
// idempotent where it matters (Stop of a stopped container, Remove of a missing
// one, etc. return nil).
type Driver interface {
	// List returns the containers labelled franz.managed-by=<agentName>.
	List(ctx context.Context, agentName string) ([]Container, error)
	// EnsureImage pulls ref unless it is already present locally.
	EnsureImage(ctx context.Context, ref string) error
	// Create makes (does not start) a container from the spec, returning its id.
	Create(ctx context.Context, spec recipe.Spec) (string, error)
	// Start / Stop toggle the container. No-op if already in that state.
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	// Remove force-deletes the container; RemoveVolume also drops volumeName.
	Remove(ctx context.Context, id string, volumeName string, removeVolume bool) error
}
