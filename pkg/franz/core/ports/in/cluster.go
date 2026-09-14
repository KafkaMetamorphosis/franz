package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
)

// CreateClusterInput is the client-settable state for a new Kafka Cluster.
type CreateClusterInput struct {
	Name                    string
	ConnectionStrings       []cluster.ConnectionString
	Labels                  map[string]string
	Configuration           map[string]string
	ProviderAgent           string
	Brokers                 int32
	DiskSize                string
	MaxConcurrentMigrations int32
}

// UpdateClusterInput carries only the fields named in the request's FieldMask;
// a nil pointer means "leave unchanged". `name` selects the cluster and is not
// itself mutable.
type UpdateClusterInput struct {
	Name                    string
	ConnectionStrings       *[]cluster.ConnectionString
	Labels                  *map[string]string
	Configuration           *map[string]string
	ProviderAgent           *string
	Brokers                 *int32
	DiskSize                *string
	MaxConcurrentMigrations *int32
}

// ListClustersInput parameterises a List call. Selector is the raw 003.1
// selector string; PageToken is opaque.
type ListClustersInput struct {
	Selector  string
	PageSize  int32
	PageToken string
}

// ClusterPage is a page of List results.
type ClusterPage struct {
	Clusters      []*cluster.Cluster
	NextPageToken string
	TotalSize     int32
}

// KafkaClusterService is the driving port for Kafka Cluster management (003.3).
// The realm is taken from the request context, never from the input.
type KafkaClusterService interface {
	Create(ctx context.Context, in CreateClusterInput) (*cluster.Cluster, error)
	Get(ctx context.Context, name string) (*cluster.Cluster, error)
	List(ctx context.Context, in ListClustersInput) (ClusterPage, error)
	Update(ctx context.Context, in UpdateClusterInput) (*cluster.Cluster, error)
	// Delete removes the cluster. If it still has live shards, force must be
	// true (003.13 OQ5) — Delete then triggers a drain migration for every
	// live shard (reason "cluster-delete") instead of deleting the cluster row
	// immediately; the cluster becomes DELETED once every shard has moved off.
	// Without force, a cluster with live shards is FAILED_PRECONDITION.
	Delete(ctx context.Context, name string, force bool) error
	Pause(ctx context.Context, name string) (*cluster.Cluster, error)
	Resume(ctx context.Context, name string) (*cluster.Cluster, error)
}
