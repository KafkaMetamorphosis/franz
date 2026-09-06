package in

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
)

// CreateClusterInput is the client-settable state for a new Kafka Cluster.
type CreateClusterInput struct {
	Name              string
	ConnectionStrings []cluster.ConnectionString
	Labels            map[string]string
	Configuration     map[string]string
	ProviderAgent     string
}

// UpdateClusterInput carries only the fields named in the request's FieldMask;
// a nil pointer means "leave unchanged". `name` selects the cluster and is not
// itself mutable.
type UpdateClusterInput struct {
	Name              string
	ConnectionStrings *[]cluster.ConnectionString
	Labels            *map[string]string
	Configuration     *map[string]string
	ProviderAgent     *string
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
	Delete(ctx context.Context, name string) error
	Pause(ctx context.Context, name string) (*cluster.Cluster, error)
	Resume(ctx context.Context, name string) (*cluster.Cluster, error)
}
