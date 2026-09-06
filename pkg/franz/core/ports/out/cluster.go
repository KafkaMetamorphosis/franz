package out

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
)

// ClusterQuery parameterises ClusterRepository.List.
type ClusterQuery struct {
	RealmID        uuid.UUID
	Selector       selector.Selector // empty ⇒ match all
	IncludeDeleted bool              // List excludes DELETED unless set
	Limit          int               // page size (already clamped)
	AfterName      string            // exclusive lower bound, "" ⇒ first page
}

// ClusterPage is one page of a List result, ordered by name ascending.
type ClusterPage struct {
	Clusters  []*cluster.Cluster
	LastName  string // name of the last row, for the next page token ("" ⇒ no more)
	TotalSize int
}

// ClusterRepository persists Kafka Cluster registrations (003.12). Realm scoping
// is the caller's responsibility — every method takes or carries a realm_id.
type ClusterRepository interface {
	// Create inserts a new row. A name/FRN collision is errs.AlreadyExists.
	Create(ctx context.Context, c *cluster.Cluster) error

	// Get returns the cluster by (realm, name), including a soft-deleted one so
	// the caller can raise FAILED_PRECONDITION. errs.NotFound if absent.
	Get(ctx context.Context, realmID uuid.UUID, name string) (*cluster.Cluster, error)

	// List returns one page per ClusterQuery.
	List(ctx context.Context, q ClusterQuery) (ClusterPage, error)

	// Mutate loads the row FOR UPDATE, runs mutate, and persists the result —
	// all in one transaction (003.12 "Concurrency"). A non-nil error from mutate
	// rolls the transaction back and is returned as-is. errs.NotFound if absent.
	Mutate(ctx context.Context, realmID uuid.UUID, name string,
		mutate func(*cluster.Cluster) error) (*cluster.Cluster, error)

	// ListByProviderAgent returns every cluster in the realm whose
	// cluster_provider_agent equals agentName, DELETED rows included (the agent
	// needs the REMOVED assignment). Not paginated — an agent's fleet is bounded.
	ListByProviderAgent(ctx context.Context, realmID uuid.UUID, agentName string) ([]*cluster.Cluster, error)
}

// ClusterTopicGuard reports whether a cluster still hosts live Kafka Topics, so
// DeleteKafkaCluster can refuse (003.3). Deliverable 09 provides the real
// implementation; until then a no-op returns 0.
type ClusterTopicGuard interface {
	CountLiveTopics(ctx context.Context, clusterID uuid.UUID) (int, error)
}
