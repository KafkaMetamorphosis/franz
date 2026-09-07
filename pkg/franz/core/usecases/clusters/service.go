// Package clusters is the Kafka Cluster application service (003.3). It
// orchestrates the domain entity and the out ports; it holds no SQL and no
// transport types. The caller's realm is read from context.
package clusters

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/scope"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/selector"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
)

// Service implements in.KafkaClusterService.
type Service struct {
	repo       out.ClusterRepository
	guard      out.ClusterTopicGuard
	providerRd out.ProviderStatusReader
	publisher  out.AssignmentPublisher
	// notifier re-resolves Resource Provider scope when a cluster's
	// franz.placement/* labels move (005 ADR §1.2 "Scope is dynamic"). Optional —
	// nil in tests that do not exercise the agent wire.
	notifier out.PartitionNotifier
	// placer re-runs channel → cluster placement when this cluster's labels or
	// state change, since either can add or remove it from a channel's candidate
	// set (003.7). Optional — nil in tests that do not exercise placement.
	placer out.ShardPlacer
}

var _ in.KafkaClusterService = (*Service)(nil)

// NewService wires the service to its ports.
func NewService(
	repo out.ClusterRepository,
	guard out.ClusterTopicGuard,
	providerRd out.ProviderStatusReader,
	publisher out.AssignmentPublisher,
	notifier out.PartitionNotifier,
	placer out.ShardPlacer,
) *Service {
	return &Service{
		repo: repo, guard: guard, providerRd: providerRd,
		publisher: publisher, notifier: notifier, placer: placer,
	}
}

// replace re-runs placement across the realm after a cluster's labels or state
// moved. Best-effort and after the caller's transaction — the cluster write
// already succeeded, and the retry sweep is the safety net.
func (s *Service) replace(ctx context.Context, realmID uuid.UUID) {
	if s.placer == nil {
		return
	}
	s.placer.PlaceRealm(ctx, realmID)
}

// Create registers a new cluster (state ACTIVE, FRN assigned).
func (s *Service) Create(ctx context.Context, input in.CreateClusterInput) (*cluster.Cluster, error) {
	r := realm.MustFromContext(ctx)
	if err := scope.ValidateClusterLabels(input.Labels); err != nil {
		return nil, err
	}
	if err := placement.ValidateClusterLabels(input.Labels); err != nil {
		return nil, err
	}
	c, err := cluster.New(r, input.Name, input.ConnectionStrings, input.Labels, input.Configuration, input.ProviderAgent)
	if err != nil {
		return nil, err
	}
	if err := c.SetShape(input.Brokers, input.DiskSize); err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, c); err != nil {
		return nil, err
	}
	s.publishTo("", c) // new cluster: SET to its provider agent, if any
	// A newly registered ACTIVE cluster may be the first candidate a channel has
	// been waiting for (003.7 "a shard places itself as soon as a cluster is
	// registered"); place now rather than waiting out a sweep interval.
	s.replace(ctx, r.ID)
	return c, nil
}

// Get returns the cluster by name (including a soft-deleted one), with the
// current provider status attached.
func (s *Service) Get(ctx context.Context, name string) (*cluster.Cluster, error) {
	r := realm.MustFromContext(ctx)
	c, err := s.repo.Get(ctx, r.ID, name)
	if err != nil {
		return nil, err
	}
	if st, err := s.providerRd.LatestStatus(ctx, c.ID); err == nil {
		c.ProviderStatus = st
	}
	return c, nil
}

// List returns one page, ordered by name, filtered by the selector. DELETED
// clusters are excluded. Provider status is not attached to list rows.
func (s *Service) List(ctx context.Context, input in.ListClustersInput) (in.ClusterPage, error) {
	r := realm.MustFromContext(ctx)

	sel, err := selector.Parse(input.Selector)
	if err != nil {
		return in.ClusterPage{}, err
	}
	queryKey := pagetoken.QueryKey("kafka-cluster", input.Selector)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.ClusterPage{}, err
	}

	page, err := s.repo.List(ctx, out.ClusterQuery{
		RealmID:   r.ID,
		Selector:  sel,
		Limit:     pagetoken.ClampSize(input.PageSize),
		AfterName: after,
	})
	if err != nil {
		return in.ClusterPage{}, err
	}
	return in.ClusterPage{
		Clusters:      page.Clusters,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
		TotalSize:     int32(page.TotalSize),
	}, nil
}

// Update applies the masked fields under a row lock (003.12) and pushes the
// resulting assignment change to the owning agent(s).
func (s *Service) Update(ctx context.Context, input in.UpdateClusterInput) (*cluster.Cluster, error) {
	r := realm.MustFromContext(ctx)
	if input.Labels != nil {
		if err := scope.ValidateClusterLabels(*input.Labels); err != nil {
			return nil, err
		}
		if err := placement.ValidateClusterLabels(*input.Labels); err != nil {
			return nil, err
		}
	}
	var (
		oldAgent     string
		labelsBefore map[string]string
	)
	updated, err := s.repo.Mutate(ctx, r.ID, input.Name, func(c *cluster.Cluster) error {
		oldAgent = c.ProviderAgent
		labelsBefore = c.Labels
		if err := c.EnsureMutable(); err != nil {
			return err
		}
		if input.ConnectionStrings != nil {
			if err := c.SetConnectionStrings(*input.ConnectionStrings); err != nil {
				return err
			}
		}
		if input.Labels != nil {
			c.Labels = *input.Labels
		}
		if input.Configuration != nil {
			c.Configuration = *input.Configuration
		}
		if input.ProviderAgent != nil {
			c.ProviderAgent = *input.ProviderAgent
		}
		if input.Brokers != nil || input.DiskSize != nil {
			brokers, diskSize := c.Brokers, c.DiskSize
			if input.Brokers != nil {
				brokers = *input.Brokers
			}
			if input.DiskSize != nil {
				diskSize = *input.DiskSize
			}
			if err := c.SetShape(brokers, diskSize); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.publishTo(oldAgent, updated)
	if s.notifier != nil && input.Labels != nil {
		s.notifier.ClusterLabelsChanged(ctx, r.ID, updated.Name, labelsBefore, updated.Labels)
	}
	// Any cluster label may be what a channel's affinity selector matches on, so
	// every label edit re-runs placement (003.7 "Cross-entity behavior").
	if input.Labels != nil {
		s.replace(ctx, r.ID)
	}
	return updated, nil
}

// Delete soft-deletes the cluster. It refuses (FAILED_PRECONDITION) while the
// cluster still hosts live Kafka Topics (003.3), then pushes a REMOVED
// assignment to the owning agent.
func (s *Service) Delete(ctx context.Context, name string) error {
	r := realm.MustFromContext(ctx)
	updated, err := s.repo.Mutate(ctx, r.ID, name, func(c *cluster.Cluster) error {
		if c.State == cluster.StateDeleted {
			return c.Delete() // yields FAILED_PRECONDITION
		}
		n, err := s.guard.CountLiveTopics(ctx, c.ID)
		if err != nil {
			return err
		}
		if n > 0 {
			return errs.Preconditionf("kafka cluster %q still hosts %d live topic(s)", name, n)
		}
		return c.Delete()
	})
	if err != nil {
		return err
	}
	s.publishTo("", updated)
	s.replace(ctx, r.ID)
	return nil
}

// Pause moves the cluster to PAUSED (idempotent) and tells the owning agent.
// Shards already on it become misplaced; new ones are never placed there.
func (s *Service) Pause(ctx context.Context, name string) (*cluster.Cluster, error) {
	r := realm.MustFromContext(ctx)
	c, err := s.repo.Mutate(ctx, r.ID, name, func(c *cluster.Cluster) error { return c.Pause() })
	if err != nil {
		return nil, err
	}
	s.publishTo("", c)
	s.replace(ctx, r.ID)
	return c, nil
}

// Resume moves the cluster to ACTIVE (idempotent) and tells the owning agent.
func (s *Service) Resume(ctx context.Context, name string) (*cluster.Cluster, error) {
	r := realm.MustFromContext(ctx)
	c, err := s.repo.Mutate(ctx, r.ID, name, func(c *cluster.Cluster) error { return c.Resume() })
	if err != nil {
		return nil, err
	}
	s.publishTo("", c)
	s.replace(ctx, r.ID)
	return c, nil
}

// publishTo fans the cluster's current assignment out to its provider agent, and
// — when the provider agent changed — a REMOVED to the previous one. A no-op
// when nobody is listening (004 ADR §1).
func (s *Service) publishTo(previousAgent string, c *cluster.Cluster) {
	if s.publisher == nil || c == nil {
		return
	}
	if previousAgent != "" && previousAgent != c.ProviderAgent {
		s.publisher.PublishAssignment(previousAgent, provider.Assignment{
			Change:      provider.ChangeRemoved,
			ClusterName: c.Name,
			ClusterFRN:  c.FRN,
		})
	}
	if c.ProviderAgent != "" {
		s.publisher.PublishAssignment(c.ProviderAgent, c.ToAssignment())
	}
}
