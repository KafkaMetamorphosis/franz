// Package topics is the Kafka Topic application service (003.6). Franz owns the
// entity — this service exposes reads and the single client mutation
// (SetConsumption); creation and deletion happen as a consequence of Async
// Channel operations (deliverable 10). The caller's realm is read from context.
package topics

import (
	"context"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
	"github.com/KafkaMetamorphosis/franz/pkg/shared/pagetoken"
)

// Service implements in.KafkaTopicService.
type Service struct {
	repo     out.TopicRepository
	clusters out.ClusterRepository // for the ListKafkaTopics kafka_cluster filter (name → id)
}

var _ in.KafkaTopicService = (*Service)(nil)

// NewService wires the service to its ports.
func NewService(repo out.TopicRepository, clusters out.ClusterRepository) *Service {
	return &Service{repo: repo, clusters: clusters}
}

// Get returns the shard by name, including a soft-deleted one.
func (s *Service) Get(ctx context.Context, name string) (*topic.KafkaTopic, error) {
	r := realm.MustFromContext(ctx)
	return s.repo.Get(ctx, r.ID, name)
}

// List returns one page, ordered by name, optionally filtered to one channel
// and/or one cluster.
func (s *Service) List(ctx context.Context, input in.ListTopicsInput) (in.TopicPage, error) {
	r := realm.MustFromContext(ctx)

	q := out.TopicQuery{
		RealmID: r.ID,
		Limit:   pagetoken.ClampSize(input.PageSize),
	}
	if input.AsyncChannel != "" {
		id, err := s.repo.ResolveChannelID(ctx, r.ID, input.AsyncChannel)
		if err != nil {
			return in.TopicPage{}, err
		}
		q.AsyncChannelID = &id
	}
	if input.KafkaCluster != "" {
		c, err := s.clusters.Get(ctx, r.ID, input.KafkaCluster)
		if err != nil {
			return in.TopicPage{}, err
		}
		q.KafkaClusterID = &c.ID
	}

	queryKey := pagetoken.QueryKey("topic", input.AsyncChannel, input.KafkaCluster)
	after, err := pagetoken.Decode(input.PageToken, queryKey)
	if err != nil {
		return in.TopicPage{}, err
	}
	q.AfterName = after

	page, err := s.repo.List(ctx, q)
	if err != nil {
		return in.TopicPage{}, err
	}
	return in.TopicPage{
		Topics:        page.Topics,
		NextPageToken: pagetoken.Encode(page.LastName, queryKey),
	}, nil
}

// SetConsumption drains (DISABLED) or restores (ENABLED) the shard and
// re-normalises the owning channel's traffic-share split: every ENABLED shard
// gets 100 / enabled-count percent, every DISABLED shard zero. The flip and the
// re-normalisation are one transaction.
func (s *Service) SetConsumption(
	ctx context.Context, name string, c topic.Consumption,
) (*topic.KafkaTopic, error) {
	r := realm.MustFromContext(ctx)

	target, err := s.repo.Get(ctx, r.ID, name)
	if err != nil {
		return nil, err
	}
	if err := target.EnsureMutable(); err != nil {
		return nil, err
	}
	if !c.Valid() {
		return nil, errs.InvalidField("consumption", "must be ENABLED or DISABLED")
	}

	shards, err := s.repo.MutateChannelShards(ctx, r.ID, target.AsyncChannelID,
		func(shards []*topic.KafkaTopic) error {
			var t *topic.KafkaTopic
			for _, sh := range shards {
				if sh.Name == name {
					t = sh
				}
			}
			if t == nil {
				return errs.Internalf("shard %q not among its channel's shards", name)
			}
			if _, err := t.SetConsumption(c); err != nil {
				return err
			}
			rebalance(shards)
			return nil
		})
	if err != nil {
		return nil, err
	}

	for _, sh := range shards {
		if sh.Name == name {
			return sh, nil
		}
	}
	return nil, errs.Internalf("shard %q not returned after mutate", name)
}

// rebalance re-normalises the traffic-share split across a channel's shards:
// equal percent across the ENABLED ones, zero for the DISABLED ones.
func rebalance(shards []*topic.KafkaTopic) {
	enabled := 0
	for _, sh := range shards {
		if sh.Consumption == topic.ConsumptionEnabled {
			enabled++
		}
	}
	share := topic.EqualSharePercent(enabled)
	for _, sh := range shards {
		if sh.Consumption == topic.ConsumptionEnabled {
			sh.SetTrafficShare(share)
		} else {
			sh.SetTrafficShare(0)
		}
	}
}
