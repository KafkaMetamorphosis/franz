package kafkaadmin

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kadmAdmin is the real Admin, backed by franz-go's kadm client.
type kadmAdmin struct {
	client *kadm.Client
	kgo    *kgo.Client
}

var _ Admin = (*kadmAdmin)(nil)

// NewKadm opens an Admin against bootstrapServers. It is the production Factory.
func NewKadm(_ context.Context, bootstrapServers []string) (Admin, error) {
	if len(bootstrapServers) == 0 {
		return nil, errors.New("kafkaadmin: no bootstrap servers")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrapServers...))
	if err != nil {
		return nil, fmt.Errorf("kafkaadmin: open client for %v: %w", bootstrapServers, err)
	}
	return &kadmAdmin{client: kadm.NewClient(cl), kgo: cl}, nil
}

func (a *kadmAdmin) Close() { a.client.Close() }

// DescribeTopic reads the topic's shape from metadata and its configuration from
// DescribeConfigs. An unknown topic is (nil, nil).
//
// The metadata read lists *all* topics and looks this one up client-side rather
// than requesting it by name. A by-name `MetadataRequest` issued on the same
// franz-go client immediately after `CreateTopic` transiently returns
// UNKNOWN_TOPIC_OR_PARTITION for seconds — the client's per-topic negative
// cache lingers — while the unfiltered list is correct on the first try. This
// matters for the read-back a reconcile does right after creating a topic.
func (a *kadmAdmin) DescribeTopic(ctx context.Context, topic string) (*Topic, error) {
	details, err := a.client.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	detail, ok := details[topic]
	if !ok {
		return nil, nil
	}
	if detail.Err != nil {
		if errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			return nil, nil
		}
		return nil, fmt.Errorf("describe topic %q: %w", topic, detail.Err)
	}

	t := &Topic{Name: topic, Partitions: int32(len(detail.Partitions)), Config: map[string]string{}}
	for _, p := range detail.Partitions {
		if len(p.ISR) < len(p.Replicas) {
			t.UnderReplicatedPartitions++
		}
	}
	// Kafka permits an uneven per-partition replica count after a reassignment;
	// report the lowest so a half-finished RF change reads as a divergence
	// rather than as already-satisfied.
	t.ReplicationFactor = -1
	for _, p := range detail.Partitions {
		if rf := int32(len(p.Replicas)); t.ReplicationFactor < 0 || rf < t.ReplicationFactor {
			t.ReplicationFactor = rf
		}
	}
	if t.ReplicationFactor < 0 {
		t.ReplicationFactor = 0
	}

	configs, err := a.client.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("describe configs for topic %q: %w", topic, err)
	}
	for _, rc := range configs {
		if rc.Name != topic {
			continue
		}
		if rc.Err != nil {
			return nil, fmt.Errorf("describe configs for topic %q: %w", topic, rc.Err)
		}
		for _, c := range rc.Configs {
			if c.Value != nil {
				t.Config[c.Key] = *c.Value
			}
		}
	}
	return t, nil
}

func (a *kadmAdmin) CreateTopic(
	ctx context.Context, topic string, partitions, replicationFactor int32, config map[string]string,
) error {
	configs := make(map[string]*string, len(config))
	for k, v := range config {
		configs[k] = &v //nolint:exportloopref // Go 1.22+ gives each iteration its own v
	}
	resp, err := a.client.CreateTopic(ctx, partitions, int16(replicationFactor), configs, topic)
	if err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", topic, resp.Err)
	}
	return nil
}

func (a *kadmAdmin) CreatePartitions(ctx context.Context, topic string, total int32) error {
	responses, err := a.client.UpdatePartitions(ctx, int(total), topic)
	if err != nil {
		return fmt.Errorf("create partitions for %q: %w", topic, err)
	}
	if err := responses.Error(); err != nil {
		return fmt.Errorf("create partitions for %q: %w", topic, err)
	}
	return nil
}

func (a *kadmAdmin) AlterConfigs(ctx context.Context, topic string, set map[string]string) error {
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic request order, for reproducible logs

	alters := make([]kadm.AlterConfig, 0, len(keys))
	for _, k := range keys {
		v := set[k]
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &v})
	}
	responses, err := a.client.AlterTopicConfigs(ctx, alters, topic)
	if err != nil {
		return fmt.Errorf("alter configs for %q: %w", topic, err)
	}
	for _, r := range responses {
		if r.Err != nil {
			return fmt.Errorf("alter configs for %q: %w (%s)", topic, r.Err, r.ErrMessage)
		}
	}
	return nil
}

func (a *kadmAdmin) DeleteTopic(ctx context.Context, topic string) error {
	resp, err := a.client.DeleteTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("delete topic %q: %w", topic, err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("delete topic %q: %w", topic, resp.Err)
	}
	return nil
}

func (a *kadmAdmin) ListOffsets(ctx context.Context, topic string) ([]PartitionOffsets, error) {
	earliest, err := a.client.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}
	latest, err := a.client.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	byPartition := map[int32]*PartitionOffsets{}
	var firstErr error
	earliest.Each(func(o kadm.ListedOffset) {
		if o.Topic != topic {
			return
		}
		if o.Err != nil && firstErr == nil {
			firstErr = fmt.Errorf("list start offsets for %q partition %d: %w", topic, o.Partition, o.Err)
			return
		}
		byPartition[o.Partition] = &PartitionOffsets{Partition: o.Partition, Earliest: o.Offset}
	})
	if firstErr != nil {
		return nil, firstErr
	}
	latest.Each(func(o kadm.ListedOffset) {
		if o.Topic != topic {
			return
		}
		if o.Err != nil && firstErr == nil {
			firstErr = fmt.Errorf("list end offsets for %q partition %d: %w", topic, o.Partition, o.Err)
			return
		}
		if p, ok := byPartition[o.Partition]; ok {
			p.Latest = o.Offset
		}
	})
	if firstErr != nil {
		return nil, firstErr
	}

	out := make([]PartitionOffsets, 0, len(byPartition))
	for _, p := range byPartition {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out, nil
}

func (a *kadmAdmin) ListConsumerGroups(ctx context.Context) ([]string, error) {
	groups, err := a.client.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (a *kadmAdmin) ListConsumerGroupOffsets(ctx context.Context, group, topic string) ([]int32, error) {
	responses, err := a.client.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets for group %q topic %q: %w", group, topic, err)
	}
	var committed []int32
	for partition, r := range responses[topic] {
		if r.Err != nil {
			return nil, fmt.Errorf("fetch offsets for group %q topic %q partition %d: %w",
				group, topic, partition, r.Err)
		}
		// FetchOffsetsForTopics fills a -1 offset in for partitions the group
		// never committed to; anything else is a real commit.
		if r.At >= 0 {
			committed = append(committed, partition)
		}
	}
	sort.Slice(committed, func(i, j int) bool { return committed[i] < committed[j] })
	return committed, nil
}

func (a *kadmAdmin) DescribeCluster(ctx context.Context) (*Cluster, error) {
	meta, err := a.client.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("describe cluster: %w", err)
	}
	c := &Cluster{
		BrokerCount:       int32(len(meta.Brokers)),
		OnlineBrokerCount: int32(len(meta.Brokers)),
		ControllerID:      meta.Controller,
		ReplicasPerBroker: map[int32]int32{},
		LeadersPerBroker:  map[int32]int32{},
	}
	// Metadata only lists brokers that answered, so every listed broker is
	// online. A broker that is down disappears from the response entirely, which
	// is why broker_count and online_broker_count agree here — the discrepancy
	// an operator cares about is against the cluster's declared broker count in
	// Franz, not against this response.
	for _, b := range meta.Brokers {
		c.ReplicasPerBroker[b.NodeID] = 0
		c.LeadersPerBroker[b.NodeID] = 0
	}
	for _, t := range meta.Topics {
		if t.Err != nil {
			continue
		}
		for _, p := range t.Partitions {
			c.TotalPartitionReplicas += int32(len(p.Replicas))
			for _, r := range p.Replicas {
				c.ReplicasPerBroker[r]++
			}
			if p.Leader >= 0 {
				c.LeadersPerBroker[p.Leader]++
			} else {
				c.OfflinePartitions++
			}
			if len(p.ISR) < len(p.Replicas) {
				c.UnderReplicatedPartitions++
			}
		}
	}
	return c, nil
}
