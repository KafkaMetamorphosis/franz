package kafkaadmin

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Mem is an in-memory Admin — the test double for the reconciler and the
// telemetry sweep. It records every mutating call so a test can assert that an
// already-converged reconcile issued no Kafka write at all.
//
// Safe for concurrent use, like the real driver.
type Mem struct {
	mu sync.Mutex

	// Topics is the fake broker's topic state, keyed by topic name.
	Topics map[string]*Topic
	// Offsets is the earliest/latest pair per topic; absent means an empty topic.
	Offsets map[string][]PartitionOffsets
	// GroupOffsets maps a consumer group to the partitions it has committed on,
	// per topic: GroupOffsets["payments-worker"]["orders-0"] = []int32{0}.
	GroupOffsets map[string]map[string][]int32
	// ClusterState is what DescribeCluster returns.
	ClusterState Cluster

	// Fail, when set for a method name ("CreateTopic", "AlterConfigs", ...),
	// makes that call return the error instead of acting.
	Fail map[string]error

	// Calls is every method invoked, in order — including reads.
	Calls []string
	// Writes is the mutating calls only (CreateTopic, CreatePartitions,
	// AlterConfigs, DeleteTopic). An idempotent reconcile leaves it empty.
	Writes []string

	closed bool
}

var _ Admin = (*Mem)(nil)

// NewMem returns an empty fake broker.
func NewMem() *Mem {
	return &Mem{
		Topics:       map[string]*Topic{},
		Offsets:      map[string][]PartitionOffsets{},
		GroupOffsets: map[string]map[string][]int32{},
		Fail:         map[string]error{},
	}
}

// MemFactory returns a Factory handing out fixed fakes keyed by the first
// bootstrap server, so a multi-cluster test can give each cluster its own broker.
// An address with no fake registered gets a fresh empty one.
func MemFactory(byBootstrap map[string]*Mem) Factory {
	var mu sync.Mutex
	return func(_ context.Context, bootstrapServers []string) (Admin, error) {
		if len(bootstrapServers) == 0 {
			return nil, fmt.Errorf("kafkaadmin: no bootstrap servers")
		}
		mu.Lock()
		defer mu.Unlock()
		key := bootstrapServers[0]
		if m, ok := byBootstrap[key]; ok {
			return m, nil
		}
		m := NewMem()
		byBootstrap[key] = m
		return m, nil
	}
}

// Closed reports whether Close was called.
func (m *Mem) Closed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *Mem) record(call string, write bool) error {
	m.Calls = append(m.Calls, call)
	if write {
		m.Writes = append(m.Writes, call)
	}
	return m.Fail[call]
}

func (m *Mem) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func (m *Mem) DescribeTopic(_ context.Context, topic string) (*Topic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("DescribeTopic", false); err != nil {
		return nil, err
	}
	t, ok := m.Topics[topic]
	if !ok {
		return nil, nil
	}
	copied := *t
	copied.Config = map[string]string{}
	for k, v := range t.Config {
		copied.Config[k] = v
	}
	return &copied, nil
}

func (m *Mem) CreateTopic(
	_ context.Context, topic string, partitions, replicationFactor int32, config map[string]string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("CreateTopic", true); err != nil {
		return err
	}
	cfg := map[string]string{}
	for k, v := range config {
		cfg[k] = v
	}
	m.Topics[topic] = &Topic{
		Name: topic, Partitions: partitions, ReplicationFactor: replicationFactor, Config: cfg,
	}
	return nil
}

func (m *Mem) CreatePartitions(_ context.Context, topic string, total int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("CreatePartitions", true); err != nil {
		return err
	}
	t, ok := m.Topics[topic]
	if !ok {
		return fmt.Errorf("kafkaadmin: topic %q does not exist", topic)
	}
	if total < t.Partitions {
		return fmt.Errorf("kafkaadmin: cannot reduce partitions of %q", topic)
	}
	t.Partitions = total
	return nil
}

func (m *Mem) AlterConfigs(_ context.Context, topic string, set map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("AlterConfigs", true); err != nil {
		return err
	}
	t, ok := m.Topics[topic]
	if !ok {
		return fmt.Errorf("kafkaadmin: topic %q does not exist", topic)
	}
	if t.Config == nil {
		t.Config = map[string]string{}
	}
	for k, v := range set {
		t.Config[k] = v
	}
	return nil
}

func (m *Mem) DeleteTopic(_ context.Context, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("DeleteTopic", true); err != nil {
		return err
	}
	delete(m.Topics, topic)
	delete(m.Offsets, topic)
	return nil
}

func (m *Mem) ListOffsets(_ context.Context, topic string) ([]PartitionOffsets, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("ListOffsets", false); err != nil {
		return nil, err
	}
	if offsets, ok := m.Offsets[topic]; ok {
		return append([]PartitionOffsets(nil), offsets...), nil
	}
	// No fixture: report an empty topic with the partition count it has.
	t, ok := m.Topics[topic]
	if !ok {
		return nil, nil
	}
	out := make([]PartitionOffsets, t.Partitions)
	for i := range out {
		out[i] = PartitionOffsets{Partition: int32(i)}
	}
	return out, nil
}

func (m *Mem) ListConsumerGroups(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("ListConsumerGroups", false); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(m.GroupOffsets))
	for name := range m.GroupOffsets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (m *Mem) ListConsumerGroupOffsets(_ context.Context, group, topic string) ([]int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("ListConsumerGroupOffsets", false); err != nil {
		return nil, err
	}
	return append([]int32(nil), m.GroupOffsets[group][topic]...), nil
}

func (m *Mem) DescribeCluster(context.Context) (*Cluster, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.record("DescribeCluster", false); err != nil {
		return nil, err
	}
	c := m.ClusterState
	c.ReplicasPerBroker = copyCounts(m.ClusterState.ReplicasPerBroker)
	c.LeadersPerBroker = copyCounts(m.ClusterState.LeadersPerBroker)
	return &c, nil
}

func copyCounts(in map[int32]int32) map[int32]int32 {
	out := make(map[int32]int32, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
