// Package telemetry is Gregor Samsa's structural observer (005 ADR Part 2). It
// reuses the AdminClients the reconciler already holds to read topic and broker
// facts, and publishes them to Franz as pre-registered indicator samples over
// TelemetryService.
//
// It observes *shape* — brokers, replicas, leaders, ISR, config drift — and
// never runs synthetic traffic. Odradek answers "can a client meet its SLO on
// this topic right now?"; this answers "does the fleet's shape match what Franz
// declared?" (005 ADR §2.3).
package telemetry

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
)

// Indicator names published by Gregor Samsa (005 ADR §2.1). Deliverable 14
// registers them; until the registry exists Franz accepts any name.
const (
	IndicatorTopicState             = "kafka.topic.state"
	IndicatorTopicPartitions        = "kafka.topic.partitions"
	IndicatorTopicReplicationFactor = "kafka.topic.replication_factor"
	IndicatorTopicUnderReplicated   = "kafka.topic.under_replicated_partitions"
	IndicatorTopicConfigDrift       = "kafka.topic.config_drift"

	IndicatorClusterBrokerCount     = "kafka.cluster.broker_count"
	IndicatorClusterOnlineBrokers   = "kafka.cluster.online_broker_count"
	IndicatorClusterControllerID    = "kafka.cluster.controller_id"
	IndicatorClusterTotalReplicas   = "kafka.cluster.total_partition_replicas"
	IndicatorClusterReplicasPerBrkr = "kafka.cluster.replicas_per_broker"
	IndicatorClusterLeadersPerBrkr  = "kafka.cluster.leaders_per_broker"
	IndicatorClusterUnderReplicated = "kafka.cluster.under_replicated_partitions"
	IndicatorClusterOfflinePartns   = "kafka.cluster.offline_partitions"
)

// Topic-state values for IndicatorTopicState.
const (
	TopicStateProvisioned = "provisioned"
	TopicStateDiverged    = "diverged"
	TopicStateMissing     = "missing"
)

// Entity is the resource kind a sample describes (governance.proto `Entity`).
type Entity string

const (
	EntityKafkaTopic   Entity = "KAFKA_TOPIC"
	EntityKafkaCluster Entity = "KAFKA_CLUSTER"
)

// Sample is one observation, ready to publish.
type Sample struct {
	Indicator      string
	ResourceFRN    string
	ResourceEntity Entity
	Value          string
	SampleAt       time.Time
}

// Publisher ships a batch of samples to Franz.
type Publisher interface {
	Publish(ctx context.Context, samples []Sample) error
}

// World is the current in-scope work: the partitions the agent manages and the
// AdminClient for each cluster. The reconciler is the source of both.
type World interface {
	// Partitions returns the assignments the agent is currently managing.
	Partitions() []assign.Assignment
	// Admins returns the cached AdminClient per cluster name.
	Admins() map[string]kafkaadmin.Admin
}

// Sweeper runs the periodic full sweep and the post-reconcile spot sample.
type Sweeper struct {
	world     World
	publisher Publisher
	log       *slog.Logger
	now       func() time.Time
	// Interval is the full-sweep cadence (005 ADR §2.2: default 60s).
	Interval time.Duration
}

// NewSweeper wires the sweeper.
func NewSweeper(world World, publisher Publisher, interval time.Duration, log *slog.Logger) *Sweeper {
	return &Sweeper{world: world, publisher: publisher, log: log, now: time.Now, Interval: interval}
}

// Run sweeps every Interval until ctx is done.
func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Sweep(ctx); err != nil && ctx.Err() == nil {
				s.log.Warn("telemetry sweep failed", "err", err)
			}
		}
	}
}

// Sweep observes every in-scope cluster and partition once and publishes the
// batch. A cluster or topic that cannot be read is reported as far as it can be
// (a missing topic is a `missing` sample, not a dropped one) rather than failing
// the whole sweep.
func (s *Sweeper) Sweep(ctx context.Context) error {
	at := s.now().UTC()
	admins := s.world.Admins()

	var (
		mu      sync.Mutex
		samples []Sample
	)
	add := func(batch []Sample) {
		mu.Lock()
		defer mu.Unlock()
		samples = append(samples, batch...)
	}

	// The cluster FRN comes off the assignments themselves — every cluster the
	// agent holds an AdminClient for got it from at least one assignment.
	byCluster := map[string][]assign.Assignment{}
	clusterFRNs := map[string]string{}
	for _, a := range s.world.Partitions() {
		byCluster[a.ClusterName] = append(byCluster[a.ClusterName], a)
		if a.ClusterFRN != "" {
			clusterFRNs[a.ClusterName] = a.ClusterFRN
		}
	}

	var wg sync.WaitGroup
	for clusterName, admin := range admins {
		wg.Add(1)
		go func(clusterName, clusterFRN string, admin kafkaadmin.Admin, assignments []assign.Assignment) {
			defer wg.Done()
			add(s.observeCluster(ctx, clusterName, clusterFRN, admin, at))
			for _, a := range assignments {
				add(s.observeTopic(ctx, admin, a, at))
			}
		}(clusterName, clusterFRNs[clusterName], admin, byCluster[clusterName])
	}
	wg.Wait()

	if len(samples) == 0 {
		return nil
	}
	return s.publisher.Publish(ctx, samples)
}

// ObservePartition publishes a fresh sample for one partition right after it
// reconciles, so `kafka.topic.state` reflects the create/alter without waiting
// for the next sweep (005 ADR §2.2 "Cadence").
func (s *Sweeper) ObservePartition(ctx context.Context, a assign.Assignment, admin kafkaadmin.Admin) {
	samples := s.observeTopic(ctx, admin, a, s.now().UTC())
	if len(samples) == 0 {
		return
	}
	if err := s.publisher.Publish(ctx, samples); err != nil && ctx.Err() == nil {
		s.log.Warn("post-reconcile telemetry publish failed", "partition", a.PartitionFRN, "err", err)
	}
}

// observeTopic reads one topic and turns it into the 005 §2.1 topic-level
// indicators, keyed by the async channel partition's FRN.
func (s *Sweeper) observeTopic(
	ctx context.Context, admin kafkaadmin.Admin, a assign.Assignment, at time.Time,
) []Sample {
	if a.PartitionFRN == "" {
		return nil
	}
	sample := func(name, value string) Sample {
		return Sample{Indicator: name, ResourceFRN: a.PartitionFRN,
			ResourceEntity: EntityKafkaTopic, Value: value, SampleAt: at}
	}

	actual, err := admin.DescribeTopic(ctx, a.TopicName)
	if err != nil {
		s.log.Warn("observe topic failed", "topic", a.TopicName, "cluster", a.ClusterName, "err", err)
		return nil
	}
	if actual == nil {
		return []Sample{sample(IndicatorTopicState, TopicStateMissing)}
	}

	drift := false
	for k, want := range a.DesiredConfig {
		if got, ok := actual.Config[k]; !ok || got != want {
			drift = true
			break
		}
	}
	state := TopicStateProvisioned
	if drift || actual.Partitions != a.Partitions ||
		(a.ReplicationFactor > 0 && actual.ReplicationFactor != a.ReplicationFactor) {
		state = TopicStateDiverged
	}

	return []Sample{
		sample(IndicatorTopicState, state),
		sample(IndicatorTopicPartitions, strconv.Itoa(int(actual.Partitions))),
		sample(IndicatorTopicReplicationFactor, strconv.Itoa(int(actual.ReplicationFactor))),
		sample(IndicatorTopicUnderReplicated, strconv.Itoa(int(actual.UnderReplicatedPartitions))),
		sample(IndicatorTopicConfigDrift, strconv.FormatBool(drift)),
	}
}

// observeCluster reads one cluster's metadata and turns it into the 005 §2.1
// cluster-level indicators. Per-broker counts are keyed by a `<cluster-frn>/broker/<id>`
// sub-resource (005 OQ6 — a label on a cluster-scoped sample is the alternative).
func (s *Sweeper) observeCluster(
	ctx context.Context, clusterName, clusterFRN string, admin kafkaadmin.Admin, at time.Time,
) []Sample {
	if clusterFRN == "" {
		return nil
	}
	state, err := admin.DescribeCluster(ctx)
	if err != nil {
		s.log.Warn("observe cluster failed", "cluster", clusterName, "err", err)
		return nil
	}

	sample := func(name, value string) Sample {
		return Sample{Indicator: name, ResourceFRN: clusterFRN,
			ResourceEntity: EntityKafkaCluster, Value: value, SampleAt: at}
	}
	brokerSample := func(name string, broker int32, value int32) Sample {
		return Sample{
			Indicator:      name,
			ResourceFRN:    clusterFRN + "/broker/" + strconv.Itoa(int(broker)),
			ResourceEntity: EntityKafkaCluster,
			Value:          strconv.Itoa(int(value)),
			SampleAt:       at,
		}
	}

	samples := []Sample{
		sample(IndicatorClusterBrokerCount, strconv.Itoa(int(state.BrokerCount))),
		sample(IndicatorClusterOnlineBrokers, strconv.Itoa(int(state.OnlineBrokerCount))),
		sample(IndicatorClusterControllerID, strconv.Itoa(int(state.ControllerID))),
		sample(IndicatorClusterTotalReplicas, strconv.Itoa(int(state.TotalPartitionReplicas))),
		sample(IndicatorClusterUnderReplicated, strconv.Itoa(int(state.UnderReplicatedPartitions))),
		sample(IndicatorClusterOfflinePartns, strconv.Itoa(int(state.OfflinePartitions))),
	}
	for broker, n := range state.ReplicasPerBroker {
		samples = append(samples, brokerSample(IndicatorClusterReplicasPerBrkr, broker, n))
	}
	for broker, n := range state.LeadersPerBroker {
		samples = append(samples, brokerSample(IndicatorClusterLeadersPerBrkr, broker, n))
	}
	return samples
}
