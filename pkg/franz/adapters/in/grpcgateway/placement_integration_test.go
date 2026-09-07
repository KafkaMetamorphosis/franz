package grpcgateway_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// Placement materialising an async-channel shard must reach the connected
// Resource Provider agent as a SET PartitionAssignment, without the agent
// reconnecting (task 13.8, 005 ADR §1.3).
func TestPlacementReachesAConnectedAgentStream(t *testing.T) {
	f := newResourceProviderFixture(t)
	ctx := context.Background()

	created, err := f.agents.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
		Name:   proto.String("gregor-samsa-prod"),
		Type:   franzv1.AgentType_AGENT_TYPE_RESOURCE_PROVIDER.Enum(),
		Labels: map[string]string{"franz.placement-selector/env": "prod"},
	}.Build())
	if err != nil {
		t.Fatalf("CreateAgent: %v", err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+created.GetToken())

	// The cluster carries both label families: `franz.placement/*` puts it in the
	// agent's scope, `env` is what the channel's affinity selector matches on.
	if _, err := f.clusters.CreateKafkaCluster(ctx, franzv1.CreateKafkaClusterRequest_builder{
		Name: proto.String("east-1"),
		ConnectionStrings: []*franzv1.ConnectionString{
			franzv1.ConnectionString_builder{BootstrapUrls: []string{"east-1:9092"}}.Build(),
		},
		Labels:               map[string]string{"franz.placement/env": "prod", "env": "prod"},
		ClusterConfiguration: map[string]string{"partitions": "6", "replication-factor": "3"},
	}.Build()); err != nil {
		t.Fatalf("CreateKafkaCluster: %v", err)
	}

	// A first channel is placed before the agent connects, so its shard arrives in
	// the stream's initial in-scope set — receiving it proves the stream is
	// registered with the hub before the delta below is published.
	if _, err := f.channels.CreateAsyncChannel(ctx, franzv1.CreateAsyncChannelRequest_builder{
		Name:              proto.String("orders"),
		Type:              franzv1.ChannelType_CHANNEL_TYPE_KAFKA_TOPIC.Enum(),
		ChannelPartitions: proto.Int32(1),
		Labels:            map[string]string{"franz.affinity/selector": "env=prod"},
	}.Build()); err != nil {
		t.Fatalf("CreateAsyncChannel(orders): %v", err)
	}

	streamCtx, cancelStream := context.WithCancel(authCtx)
	defer cancelStream()
	stream, err := f.resource.WatchPartitionAssignments(streamCtx,
		franzv1.WatchPartitionAssignmentsRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("WatchPartitionAssignments: %v", err)
	}
	if first := recvPartition(t, stream); first.GetTopicName() != "orders-0" {
		t.Fatalf("initial set = %+v, want the already-placed orders-0", first)
	}

	if _, err := f.channels.CreateAsyncChannel(ctx, franzv1.CreateAsyncChannelRequest_builder{
		Name:              proto.String("billing-events"),
		Type:              franzv1.ChannelType_CHANNEL_TYPE_KAFKA_TOPIC.Enum(),
		ChannelPartitions: proto.Int32(1),
		Labels:            map[string]string{"franz.affinity/selector": "env=prod"},
	}.Build()); err != nil {
		t.Fatalf("CreateAsyncChannel: %v", err)
	}

	assignment := recvPartition(t, stream)
	if assignment.GetChange() != franzv1.PartitionAssignment_CHANGE_SET {
		t.Errorf("change = %v, want CHANGE_SET", assignment.GetChange())
	}
	if assignment.GetTopicName() != "billing-events-0" {
		t.Errorf("topic_name = %q, want billing-events-0", assignment.GetTopicName())
	}
	if assignment.GetKafkaCluster() != "east-1" {
		t.Errorf("kafka_cluster = %q, want east-1", assignment.GetKafkaCluster())
	}
	if assignment.GetPartitions() != 6 || assignment.GetReplicationFactor() != 3 {
		t.Errorf("kafka shape = %d/%d, want 6/3 seeded from cluster_configuration",
			assignment.GetPartitions(), assignment.GetReplicationFactor())
	}
	if _, leaked := assignment.GetDesiredConfig()["partitions"]; leaked {
		t.Errorf("the `partitions` seed key leaked into desired_config: %v",
			assignment.GetDesiredConfig())
	}

	// The API surfaces the shard placement too, and it is not misplaced.
	shard, err := f.topics.GetKafkaTopic(ctx, franzv1.GetKafkaTopicRequest_builder{
		Name: proto.String("billing-events-0"),
	}.Build())
	if err != nil {
		t.Fatalf("GetKafkaTopic: %v", err)
	}
	if shard.GetKafkaTopic().GetMisplaced() {
		t.Errorf("misplaced = true on a freshly placed shard (%q)",
			shard.GetKafkaTopic().GetMisplacedReason())
	}

	// Relabelling the cluster out of the channel's affinity marks the shard
	// misplaced and moves nothing (003.7 interim behaviour).
	if _, err := f.clusters.UpdateKafkaCluster(ctx, franzv1.UpdateKafkaClusterRequest_builder{
		Name:       proto.String("east-1"),
		Labels:     map[string]string{"franz.placement/env": "prod", "env": "staging"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}.Build()); err != nil {
		t.Fatalf("UpdateKafkaCluster: %v", err)
	}
	shard, err = f.topics.GetKafkaTopic(ctx, franzv1.GetKafkaTopicRequest_builder{
		Name: proto.String("billing-events-0"),
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if !shard.GetKafkaTopic().GetMisplaced() {
		t.Error("misplaced = false after the cluster left the channel's affinity")
	}
	if shard.GetKafkaTopic().GetKafkaCluster() != "east-1" {
		t.Errorf("kafka_cluster = %q; only migration (003.13) may move a shard",
			shard.GetKafkaTopic().GetKafkaCluster())
	}
}
