package grpcgateway

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

type fakeTopicSvc struct {
	ret       *topic.KafkaTopic
	list      in.TopicPage
	err       error
	setName   string
	setConsum topic.Consumption
}

func (f *fakeTopicSvc) Get(context.Context, string) (*topic.KafkaTopic, error) {
	return f.ret, f.err
}
func (f *fakeTopicSvc) List(context.Context, in.ListTopicsInput) (in.TopicPage, error) {
	return f.list, f.err
}
func (f *fakeTopicSvc) SetConsumption(_ context.Context, name string, c topic.Consumption) (*topic.KafkaTopic, error) {
	f.setName, f.setConsum = name, c
	return f.ret, f.err
}

func sampleTopic(t *testing.T) *topic.KafkaTopic {
	t.Helper()
	sh, err := topic.New(realm.Realm{Slug: "default"}, uuid.New(), "orders", 1,
		map[string]string{"a": "1"}, map[string]string{"b": "2"}, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	sh.ChannelName = "orders"
	sh.ClusterName = "east-1"
	sh.TrafficShare = topic.TrafficShare{Value: 50, Unit: "percent"}
	sh.CreatedAt = time.Unix(1700000000, 0)
	return sh
}

func newTopicHandler(svc in.KafkaTopicService) *kafkaTopicHandler {
	return &kafkaTopicHandler{svc: svc, codec: frn.MustCodec("frn")}
}

func TestGetKafkaTopicRendersNamesAndFRN(t *testing.T) {
	h := newTopicHandler(&fakeTopicSvc{ret: sampleTopic(t)})
	resp, err := h.GetKafkaTopic(context.Background(), franzv1.GetKafkaTopicRequest_builder{
		Name: proto.String("orders-1"),
	}.Build())
	if err != nil {
		t.Fatalf("GetKafkaTopic: %v", err)
	}
	kt := resp.GetKafkaTopic()
	if kt.GetName() != "orders-1" || kt.GetFrn() != "frn:default:kafka-topic:orders-1" {
		t.Errorf("name/frn = %q / %q", kt.GetName(), kt.GetFrn())
	}
	if kt.GetAsyncChannel() != "orders" || kt.GetKafkaCluster() != "east-1" {
		t.Errorf("channel/cluster = %q / %q", kt.GetAsyncChannel(), kt.GetKafkaCluster())
	}
	if kt.GetState() != franzv1.KafkaTopicState_KAFKA_TOPIC_STATE_PENDING {
		t.Errorf("state = %v", kt.GetState())
	}
	if kt.GetTrafficShare().GetValue() != 50 || kt.GetTrafficShare().GetUnit() != "percent" {
		t.Errorf("traffic share = %+v", kt.GetTrafficShare())
	}
	if kt.GetPartitions() != 3 || kt.GetReplicationFactor() != 2 {
		t.Errorf("partitions/rf = %d / %d", kt.GetPartitions(), kt.GetReplicationFactor())
	}
}

func TestSetConsumptionForwardsAndMaps(t *testing.T) {
	fake := &fakeTopicSvc{ret: sampleTopic(t)}
	h := newTopicHandler(fake)
	_, err := h.SetConsumption(context.Background(), franzv1.SetConsumptionRequest_builder{
		Name:        proto.String("orders-1"),
		Consumption: franzv1.Consumption_CONSUMPTION_DISABLED.Enum(),
	}.Build())
	if err != nil {
		t.Fatalf("SetConsumption: %v", err)
	}
	if fake.setName != "orders-1" || fake.setConsum != topic.ConsumptionDisabled {
		t.Errorf("service saw %q / %q", fake.setName, fake.setConsum)
	}
}

func TestKafkaTopicErrorMapping(t *testing.T) {
	h := newTopicHandler(&fakeTopicSvc{err: errs.Preconditionf("kafka topic %q is deleted", "x")})
	_, err := h.SetConsumption(context.Background(), franzv1.SetConsumptionRequest_builder{
		Name: proto.String("x"), Consumption: franzv1.Consumption_CONSUMPTION_ENABLED.Enum(),
	}.Build())
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %v", status.Code(err))
	}
}

func TestListKafkaTopicsPassesFilters(t *testing.T) {
	fake := &fakeTopicSvc{list: in.TopicPage{Topics: []*topic.KafkaTopic{sampleTopic(t)}, NextPageToken: "next"}}
	h := newTopicHandler(fake)
	resp, err := h.ListKafkaTopics(context.Background(), franzv1.ListKafkaTopicsRequest_builder{
		AsyncChannel: proto.String("orders"),
	}.Build())
	if err != nil {
		t.Fatalf("ListKafkaTopics: %v", err)
	}
	if len(resp.GetKafkaTopics()) != 1 || resp.GetPage().GetNextPageToken() != "next" {
		t.Errorf("bad list response: %+v", resp)
	}
}
