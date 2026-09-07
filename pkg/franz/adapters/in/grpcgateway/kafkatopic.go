package grpcgateway

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// kafkaTopicHandler adapts KafkaTopicService onto the generated gRPC server.
type kafkaTopicHandler struct {
	franzv1.UnimplementedKafkaTopicServiceServer
	svc   in.KafkaTopicService
	codec frn.Codec
}

// RegisterKafkaTopicService mounts the KafkaTopicService on the gRPC server and
// the in-process REST gateway.
func RegisterKafkaTopicService(s *Server, svc in.KafkaTopicService, codec frn.Codec) error {
	h := &kafkaTopicHandler{svc: svc, codec: codec}
	franzv1.RegisterKafkaTopicServiceServer(s.grpc, h)
	return franzv1.RegisterKafkaTopicServiceHandlerServer(context.Background(), s.gw, h)
}

func (h *kafkaTopicHandler) GetKafkaTopic(
	ctx context.Context, req *franzv1.GetKafkaTopicRequest,
) (*franzv1.GetKafkaTopicResponse, error) {
	t, err := h.svc.Get(ctx, req.GetName())
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.GetKafkaTopicResponse_builder{KafkaTopic: h.toProto(t)}.Build(), nil
}

func (h *kafkaTopicHandler) ListKafkaTopics(
	ctx context.Context, req *franzv1.ListKafkaTopicsRequest,
) (*franzv1.ListKafkaTopicsResponse, error) {
	page, err := h.svc.List(ctx, in.ListTopicsInput{
		AsyncChannel: req.GetAsyncChannel(),
		KafkaCluster: req.GetKafkaCluster(),
		PageSize:     req.GetPage().GetPageSize(),
		PageToken:    req.GetPage().GetPageToken(),
	})
	if err != nil {
		return nil, ToError(err)
	}
	out := make([]*franzv1.KafkaTopic, len(page.Topics))
	for i, t := range page.Topics {
		out[i] = h.toProto(t)
	}
	return franzv1.ListKafkaTopicsResponse_builder{
		KafkaTopics: out,
		Page: franzv1.PageResponse_builder{
			NextPageToken: proto.String(page.NextPageToken),
			TotalSize:     proto.Int32(0), // best-effort; not computed (003.1)
		}.Build(),
	}.Build(), nil
}

func (h *kafkaTopicHandler) SetConsumption(
	ctx context.Context, req *franzv1.SetConsumptionRequest,
) (*franzv1.SetConsumptionResponse, error) {
	t, err := h.svc.SetConsumption(ctx, req.GetName(), consumptionFromProto(req.GetConsumption()))
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.SetConsumptionResponse_builder{KafkaTopic: h.toProto(t)}.Build(), nil
}

// --- mapping helpers -----------------------------------------------------

func (h *kafkaTopicHandler) toProto(t *topic.KafkaTopic) *franzv1.KafkaTopic {
	return franzv1.KafkaTopic_builder{
		Name:               proto.String(t.Name),
		Frn:                proto.String(h.codec.Render(t.FRN)),
		AsyncChannel:       proto.String(t.ChannelName),
		KafkaCluster:       proto.String(t.ClusterName),
		State:              topicStateToProto(t.State),
		Consumption:        consumptionToProto(t.Consumption),
		TopicConfiguration: t.TopicConfiguration,
		Partitions:         proto.Int32(t.Partitions),
		ReplicationFactor:  proto.Int32(t.ReplicationFactor),
		TrafficShare: franzv1.TrafficShare_builder{
			Value: proto.Float64(t.TrafficShare.Value),
			Unit:  proto.String(t.TrafficShare.Unit),
		}.Build(),
		Generation:      proto.Int64(t.Generation),
		Misplaced:       proto.Bool(t.Misplaced),
		MisplacedReason: proto.String(t.MisplacedReason),
		CreatedAt:       timestamppb.New(t.CreatedAt),
		UpdatedAt:       timestamppb.New(t.UpdatedAt),
	}.Build()
}

func topicStateToProto(s topic.State) *franzv1.KafkaTopicState {
	v := franzv1.KafkaTopicState_KAFKA_TOPIC_STATE_UNSPECIFIED
	switch s {
	case topic.StatePending:
		v = franzv1.KafkaTopicState_KAFKA_TOPIC_STATE_PENDING
	case topic.StateReady:
		v = franzv1.KafkaTopicState_KAFKA_TOPIC_STATE_READY
	case topic.StatePaused:
		v = franzv1.KafkaTopicState_KAFKA_TOPIC_STATE_PAUSED
	case topic.StateError:
		v = franzv1.KafkaTopicState_KAFKA_TOPIC_STATE_ERROR
	case topic.StateDeleted:
		v = franzv1.KafkaTopicState_KAFKA_TOPIC_STATE_DELETED
	}
	return &v
}

func consumptionToProto(c topic.Consumption) *franzv1.Consumption {
	v := franzv1.Consumption_CONSUMPTION_UNSPECIFIED
	switch c {
	case topic.ConsumptionEnabled:
		v = franzv1.Consumption_CONSUMPTION_ENABLED
	case topic.ConsumptionDisabled:
		v = franzv1.Consumption_CONSUMPTION_DISABLED
	}
	return &v
}

func consumptionFromProto(c franzv1.Consumption) topic.Consumption {
	switch c {
	case franzv1.Consumption_CONSUMPTION_ENABLED:
		return topic.ConsumptionEnabled
	case franzv1.Consumption_CONSUMPTION_DISABLED:
		return topic.ConsumptionDisabled
	default:
		return "" // domain rejects
	}
}
