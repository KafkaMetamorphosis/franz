package grpcgateway

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

// telemetryHandler implements the agent-facing TelemetryService sample ingest
// (003.14). gRPC only — no REST gateway. ReportConsumerGroups lands with
// deliverable 14 and stays Unimplemented here.
type telemetryHandler struct {
	franzv1.UnimplementedTelemetryServiceServer
	svc in.TelemetryIngestService
}

// RegisterTelemetryService mounts the service on the gRPC server only.
func RegisterTelemetryService(s *Server, svc in.TelemetryIngestService) {
	franzv1.RegisterTelemetryServiceServer(s.grpc, &telemetryHandler{svc: svc})
}

// PublishIndicatorSamples appends one batch — the one-shot form.
func (h *telemetryHandler) PublishIndicatorSamples(
	ctx context.Context, req *franzv1.PublishIndicatorSamplesRequest,
) (*franzv1.PublishIndicatorSamplesResponse, error) {
	accepted, err := h.svc.IngestSamples(ctx, samplesFromProto(req.GetSamples()))
	if err != nil {
		return nil, ToError(err)
	}
	return franzv1.PublishIndicatorSamplesResponse_builder{
		Accepted: proto.Int32(int32(accepted)),
	}.Build(), nil
}

// StreamIndicatorSamples appends batch after batch over one long-lived client
// stream (005 ADR §2.2) and answers once, when the agent half-closes, with the
// running total. A batch that fails validation ends the stream so the agent sees
// the error instead of silently dropping samples.
func (h *telemetryHandler) StreamIndicatorSamples(
	stream grpc.ClientStreamingServer[franzv1.StreamIndicatorSamplesRequest, franzv1.StreamIndicatorSamplesResponse],
) error {
	var total int
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(franzv1.StreamIndicatorSamplesResponse_builder{
				Accepted: proto.Int32(int32(total)),
			}.Build())
		}
		if err != nil {
			return err
		}
		accepted, err := h.svc.IngestSamples(stream.Context(), samplesFromProto(req.GetSamples()))
		if err != nil {
			return ToError(err)
		}
		total += accepted
	}
}

// samplesFromProto maps the wire batch onto the domain. `agent` on the request
// is ignored: the authenticated identity from the bearer token is authoritative,
// so an agent cannot attribute samples to another.
func samplesFromProto(in []*franzv1.IndicatorSample) []indicator.Sample {
	out := make([]indicator.Sample, 0, len(in))
	for _, s := range in {
		// An unset sample_at stays the zero time so the service stamps its own
		// clock; AsTime() on a nil timestamp would yield the Unix epoch instead.
		var sampleAt time.Time
		if ts := s.GetSampleAt(); ts != nil {
			sampleAt = ts.AsTime()
		}
		out = append(out, indicator.Sample{
			Indicator:      s.GetIndicator(),
			ResourceFRN:    s.GetResourceFrn(),
			ResourceEntity: entityFromProto(s.GetResourceEntity()),
			Value:          s.GetValue(),
			SampleAt:       sampleAt,
		})
	}
	return out
}

func entityFromProto(e franzv1.Entity) indicator.Entity {
	switch e {
	case franzv1.Entity_ENTITY_ASYNC_CHANNEL:
		return indicator.EntityAsyncChannel
	case franzv1.Entity_ENTITY_KAFKA_TOPIC:
		return indicator.EntityKafkaTopic
	case franzv1.Entity_ENTITY_KAFKA_CLUSTER:
		return indicator.EntityKafkaCluster
	default:
		return "" // domain rejects
	}
}
