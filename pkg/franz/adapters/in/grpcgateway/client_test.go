package grpcgateway

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

type fakeClientSvc struct {
	created       in.CreateClientInput
	updated       in.UpdateClientInput
	ret           *client.Client
	err           error
	listResp      in.ClientPage
	groups        in.ObservedGroupPage
	channelAccess in.ClientChannelAccessPage
}

func (f *fakeClientSvc) Create(_ context.Context, i in.CreateClientInput) (*client.Client, error) {
	f.created = i
	return f.ret, f.err
}
func (f *fakeClientSvc) Get(context.Context, string) (*client.Client, error) { return f.ret, f.err }
func (f *fakeClientSvc) List(context.Context, in.ListClientsInput) (in.ClientPage, error) {
	return f.listResp, f.err
}
func (f *fakeClientSvc) Update(_ context.Context, i in.UpdateClientInput) (*client.Client, error) {
	f.updated = i
	return f.ret, f.err
}
func (f *fakeClientSvc) Delete(context.Context, string) error { return f.err }
func (f *fakeClientSvc) ListObservedConsumerGroups(
	context.Context, in.ListObservedConsumerGroupsInput,
) (in.ObservedGroupPage, error) {
	return f.groups, f.err
}
func (f *fakeClientSvc) ListConsumerGroupObservations(
	context.Context, in.ListConsumerGroupObservationsInput,
) (in.ObservedGroupPage, error) {
	return f.groups, f.err
}
func (f *fakeClientSvc) ListClientChannelAccess(
	context.Context, in.ListClientChannelAccessInput,
) (in.ClientChannelAccessPage, error) {
	return f.channelAccess, f.err
}

func sampleClient(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.New(realm.Realm{Slug: "default"}, "billing",
		map[string]string{"org.com/owner": "payments-team"})
	if err != nil {
		t.Fatal(err)
	}
	c.CreatedAt = time.Unix(1700000000, 0)
	c.UpdatedAt = time.Unix(1700000001, 0)
	return c
}

func newClientHandler(svc in.ClientService, prefix string) *clientHandler {
	return &clientHandler{svc: svc, codec: frn.MustCodec(prefix)}
}

func TestCreateClientRendersFRN(t *testing.T) {
	fake := &fakeClientSvc{ret: sampleClient(t)}
	h := newClientHandler(fake, "acme")

	resp, err := h.CreateClient(context.Background(), franzv1.CreateClientRequest_builder{
		Name:   proto.String("billing"),
		Labels: map[string]string{"org.com/owner": "payments-team"},
	}.Build())
	if err != nil {
		t.Fatalf("CreateClient: %v", err)
	}
	if resp.GetClient().GetFrn() != "acme:default:client:billing" {
		t.Errorf("frn = %q", resp.GetClient().GetFrn())
	}
	if fake.created.Labels["org.com/owner"] != "payments-team" {
		t.Errorf("labels not forwarded: %+v", fake.created.Labels)
	}
}

func TestClientErrorMapping(t *testing.T) {
	h := newClientHandler(&fakeClientSvc{err: errs.NotFoundf("client %q not found", "x")}, "frn")
	_, err := h.GetClient(context.Background(), franzv1.GetClientRequest_builder{Name: proto.String("x")}.Build())
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v", status.Code(err))
	}
}

func TestUpdateClientOnlyForwardsMaskedLabels(t *testing.T) {
	fake := &fakeClientSvc{ret: sampleClient(t)}
	h := newClientHandler(fake, "frn")

	_, err := h.UpdateClient(context.Background(), franzv1.UpdateClientRequest_builder{
		Name:       proto.String("billing"),
		Labels:     map[string]string{"team": "obs"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}.Build())
	if err != nil {
		t.Fatalf("UpdateClient: %v", err)
	}
	if fake.updated.Labels == nil || (*fake.updated.Labels)["team"] != "obs" {
		t.Errorf("labels not forwarded: %+v", fake.updated)
	}
}

// TestUpdateClientRejectsNameInMask pins name immutability (003.10) at the
// wire boundary, not just the domain.
func TestUpdateClientRejectsNameInMask(t *testing.T) {
	fake := &fakeClientSvc{ret: sampleClient(t)}
	h := newClientHandler(fake, "frn")

	_, err := h.UpdateClient(context.Background(), franzv1.UpdateClientRequest_builder{
		Name:       proto.String("billing"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v", status.Code(err))
	}
}

func TestDeleteClientForwardsName(t *testing.T) {
	fake := &fakeClientSvc{}
	h := newClientHandler(fake, "frn")
	if _, err := h.DeleteClient(context.Background(),
		franzv1.DeleteClientRequest_builder{Name: proto.String("billing")}.Build()); err != nil {
		t.Fatalf("DeleteClient: %v", err)
	}
}

// TestObservedConsumerGroupViewsShareTheSameWireMapping pins client.proto's
// "current view / history view: both read Observation.ObservedAt" contract.
func TestObservedConsumerGroupViewsShareTheSameWireMapping(t *testing.T) {
	seenAt := time.Unix(1700000010, 0)
	obs, err := consumergroup.NewObservation(realm.Realm{}.ID, "billing.orders-0",
		"default:client:billing", "", "orders", "orders-0", "odradek-prod", seenAt, seenAt)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeClientSvc{groups: in.ObservedGroupPage{Groups: []*consumergroup.Observation{obs}}}
	h := newClientHandler(fake, "frn")

	current, err := h.ListObservedConsumerGroups(context.Background(),
		franzv1.ListObservedConsumerGroupsRequest_builder{Name: proto.String("billing")}.Build())
	if err != nil {
		t.Fatalf("ListObservedConsumerGroups: %v", err)
	}
	if len(current.GetGroups()) != 1 || !current.GetGroups()[0].GetLastSeenAt().AsTime().Equal(seenAt) {
		t.Fatalf("current view = %+v", current.GetGroups())
	}

	history, err := h.ListConsumerGroupObservations(context.Background(),
		franzv1.ListConsumerGroupObservationsRequest_builder{Name: proto.String("billing")}.Build())
	if err != nil {
		t.Fatalf("ListConsumerGroupObservations: %v", err)
	}
	if len(history.GetObservations()) != 1 || !history.GetObservations()[0].GetLastSeenAt().AsTime().Equal(seenAt) {
		t.Fatalf("history view = %+v", history.GetObservations())
	}
}

// TestListConsumerGroupObservationsUnsetTimestampsStayZero pins the
// nil-Timestamp-vs-epoch distinction the handler comments explain.
func TestListConsumerGroupObservationsUnsetTimestampsStayZero(t *testing.T) {
	var captured in.ListConsumerGroupObservationsInput
	fake := &fakeClientSvc{}
	h := newClientHandler(recordingClientSvc{fakeClientSvc: fake, capture: &captured}, "frn")

	if _, err := h.ListConsumerGroupObservations(context.Background(),
		franzv1.ListConsumerGroupObservationsRequest_builder{Name: proto.String("billing")}.Build()); err != nil {
		t.Fatal(err)
	}
	if !captured.From.IsZero() || !captured.To.IsZero() {
		t.Fatalf("From/To = %v/%v, want the zero time when unset", captured.From, captured.To)
	}

	from := time.Unix(1700000000, 0)
	if _, err := h.ListConsumerGroupObservations(context.Background(),
		franzv1.ListConsumerGroupObservationsRequest_builder{
			Name: proto.String("billing"), From: timestamppb.New(from),
		}.Build()); err != nil {
		t.Fatal(err)
	}
	if !captured.From.Equal(from) {
		t.Fatalf("From = %v, want %v", captured.From, from)
	}
}

func TestListClientChannelAccessForwardsPageAndMapping(t *testing.T) {
	fake := &fakeClientSvc{channelAccess: in.ClientChannelAccessPage{
		Access: []in.ClientChannelAccess{{
			AsyncChannel: "orders",
			Effective:    []accesspolicy.Permission{accesspolicy.Read, accesspolicy.Write},
			MatchedBy:    "statement 0 (ALLOW client_frn=acme:client:billing)",
		}},
		NextPageToken: "next-page",
	}}
	h := newClientHandler(fake, "frn")

	resp, err := h.ListClientChannelAccess(context.Background(),
		franzv1.ListClientChannelAccessRequest_builder{Name: proto.String("billing")}.Build())
	if err != nil {
		t.Fatalf("ListClientChannelAccess: %v", err)
	}
	if len(resp.GetAccess()) != 1 || resp.GetAccess()[0].GetAsyncChannel() != "orders" {
		t.Fatalf("access = %+v", resp.GetAccess())
	}
	if len(resp.GetAccess()[0].GetEffective()) != 2 {
		t.Fatalf("effective = %+v", resp.GetAccess()[0].GetEffective())
	}
	if resp.GetPage().GetNextPageToken() != "next-page" {
		t.Fatalf("page token = %q", resp.GetPage().GetNextPageToken())
	}
}

func TestListClientChannelAccessErrorMapping(t *testing.T) {
	h := newClientHandler(&fakeClientSvc{err: errs.NotFoundf("client %q not found", "x")}, "frn")
	_, err := h.ListClientChannelAccess(context.Background(),
		franzv1.ListClientChannelAccessRequest_builder{Name: proto.String("x")}.Build())
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v", status.Code(err))
	}
}

// recordingClientSvc wraps fakeClientSvc to capture the exact input the
// handler builds, without changing fakeClientSvc's simple return-value shape.
type recordingClientSvc struct {
	*fakeClientSvc
	capture *in.ListConsumerGroupObservationsInput
}

func (r recordingClientSvc) ListConsumerGroupObservations(
	ctx context.Context, input in.ListConsumerGroupObservationsInput,
) (in.ObservedGroupPage, error) {
	*r.capture = input
	return r.fakeClientSvc.ListConsumerGroupObservations(ctx, input)
}
