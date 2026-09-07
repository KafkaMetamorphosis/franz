package grpcgateway

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

type fakeChannelSvc struct {
	created in.CreateChannelInput
	updated in.UpdateChannelInput
	policy  accesspolicy.Policy
	ret     *channel.AsyncChannel
	err     error
}

func (f *fakeChannelSvc) Create(_ context.Context, i in.CreateChannelInput) (*channel.AsyncChannel, error) {
	f.created = i
	return f.ret, f.err
}
func (f *fakeChannelSvc) Get(context.Context, string) (*channel.AsyncChannel, error) {
	return f.ret, f.err
}
func (f *fakeChannelSvc) List(context.Context, in.ListChannelsInput) (in.ChannelPage, error) {
	return in.ChannelPage{Channels: []*channel.AsyncChannel{f.ret}}, f.err
}
func (f *fakeChannelSvc) Update(_ context.Context, i in.UpdateChannelInput) (*channel.AsyncChannel, error) {
	f.updated = i
	return f.ret, f.err
}
func (f *fakeChannelSvc) Delete(context.Context, string) error { return f.err }
func (f *fakeChannelSvc) Pause(context.Context, string) (*channel.AsyncChannel, error) {
	return f.ret, f.err
}
func (f *fakeChannelSvc) Resume(context.Context, string) (*channel.AsyncChannel, error) {
	return f.ret, f.err
}
func (f *fakeChannelSvc) SetAccessPolicy(_ context.Context, _ string, p accesspolicy.Policy) (*channel.AsyncChannel, error) {
	f.policy = p
	return f.ret, f.err
}

func sampleChannel(t *testing.T) *channel.AsyncChannel {
	t.Helper()
	c, err := channel.New(realm.Realm{Slug: "default"}, "orders", channel.TypeKafkaTopic, 3, map[string]string{"team": "x"}, accesspolicy.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	c.CreatedAt = time.Unix(1700000000, 0)
	return c
}

func newChannelHandler(svc in.AsyncChannelService) *asyncChannelHandler {
	return &asyncChannelHandler{svc: svc, codec: frn.MustCodec("frn")}
}

func TestCreateAsyncChannelForwardsAndRenders(t *testing.T) {
	fake := &fakeChannelSvc{ret: sampleChannel(t)}
	h := newChannelHandler(fake)
	resp, err := h.CreateAsyncChannel(context.Background(), franzv1.CreateAsyncChannelRequest_builder{
		Name:              proto.String("orders"),
		Type:              franzv1.ChannelType_CHANNEL_TYPE_KAFKA_TOPIC.Enum(),
		ChannelPartitions: proto.Int32(3),
		AccessPolicy: franzv1.AccessPolicy_builder{Statements: []*franzv1.AccessPolicyStatement{
			franzv1.AccessPolicyStatement_builder{
				Effect:      franzv1.Effect_EFFECT_ALLOW.Enum(),
				Principal:   franzv1.Principal_builder{ClientFrn: proto.String("frn:default:client:a")}.Build(),
				Permissions: []franzv1.Permission{franzv1.Permission_PERMISSION_READ},
			}.Build(),
		}}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("CreateAsyncChannel: %v", err)
	}
	if fake.created.ChannelPartitions != 3 || fake.created.Type != channel.TypeKafkaTopic {
		t.Errorf("service saw %+v", fake.created)
	}
	if len(fake.created.AccessPolicy.Statements) != 1 ||
		fake.created.AccessPolicy.Statements[0].Effect != accesspolicy.Allow {
		t.Errorf("policy not forwarded: %+v", fake.created.AccessPolicy)
	}
	if resp.GetAsyncChannel().GetFrn() != "frn:default:async-channel:orders" {
		t.Errorf("frn = %q", resp.GetAsyncChannel().GetFrn())
	}
}

func TestUpdateAsyncChannelRejectsNonLabelMask(t *testing.T) {
	h := newChannelHandler(&fakeChannelSvc{ret: sampleChannel(t)})
	_, err := h.UpdateAsyncChannel(context.Background(), franzv1.UpdateAsyncChannelRequest_builder{
		Name:       proto.String("orders"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"channel_partitions"}},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("channel_partitions in mask → %v", status.Code(err))
	}
}

func TestUpdateAsyncChannelLabels(t *testing.T) {
	fake := &fakeChannelSvc{ret: sampleChannel(t)}
	h := newChannelHandler(fake)
	_, err := h.UpdateAsyncChannel(context.Background(), franzv1.UpdateAsyncChannelRequest_builder{
		Name:       proto.String("orders"),
		Labels:     map[string]string{"env": "prod"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}.Build())
	if err != nil {
		t.Fatalf("UpdateAsyncChannel: %v", err)
	}
	if fake.updated.Labels == nil || (*fake.updated.Labels)["env"] != "prod" {
		t.Errorf("labels not forwarded: %+v", fake.updated)
	}
}

func TestSetAccessPolicyForwards(t *testing.T) {
	fake := &fakeChannelSvc{ret: sampleChannel(t)}
	h := newChannelHandler(fake)
	_, err := h.SetAccessPolicy(context.Background(), franzv1.SetAccessPolicyRequest_builder{
		Name: proto.String("orders"),
		AccessPolicy: franzv1.AccessPolicy_builder{Statements: []*franzv1.AccessPolicyStatement{
			franzv1.AccessPolicyStatement_builder{
				Effect:      franzv1.Effect_EFFECT_DENY.Enum(),
				Principal:   franzv1.Principal_builder{Labels: proto.String("team=x")}.Build(),
				Permissions: []franzv1.Permission{franzv1.Permission_PERMISSION_WRITE},
			}.Build(),
		}}.Build(),
	}.Build())
	if err != nil {
		t.Fatalf("SetAccessPolicy: %v", err)
	}
	if len(fake.policy.Statements) != 1 || fake.policy.Statements[0].Effect != accesspolicy.Deny ||
		fake.policy.Statements[0].Principal.LabelSelector != "team=x" {
		t.Errorf("policy not forwarded: %+v", fake.policy)
	}
}

func TestListChannelClientsUnimplemented(t *testing.T) {
	h := newChannelHandler(&fakeChannelSvc{})
	_, err := h.ListChannelClients(context.Background(), franzv1.ListChannelClientsRequest_builder{
		Name: proto.String("orders"),
	}.Build())
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %v", status.Code(err))
	}
}

func TestAsyncChannelErrorMapping(t *testing.T) {
	h := newChannelHandler(&fakeChannelSvc{err: errs.Existsf("async channel %q already exists", "orders")})
	_, err := h.CreateAsyncChannel(context.Background(), franzv1.CreateAsyncChannelRequest_builder{
		Name: proto.String("orders"), Type: franzv1.ChannelType_CHANNEL_TYPE_KAFKA_TOPIC.Enum(),
		ChannelPartitions: proto.Int32(1),
	}.Build())
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("code = %v", status.Code(err))
	}
}
