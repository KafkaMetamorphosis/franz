package grpcgateway

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

type fakeClusterSvc struct {
	created  in.CreateClusterInput
	updated  in.UpdateClusterInput
	ret      *cluster.Cluster
	err      error
	listResp in.ClusterPage
}

func (f *fakeClusterSvc) Create(_ context.Context, i in.CreateClusterInput) (*cluster.Cluster, error) {
	f.created = i
	return f.ret, f.err
}
func (f *fakeClusterSvc) Get(context.Context, string) (*cluster.Cluster, error) { return f.ret, f.err }
func (f *fakeClusterSvc) List(context.Context, in.ListClustersInput) (in.ClusterPage, error) {
	return f.listResp, f.err
}
func (f *fakeClusterSvc) Update(_ context.Context, i in.UpdateClusterInput) (*cluster.Cluster, error) {
	f.updated = i
	return f.ret, f.err
}
func (f *fakeClusterSvc) Delete(context.Context, string) error { return f.err }
func (f *fakeClusterSvc) Pause(context.Context, string) (*cluster.Cluster, error) {
	return f.ret, f.err
}
func (f *fakeClusterSvc) Resume(context.Context, string) (*cluster.Cluster, error) {
	return f.ret, f.err
}

func sampleCluster(t *testing.T) *cluster.Cluster {
	t.Helper()
	c, err := cluster.New(
		realm.Realm{Slug: "default"},
		"east-1",
		[]cluster.ConnectionString{{BootstrapURLs: []string{"b:9092"}, Type: cluster.ConnectionPlaintext}},
		map[string]string{"env": "prod"},
		map[string]string{"retention.ms": "1000"},
		"agent-x",
	)
	if err != nil {
		t.Fatal(err)
	}
	c.CreatedAt = time.Unix(1700000000, 0)
	c.UpdatedAt = time.Unix(1700000001, 0)
	return c
}

func newHandler(svc in.KafkaClusterService, prefix string) *kafkaClusterHandler {
	return &kafkaClusterHandler{svc: svc, codec: frn.MustCodec(prefix)}
}

func TestCreateRendersFRNWithConfiguredPrefix(t *testing.T) {
	fake := &fakeClusterSvc{ret: sampleCluster(t)}
	h := newHandler(fake, "acme")

	resp, err := h.CreateKafkaCluster(context.Background(), franzv1.CreateKafkaClusterRequest_builder{
		Name:              proto.String("east-1"),
		ConnectionStrings: []*franzv1.ConnectionString{franzv1.ConnectionString_builder{BootstrapUrls: []string{"b:9092"}}.Build()},
		Labels:            map[string]string{"env": "prod"},
	}.Build())
	if err != nil {
		t.Fatalf("CreateKafkaCluster: %v", err)
	}
	kc := resp.GetKafkaCluster()
	if kc.GetFrn() != "acme:default:kafka-cluster:east-1" {
		t.Errorf("frn = %q, want acme-prefixed", kc.GetFrn())
	}
	if kc.GetState() != franzv1.KafkaClusterState_KAFKA_CLUSTER_STATE_ACTIVE {
		t.Errorf("state = %v", kc.GetState())
	}
	if fake.created.Name != "east-1" || len(fake.created.ConnectionStrings) != 1 {
		t.Errorf("service saw wrong input: %+v", fake.created)
	}
}

func TestDomainErrorMapsToStatus(t *testing.T) {
	fake := &fakeClusterSvc{err: errs.Existsf("kafka cluster %q already exists", "east-1")}
	h := newHandler(fake, "frn")

	_, err := h.CreateKafkaCluster(context.Background(), franzv1.CreateKafkaClusterRequest_builder{
		Name: proto.String("east-1"),
	}.Build())
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestUpdateOnlyForwardsMaskedFields(t *testing.T) {
	fake := &fakeClusterSvc{ret: sampleCluster(t)}
	h := newHandler(fake, "frn")

	_, err := h.UpdateKafkaCluster(context.Background(), franzv1.UpdateKafkaClusterRequest_builder{
		Name:                 proto.String("east-1"),
		Labels:               map[string]string{"env": "staging"},
		ClusterConfiguration: map[string]string{"x": "y"},
		UpdateMask:           &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}.Build())
	if err != nil {
		t.Fatalf("UpdateKafkaCluster: %v", err)
	}
	if fake.updated.Labels == nil {
		t.Error("labels should have been forwarded")
	}
	if fake.updated.Configuration != nil {
		t.Error("cluster_configuration was not masked but got forwarded")
	}
	if fake.updated.ConnectionStrings != nil || fake.updated.ProviderAgent != nil {
		t.Error("unmasked fields forwarded")
	}
}

func TestUpdateRejectsEmptyMask(t *testing.T) {
	h := newHandler(&fakeClusterSvc{}, "frn")
	_, err := h.UpdateKafkaCluster(context.Background(), franzv1.UpdateKafkaClusterRequest_builder{
		Name: proto.String("east-1"),
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty mask code = %v", status.Code(err))
	}
}

func TestUpdateRejectsImmutableFieldInMask(t *testing.T) {
	h := newHandler(&fakeClusterSvc{}, "frn")
	_, err := h.UpdateKafkaCluster(context.Background(), franzv1.UpdateKafkaClusterRequest_builder{
		Name:       proto.String("east-1"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	}.Build())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("immutable mask code = %v", status.Code(err))
	}
}

func TestListMapsPageResponse(t *testing.T) {
	fake := &fakeClusterSvc{listResp: in.ClusterPage{
		Clusters:      []*cluster.Cluster{sampleCluster(t)},
		NextPageToken: "tok",
		TotalSize:     1,
	}}
	h := newHandler(fake, "frn")

	resp, err := h.ListKafkaClusters(context.Background(), franzv1.ListKafkaClustersRequest_builder{}.Build())
	if err != nil {
		t.Fatalf("ListKafkaClusters: %v", err)
	}
	if len(resp.GetKafkaClusters()) != 1 || resp.GetPage().GetNextPageToken() != "tok" {
		t.Errorf("bad list response: %+v", resp)
	}
}
