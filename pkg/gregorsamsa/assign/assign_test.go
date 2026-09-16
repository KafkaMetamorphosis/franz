package assign_test

import (
	"testing"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
	"google.golang.org/protobuf/proto"
)

func cluster(name, frn string, bootstrap ...string) *franzv1.StreamScope_Cluster {
	b := franzv1.StreamScope_Cluster_builder{
		Name:            proto.String(name),
		KafkaClusterFrn: proto.String(frn),
	}
	if len(bootstrap) > 0 {
		b.ConnectionStrings = []*franzv1.ConnectionString{
			franzv1.ConnectionString_builder{BootstrapUrls: bootstrap}.Build(),
		}
	}
	return b.Build()
}

func TestScopeFromProtoFlattensConnectionStrings(t *testing.T) {
	got := assign.ScopeFromProto(franzv1.StreamScope_builder{
		Clusters: []*franzv1.StreamScope_Cluster{
			cluster("local-1", "frn:default:kafka-cluster:local-1", "a:9092", "b:9092"),
			cluster("carol", "frn:default:kafka-cluster:carol", "c:9092"),
		},
	}.Build())

	if len(got) != 2 {
		t.Fatalf("clusters = %d, want 2", len(got))
	}
	if got[0].Name != "local-1" || got[0].FRN != "frn:default:kafka-cluster:local-1" {
		t.Errorf("first = %+v", got[0])
	}
	if len(got[0].BootstrapServers) != 2 ||
		got[0].BootstrapServers[0] != "a:9092" || got[0].BootstrapServers[1] != "b:9092" {
		t.Errorf("bootstrap = %v, want both urls flattened in order", got[0].BootstrapServers)
	}
}

// TestScopeFromProtoDropsANamelessCluster — the cluster name is the admin
// cache's key, so a nameless entry could only overwrite another's slot.
func TestScopeFromProtoDropsANamelessCluster(t *testing.T) {
	got := assign.ScopeFromProto(franzv1.StreamScope_builder{
		Clusters: []*franzv1.StreamScope_Cluster{
			cluster("", "frn:default:kafka-cluster:ghost", "a:9092"),
			cluster("local-1", "frn:default:kafka-cluster:local-1", "b:9092"),
		},
	}.Build())

	if len(got) != 1 || got[0].Name != "local-1" {
		t.Fatalf("got %+v, want only local-1", got)
	}
}

func TestScopeFromProtoHandlesNilAndEmpty(t *testing.T) {
	if got := assign.ScopeFromProto(nil); got != nil {
		t.Errorf("ScopeFromProto(nil) = %v, want nil", got)
	}
	// An agent whose selectors match nothing: empty, not an error.
	if got := assign.ScopeFromProto(franzv1.StreamScope_builder{}.Build()); len(got) != 0 {
		t.Errorf("ScopeFromProto(empty) = %v, want no clusters", got)
	}
}

// TestScopeFromProtoKeepsAClusterWithNoBrokers documents that filtering an
// unusable cluster is the reconciler's job, not the mapper's — it warns about
// one it cannot dial, which is more useful than silently losing it here.
func TestScopeFromProtoKeepsAClusterWithNoBrokers(t *testing.T) {
	got := assign.ScopeFromProto(franzv1.StreamScope_builder{
		Clusters: []*franzv1.StreamScope_Cluster{
			cluster("local-1", "frn:default:kafka-cluster:local-1"),
		},
	}.Build())

	if len(got) != 1 {
		t.Fatalf("clusters = %d, want 1", len(got))
	}
	if len(got[0].BootstrapServers) != 0 {
		t.Errorf("bootstrap = %v, want empty", got[0].BootstrapServers)
	}
}
