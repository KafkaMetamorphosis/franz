package stream

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
	"google.golang.org/protobuf/proto"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// scriptedStream replays a fixed set of messages, then blocks until released so
// a test can assert on what the watcher did without racing the stream teardown.
type scriptedStream struct {
	msgs    []*franzv1.WatchPartitionAssignmentsResponse
	i       int
	release chan struct{}
}

func (s *scriptedStream) Recv() (*franzv1.WatchPartitionAssignmentsResponse, error) {
	if s.i < len(s.msgs) {
		m := s.msgs[s.i]
		s.i++
		return m, nil
	}
	<-s.release
	return nil, io.EOF
}

func scopeMessage(name, frn string, bootstrap ...string) *franzv1.WatchPartitionAssignmentsResponse {
	return franzv1.WatchPartitionAssignmentsResponse_builder{
		Scope: franzv1.StreamScope_builder{
			Clusters: []*franzv1.StreamScope_Cluster{
				franzv1.StreamScope_Cluster_builder{
					Name:            proto.String(name),
					KafkaClusterFrn: proto.String(frn),
					ConnectionStrings: []*franzv1.ConnectionString{
						franzv1.ConnectionString_builder{BootstrapUrls: bootstrap}.Build(),
					},
				}.Build(),
			},
		}.Build(),
	}.Build()
}

// TestConnectDeliversScope is the regression guard for the scope message being
// logged and dropped: cluster-level indicators (005 §2.1) are only reportable
// for an unplaced cluster if its FRN and brokers reach the reconciler.
func TestConnectDeliversScope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	defer close(release)

	got := make(chan []assign.ScopedCluster, 1)
	w := &Watcher{
		Open: func(context.Context) (AssignmentStream, error) {
			return &scriptedStream{
				msgs:    []*franzv1.WatchPartitionAssignmentsResponse{scopeMessage("local-1", "frn:default:kafka-cluster:local-1", "localhost:9092")},
				release: release,
			}, nil
		},
		Sync:     func(context.Context, map[string]assign.Assignment) error { return nil },
		Scope:    func(_ context.Context, cs []assign.ScopedCluster) { got <- cs; cancel() },
		Log:      quiet(),
		Debounce: 10 * time.Millisecond,
	}

	_ = w.connect(ctx)

	select {
	case clusters := <-got:
		if len(clusters) != 1 {
			t.Fatalf("clusters = %d, want 1", len(clusters))
		}
		c := clusters[0]
		if c.Name != "local-1" || c.FRN != "frn:default:kafka-cluster:local-1" {
			t.Errorf("cluster = %+v", c)
		}
		if len(c.BootstrapServers) != 1 || c.BootstrapServers[0] != "localhost:9092" {
			t.Errorf("bootstrap = %v, want [localhost:9092]", c.BootstrapServers)
		}
	default:
		t.Fatal("scope was never delivered — it is being logged and dropped")
	}
}

// TestConnectWithoutScopeHandlerDoesNotBlock keeps Scope optional: an agent that
// does not care about scope must still process the message.
func TestConnectWithoutScopeHandlerDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	release := make(chan struct{})
	synced := make(chan struct{}, 1)

	w := &Watcher{
		Open: func(context.Context) (AssignmentStream, error) {
			return &scriptedStream{
				msgs: []*franzv1.WatchPartitionAssignmentsResponse{
					scopeMessage("local-1", "frn:default:kafka-cluster:local-1", "localhost:9092"),
					franzv1.WatchPartitionAssignmentsResponse_builder{
						Assignment: franzv1.PartitionAssignment_builder{
							PartitionFrn: proto.String("frn:default:kafka-topic:orders-0"),
							Change:       franzv1.PartitionAssignment_CHANGE_SET.Enum(),
						}.Build(),
					}.Build(),
				},
				release: release,
			}, nil
		},
		Sync: func(context.Context, map[string]assign.Assignment) error {
			select {
			case synced <- struct{}{}:
			default:
			}
			return nil
		},
		Scope:    nil, // deliberately unset
		Log:      quiet(),
		Debounce: 10 * time.Millisecond,
	}

	done := make(chan struct{})
	go func() { _ = w.connect(ctx); close(done) }()

	select {
	case <-synced:
	case <-ctx.Done():
		t.Fatal("the assignment after a scope message was never reconciled")
	}
	close(release)
	<-done
}
