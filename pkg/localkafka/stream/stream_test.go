package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/assign"
)

func setMsg(name string, change franzv1.ClusterAssignment_Change) *franzv1.WatchClusterAssignmentsResponse {
	return franzv1.WatchClusterAssignmentsResponse_builder{
		Assignment: franzv1.ClusterAssignment_builder{
			Change:      change.Enum(),
			ClusterName: &name,
			ClusterFrn:  strp("frn:default:kafka-cluster:" + name),
		}.Build(),
	}.Build()
}

func strp(s string) *string { return &s }

// scriptStream yields queued messages, then (after holdAfterMsgs, mimicking a
// stream that stays open past the initial set) a terminal error.
type scriptStream struct {
	mu            sync.Mutex
	msgs          []*franzv1.WatchClusterAssignmentsResponse
	end           error
	holdAfterMsgs time.Duration
	held          bool
}

func (s *scriptStream) Recv() (*franzv1.WatchClusterAssignmentsResponse, error) {
	s.mu.Lock()
	if len(s.msgs) > 0 {
		m := s.msgs[0]
		s.msgs = s.msgs[1:]
		s.mu.Unlock()
		return m, nil
	}
	hold := s.holdAfterMsgs
	first := !s.held
	s.held = true
	end := s.end
	s.mu.Unlock()

	if first && hold > 0 {
		time.Sleep(hold)
	}
	if end != nil {
		return nil, end
	}
	time.Sleep(50 * time.Millisecond)
	return nil, io.EOF
}

func TestConnectDebouncesInitialSet(t *testing.T) {
	var mu sync.Mutex
	var syncs []map[string]assign.Assignment

	w := &Watcher{
		Open: func(context.Context) (AssignmentStream, error) {
			return &scriptStream{
				msgs: []*franzv1.WatchClusterAssignmentsResponse{
					setMsg("a", franzv1.ClusterAssignment_CHANGE_SET),
					setMsg("b", franzv1.ClusterAssignment_CHANGE_SET),
					setMsg("c", franzv1.ClusterAssignment_CHANGE_PAUSED),
				},
				holdAfterMsgs: 120 * time.Millisecond,
				end:           errors.New("closed"),
			}, nil
		},
		Sync: func(_ context.Context, d map[string]assign.Assignment) error {
			mu.Lock()
			cp := map[string]assign.Assignment{}
			for k, v := range d {
				cp[k] = v
			}
			syncs = append(syncs, cp)
			mu.Unlock()
			return nil
		},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Debounce: 30 * time.Millisecond,
		Resync:   time.Hour,
	}

	err := w.connect(context.Background())
	if err == nil {
		t.Fatal("expected the terminal stream error to propagate")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(syncs) != 1 {
		t.Fatalf("got %d syncs, want 1 (debounced)", len(syncs))
	}
	got := syncs[0]
	if len(got) != 3 || got["a"].Change != assign.ChangeSet || got["c"].Change != assign.ChangePaused {
		t.Fatalf("sync map = %+v", got)
	}
}

func TestConnectPrunesRemovedAfterSync(t *testing.T) {
	var mu sync.Mutex
	var syncs []map[string]assign.Assignment

	// a is SET then REMOVED in the same burst; after the sync that carried
	// REMOVED, a resync (forced) should no longer see it.
	stream := &scriptStream{
		msgs: []*franzv1.WatchClusterAssignmentsResponse{
			setMsg("a", franzv1.ClusterAssignment_CHANGE_SET),
			setMsg("a", franzv1.ClusterAssignment_CHANGE_REMOVED),
		},
	}
	w := &Watcher{
		Open: func(context.Context) (AssignmentStream, error) { return stream, nil },
		Sync: func(_ context.Context, d map[string]assign.Assignment) error {
			mu.Lock()
			cp := map[string]assign.Assignment{}
			for k, v := range d {
				cp[k] = v
			}
			syncs = append(syncs, cp)
			mu.Unlock()
			return nil
		},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Debounce: 20 * time.Millisecond,
		Resync:   40 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = w.connect(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(syncs) < 2 {
		t.Fatalf("want at least a debounce sync + a resync, got %d", len(syncs))
	}
	if _, still := syncs[len(syncs)-1]["a"]; still {
		t.Errorf("REMOVED cluster still in a later sync: %+v", syncs[len(syncs)-1])
	}
	if syncs[0]["a"].Change != assign.ChangeRemoved {
		t.Errorf("first sync should carry the REMOVED so the reconciler tears down: %+v", syncs[0])
	}
}
