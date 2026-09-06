// Package stream watches WatchClusterAssignments with reconnect + backoff and
// hands the settled assignment set to a sync callback (ADR 004 §1, §6). The
// stream sends one message per assignment (the full set on open, then deltas);
// this package debounces them into a single reconcile.
package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/assign"
)

// AssignmentStream is the client side of WatchClusterAssignments.
type AssignmentStream interface {
	Recv() (*franzv1.WatchClusterAssignmentsResponse, error)
}

// Opener opens a fresh assignment stream.
type Opener func(ctx context.Context) (AssignmentStream, error)

// SyncFunc converges to the given assignment world.
type SyncFunc func(ctx context.Context, desired map[string]assign.Assignment) error

// Watcher drives the stream lifecycle.
type Watcher struct {
	Open       Opener
	Sync       SyncFunc
	Log        *slog.Logger
	BackoffMax time.Duration
	Debounce   time.Duration
	// Resync forces a Sync even without a stream delta (drift correction).
	Resync time.Duration
}

// Run blocks until ctx is done. Each stream connection rebuilds the assignment
// map from scratch; the incoming full set defines the world.
func (w *Watcher) Run(ctx context.Context) error {
	backoff := time.Second
	for ctx.Err() == nil {
		err := w.connect(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			w.Log.Warn("assignment stream dropped; reconnecting", "err", err, "in", backoff)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > w.BackoffMax {
			backoff = w.BackoffMax
		}
	}
	return ctx.Err()
}

func (w *Watcher) connect(ctx context.Context) error {
	stream, err := w.Open(ctx)
	if err != nil {
		return err
	}
	w.Log.Info("assignment stream open")

	desired := map[string]assign.Assignment{}
	removedPending := map[string]bool{}

	msgs := make(chan *franzv1.ClusterAssignment, 32)
	recvErr := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if a := resp.GetAssignment(); a != nil {
				select {
				case msgs <- a:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	debounce := time.NewTimer(time.Hour)
	debounce.Stop()
	resync := time.NewTicker(w.Resync)
	defer resync.Stop()

	doSync := func() {
		snapshot := make(map[string]assign.Assignment, len(desired))
		for k, v := range desired {
			snapshot[k] = v
		}
		if err := w.Sync(ctx, snapshot); err != nil {
			w.Log.Warn("reconcile failed", "err", err)
			return
		}
		for k := range removedPending {
			delete(desired, k)
			delete(removedPending, k)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return errors.New("server closed the stream")
			}
			return err
		case a := <-msgs:
			as := assign.FromProto(a)
			desired[as.ClusterName] = as
			if as.Change == assign.ChangeRemoved {
				removedPending[as.ClusterName] = true
			}
			debounce.Reset(w.Debounce)
		case <-debounce.C:
			doSync()
		case <-resync.C:
			doSync()
		}
	}
}
