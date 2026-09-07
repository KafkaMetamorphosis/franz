// Package stream watches WatchPartitionAssignments with reconnect + exponential
// backoff and hands the settled desired world to a sync callback (005 ADR §1.3).
//
// Each connection rebuilds the world from scratch: the full set Franz sends on
// open *is* the world, and later messages are deltas on top of it. Messages are
// debounced into one reconcile so a burst (the initial set, a channel with many
// shards) costs a single pass.
package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
)

// AssignmentStream is the client side of WatchPartitionAssignments.
type AssignmentStream interface {
	Recv() (*franzv1.WatchPartitionAssignmentsResponse, error)
}

// Opener opens a fresh assignment stream.
type Opener func(ctx context.Context) (AssignmentStream, error)

// SyncFunc converges to the given desired world, keyed by partition FRN.
type SyncFunc func(ctx context.Context, world map[string]assign.Assignment) error

// Watcher drives the stream lifecycle.
type Watcher struct {
	Open Opener
	Sync SyncFunc
	Log  *slog.Logger
	// BackoffMin / BackoffMax bound the reconnect backoff (005 ADR §1.6:
	// 5s → 120s).
	BackoffMin time.Duration
	BackoffMax time.Duration
	// Debounce is how long to wait for the message burst to settle before
	// reconciling.
	Debounce time.Duration
}

// Run blocks until ctx is done, reconnecting with exponential backoff whenever
// the stream drops. It never mutates Kafka on guesswork while Franz is
// unreachable — it simply has nothing new to converge to.
func (w *Watcher) Run(ctx context.Context) error {
	backoff := w.BackoffMin
	if backoff <= 0 {
		backoff = 5 * time.Second
	}
	for ctx.Err() == nil {
		err := w.connect(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.Log.Warn("assignment stream dropped; reconnecting", "err", err, "in", backoff)

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
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := w.Open(streamCtx)
	if err != nil {
		return err
	}
	w.Log.Info("connected to franz; watching partition assignments")

	world := map[string]assign.Assignment{}

	msgs := make(chan *franzv1.PartitionAssignment, 64)
	recvErr := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if sc := resp.GetScope(); sc != nil {
				w.logScope(sc)
				continue
			}
			if a := resp.GetAssignment(); a != nil {
				select {
				case msgs <- a:
				case <-streamCtx.Done():
					return
				}
			}
		}
	}()

	debounce := time.NewTimer(time.Hour)
	debounce.Stop()
	defer debounce.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return errors.New("server closed the stream")
			}
			return err
		case msg := <-msgs:
			a := assign.FromProto(msg)
			if a.PartitionFRN == "" {
				continue
			}
			world[a.PartitionFRN] = a
			debounce.Reset(w.Debounce)
		case <-debounce.C:
			snapshot := make(map[string]assign.Assignment, len(world))
			for k, v := range world {
				snapshot[k] = v
			}
			if err := w.Sync(ctx, snapshot); err != nil {
				w.Log.Warn("reconcile failed", "err", err)
			}
			// A partition the agent stopped managing (PAUSED, scope loss) or
			// deleted is dropped from the world so a later SET is treated as new.
			for frn, a := range world {
				if a.Change == assign.ChangePaused || a.IsScopeLoss() {
					delete(world, frn)
				}
			}
		}
	}
}

// logScope reports the clusters Franz says this agent is responsible for — the
// first message on every (re)connected stream. Zero clusters means the agent's
// `franz.placement-selector/*` labels match nothing (or it has none).
func (w *Watcher) logScope(sc *franzv1.StreamScope) {
	clusters := sc.GetClusters()
	if len(clusters) == 0 {
		w.Log.Warn("no Kafka clusters in scope — check this agent's franz.placement-selector/* labels against the clusters' franz.placement/* labels")
		return
	}
	names := make([]string, len(clusters))
	bootstraps := make([]string, len(clusters))
	for i, c := range clusters {
		names[i] = c.GetName()
		var urls []string
		for _, cs := range c.GetConnectionStrings() {
			urls = append(urls, cs.GetBootstrapUrls()...)
		}
		bootstraps[i] = c.GetName() + "=" + strings.Join(urls, ",")
	}
	w.Log.Info("clusters in scope", "count", len(names), "clusters", names, "bootstrap", bootstraps)
}
