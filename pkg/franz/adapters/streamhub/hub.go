// Package streamhub is the in-memory registry of connected agents and their open
// assignment streams. It carries two independent fan-outs:
//
//   - Cluster Provider assignments (004 ADR §1) — out.AssignmentPublisher,
//     consumed by the WatchClusterAssignments handler.
//   - Resource Provider partition assignments (005 ADR §1.3) —
//     out.PartitionAssignmentPublisher, consumed by the
//     WatchPartitionAssignments handler.
//
// Both keep the same contract: a subscriber that falls behind is dropped rather
// than blocking a publisher, forcing the agent to reconnect and full-resync.
package streamhub

import (
	"sync"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/resource"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// subBuffer is how many undelivered deltas a stream may fall behind before it is
// dropped (forcing the agent to reconnect and full-resync).
const subBuffer = 64

type subscription[T any] struct {
	ch    chan T
	close sync.Once
}

func (s *subscription[T]) shut() { s.close.Do(func() { close(s.ch) }) }

// fanout delivers values of one payload type to the open streams of a named
// agent. The zero value is not usable — construct with newFanout.
type fanout[T any] struct {
	mu     sync.Mutex
	nextID int
	subs   map[string]map[int]*subscription[T] // agentName -> id -> sub
}

func newFanout[T any]() *fanout[T] {
	return &fanout[T]{subs: map[string]map[int]*subscription[T]{}}
}

func (f *fanout[T]) subscribe(agentName string) (<-chan T, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := f.nextID
	f.nextID++
	sub := &subscription[T]{ch: make(chan T, subBuffer)}
	if f.subs[agentName] == nil {
		f.subs[agentName] = map[int]*subscription[T]{}
	}
	f.subs[agentName][id] = sub

	return sub.ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if m := f.subs[agentName]; m != nil {
			if s, ok := m[id]; ok {
				s.shut()
				delete(m, id)
			}
			if len(m) == 0 {
				delete(f.subs, agentName)
			}
		}
	}
}

func (f *fanout[T]) publish(agentName string, v T) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for id, sub := range f.subs[agentName] {
		select {
		case sub.ch <- v:
		default:
			sub.shut()
			delete(f.subs[agentName], id)
		}
	}
	if len(f.subs[agentName]) == 0 {
		delete(f.subs, agentName)
	}
}

func (f *fanout[T]) connected() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.subs))
	for name := range f.subs {
		names = append(names, name)
	}
	return names
}

// Hub fans assignment changes out to the streams of the agents concerned.
type Hub struct {
	clusters   *fanout[provider.Assignment]
	partitions *fanout[resource.PartitionAssignment]
}

var (
	_ out.AssignmentPublisher          = (*Hub)(nil)
	_ out.PartitionAssignmentPublisher = (*Hub)(nil)
)

// New returns an empty hub.
func New() *Hub {
	return &Hub{
		clusters:   newFanout[provider.Assignment](),
		partitions: newFanout[resource.PartitionAssignment](),
	}
}

// Subscribe registers a Cluster Provider stream for agentName. The returned
// channel delivers assignment deltas until unsubscribe is called or the
// subscriber lags past subBuffer (channel closed). Always call unsubscribe
// (defer).
func (h *Hub) Subscribe(agentName string) (<-chan provider.Assignment, func()) {
	return h.clusters.subscribe(agentName)
}

// PublishAssignment delivers a to every open Cluster Provider stream of
// agentName. A stream that has lagged past its buffer is dropped (channel
// closed) so the agent reconnects and re-syncs from the full set.
func (h *Hub) PublishAssignment(agentName string, a provider.Assignment) {
	h.clusters.publish(agentName, a)
}

// ConnectedAgents lists the agent names with at least one open Cluster Provider
// stream. For diagnostics / a future "connected" flag.
func (h *Hub) ConnectedAgents() []string { return h.clusters.connected() }

// SubscribePartitions registers a Resource Provider stream for agentName, with
// the same lag-drop contract as Subscribe.
func (h *Hub) SubscribePartitions(agentName string) (<-chan resource.PartitionAssignment, func()) {
	return h.partitions.subscribe(agentName)
}

// PublishPartitionAssignment delivers a to every open Resource Provider stream
// of agentName (005 ADR §1.3).
func (h *Hub) PublishPartitionAssignment(agentName string, a resource.PartitionAssignment) {
	h.partitions.publish(agentName, a)
}

// ConnectedPartitionAgents lists the agent names with at least one open Resource
// Provider stream. The partition-assignment notifier only pushes to these: an
// agent that is not connected picks the change up in the full set it receives on
// its next reconnect (005 ADR §1.3 "Reconnect = full resync").
func (h *Hub) ConnectedPartitionAgents() []string { return h.partitions.connected() }
