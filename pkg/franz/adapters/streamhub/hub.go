// Package streamhub is the in-memory registry of connected Cluster Provider
// agents and their open assignment streams (004 ADR §1). It implements
// out.AssignmentPublisher; the WatchClusterAssignments handler subscribes.
package streamhub

import (
	"sync"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// subBuffer is how many undelivered assignment deltas a stream may fall behind
// before it is dropped (forcing the agent to reconnect and full-resync).
const subBuffer = 64

type subscription struct {
	ch    chan provider.Assignment
	close sync.Once
}

func (s *subscription) shut() { s.close.Do(func() { close(s.ch) }) }

// Hub fans assignment changes out to the streams of the owning agent.
type Hub struct {
	mu     sync.Mutex
	nextID int
	subs   map[string]map[int]*subscription // agentName -> id -> sub
}

var _ out.AssignmentPublisher = (*Hub)(nil)

// New returns an empty hub.
func New() *Hub { return &Hub{subs: map[string]map[int]*subscription{}} }

// Subscribe registers a stream for agentName. The returned channel delivers
// assignment deltas until unsubscribe is called or the subscriber lags past
// subBuffer (channel closed). Always call unsubscribe (defer).
func (h *Hub) Subscribe(agentName string) (<-chan provider.Assignment, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	id := h.nextID
	h.nextID++
	sub := &subscription{ch: make(chan provider.Assignment, subBuffer)}
	if h.subs[agentName] == nil {
		h.subs[agentName] = map[int]*subscription{}
	}
	h.subs[agentName][id] = sub

	return sub.ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if m := h.subs[agentName]; m != nil {
			if s, ok := m[id]; ok {
				s.shut()
				delete(m, id)
			}
			if len(m) == 0 {
				delete(h.subs, agentName)
			}
		}
	}
}

// PublishAssignment delivers a to every open stream of agentName. A stream that
// has lagged past its buffer is dropped (channel closed) so the agent
// reconnects and re-syncs from the full set.
func (h *Hub) PublishAssignment(agentName string, a provider.Assignment) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id, sub := range h.subs[agentName] {
		select {
		case sub.ch <- a:
		default:
			sub.shut()
			delete(h.subs[agentName], id)
		}
	}
	if len(h.subs[agentName]) == 0 {
		delete(h.subs, agentName)
	}
}

// ConnectedAgents lists the agent names with at least one open stream. For
// diagnostics / a future "connected" flag.
func (h *Hub) ConnectedAgents() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := make([]string, 0, len(h.subs))
	for name := range h.subs {
		names = append(names, name)
	}
	return names
}
