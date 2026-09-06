package streamhub

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/provider"
)

func set(name string) provider.Assignment {
	return provider.Assignment{Change: provider.ChangeSet, ClusterName: name}
}

func TestPublishReachesSubscribers(t *testing.T) {
	h := New()
	ch, unsub := h.Subscribe("prov-1")
	defer unsub()

	h.PublishAssignment("prov-1", set("east-1"))
	h.PublishAssignment("prov-2", set("other")) // different agent, ignored

	select {
	case a := <-ch:
		if a.ClusterName != "east-1" {
			t.Errorf("got %q", a.ClusterName)
		}
	default:
		t.Fatal("expected a delivered assignment")
	}
	select {
	case a := <-ch:
		t.Fatalf("unexpected second message %q", a.ClusterName)
	default:
	}
}

func TestMultipleSubscribersSameAgent(t *testing.T) {
	h := New()
	a, ua := h.Subscribe("prov-1")
	b, ub := h.Subscribe("prov-1")
	defer ua()
	defer ub()

	h.PublishAssignment("prov-1", set("east-1"))
	if (<-a).ClusterName != "east-1" || (<-b).ClusterName != "east-1" {
		t.Error("both subscribers should receive the message")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	h := New()
	ch, unsub := h.Subscribe("prov-1")
	unsub()

	if _, ok := <-ch; ok {
		t.Error("channel should be closed after unsubscribe")
	}
	h.PublishAssignment("prov-1", set("east-1")) // must not panic
	if got := h.ConnectedAgents(); len(got) != 0 {
		t.Errorf("ConnectedAgents = %v, want empty", got)
	}
}

func TestLaggingSubscriberIsDropped(t *testing.T) {
	h := New()
	ch, unsub := h.Subscribe("prov-1")
	defer unsub()

	// fill the buffer + 1
	for i := 0; i < subBuffer+5; i++ {
		h.PublishAssignment("prov-1", set("east-1"))
	}
	// drain what buffered; the channel must eventually be closed
	closed := false
	for range subBuffer + 5 {
		if _, ok := <-ch; !ok {
			closed = true
			break
		}
	}
	if !closed {
		t.Error("a lagging subscriber's channel should be closed")
	}
	if len(h.ConnectedAgents()) != 0 {
		t.Error("dropped subscriber should be removed from the hub")
	}
}
