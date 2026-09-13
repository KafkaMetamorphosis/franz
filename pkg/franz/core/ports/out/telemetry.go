package out

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
)

// ObservedGroupQuery parameterises the *current view* read — 003.14's "distinct
// (group, topic) with the latest observed_at". ClientFRN narrows it to one
// client's groups and is what ClientService.ListObservedConsumerGroups passes;
// empty means the whole realm.
type ObservedGroupQuery struct {
	RealmID   uuid.UUID
	ClientFRN string
	Limit     int
	// AfterCursor is the opaque "<group>|<topic>" position of the last row of the
	// previous page, "" ⇒ first page.
	AfterCursor string
}

// ObservationQuery parameterises the *history* read — the raw sightings behind
// the current view, newest first. From/To are optional bounds on observed_at.
type ObservationQuery struct {
	RealmID     uuid.UUID
	ClientFRN   string
	From        time.Time
	To          time.Time
	Limit       int
	AfterCursor string
}

// ObservationPage is one page of either read. LastCursor is "" when there are no
// more rows.
type ObservationPage struct {
	Observations []*consumergroup.Observation
	LastCursor   string
}

// ObservedConsumerGroupRepository persists the append-only
// observed_consumer_group time series (003.14).
//
// The two reads back ClientService.ListObservedConsumerGroups (current) and
// ListConsumerGroupObservations (history). Those RPCs live on ClientService,
// which deliverable 16 builds and registers; deliverable 15 owns the write side
// and the store beneath them.
type ObservedConsumerGroupRepository interface {
	// Append writes a batch in one round trip and returns how many rows landed.
	Append(ctx context.Context, observations []*consumergroup.Observation) (int, error)

	// ListCurrent returns the newest sighting per (group, kafka_topic), ordered by
	// group then topic ascending so paging is stable under concurrent ingest.
	ListCurrent(ctx context.Context, q ObservedGroupQuery) (ObservationPage, error)

	// ListObservations returns the raw sightings in the requested window, newest
	// first.
	ListObservations(ctx context.Context, q ObservationQuery) (ObservationPage, error)

	// PruneOlderThan deletes sightings with observed_at < cutoff (003.14 — nightly
	// 30-day prune) and returns the count removed.
	PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}
