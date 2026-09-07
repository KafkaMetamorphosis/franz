// Package indicator is the telemetry-ingest domain (003.14): one observation of
// one indicator on one resource at one time. Deliverable 12 needs the sample
// half only — Gregor Samsa publishes structural indicators over TelemetryService
// (005 ADR Part 2). The `Indicator` registry entity, pre-registration
// enforcement, and the governance evaluation trigger land with deliverable 14.
package indicator

import (
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// Entity is the kind of resource a sample describes (governance.proto `Entity`).
type Entity string

const (
	EntityAsyncChannel Entity = "ASYNC_CHANNEL"
	EntityKafkaTopic   Entity = "KAFKA_TOPIC"
	EntityKafkaCluster Entity = "KAFKA_CLUSTER"
)

// Valid reports whether e is a known entity.
func (e Entity) Valid() bool {
	switch e {
	case EntityAsyncChannel, EntityKafkaTopic, EntityKafkaCluster:
		return true
	default:
		return false
	}
}

// maxValueLen bounds the encoded value so a misbehaving agent cannot write
// unbounded rows. 003.14 does not fix a limit; this is a defensive cap.
const maxValueLen = 1024

// Sample is one append-only observation.
type Sample struct {
	ID      uuid.UUID // surrogate key; assigned by the repository on Append
	RealmID uuid.UUID

	Indicator string
	// ResourceFRN is a plain string, not a parsed FRN: 005 ADR §2.1 samples
	// cluster sub-resources ("<cluster-frn>/broker/3") that are not themselves
	// Franz resources.
	ResourceFRN    string
	ResourceEntity Entity
	// Value is encoded per the indicator's unit. Franz stores it verbatim;
	// unit-aware parsing arrives with the registry (deliverable 14).
	Value string

	ReportingAgent string
	SampleAt       time.Time
	ReceivedAt     time.Time
}

// NewSample validates one inbound sample. receivedAt is Franz's clock, so an
// agent cannot backdate ingest.
func NewSample(
	realmID uuid.UUID,
	name, resourceFRN string, entity Entity, value, reportingAgent string,
	sampleAt, receivedAt time.Time,
) (*Sample, error) {
	if name == "" {
		return nil, errs.InvalidField("indicator", "must not be empty")
	}
	if resourceFRN == "" {
		return nil, errs.InvalidField("resource_frn", "must not be empty")
	}
	if !entity.Valid() {
		return nil, errs.InvalidField("resource_entity",
			"must be one of ASYNC_CHANNEL, KAFKA_TOPIC, KAFKA_CLUSTER")
	}
	if value == "" {
		return nil, errs.InvalidField("value", "must not be empty")
	}
	if len(value) > maxValueLen {
		return nil, errs.InvalidField("value", "must be at most 1024 characters")
	}
	if sampleAt.IsZero() {
		sampleAt = receivedAt
	}
	return &Sample{
		RealmID:        realmID,
		Indicator:      name,
		ResourceFRN:    resourceFRN,
		ResourceEntity: entity,
		Value:          value,
		ReportingAgent: reportingAgent,
		SampleAt:       sampleAt.UTC(),
		ReceivedAt:     receivedAt.UTC(),
	}, nil
}
