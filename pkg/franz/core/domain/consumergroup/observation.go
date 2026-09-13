// Package consumergroup is the second inbound telemetry stream (003.14
// "Consumer-group observations"): which consumer groups an agent saw on which
// Kafka topics, and which client Franz should attribute each to.
//
// Unlike an indicator sample, an observation triggers no governance evaluation —
// 003.14 is explicit that consumer-group observations are read-only context.
package consumergroup

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
)

// maxFieldLen bounds every free-form field so a misbehaving agent cannot write
// unbounded rows, the same defensive cap indicator.Sample applies to `value`.
const maxFieldLen = 1024

// Observation is one append-only sighting of a consumer group on a topic.
//
// Custom is derived, not reported: the agent says what it saw, Franz decides
// whether that name follows the convention.
type Observation struct {
	ID      uuid.UUID // surrogate key; assigned by the repository on Append
	RealmID uuid.UUID

	// Group is the Kafka consumer group id, verbatim.
	Group string
	// ClientFRN is the client the agent attributed the group to, prefix-less.
	// Empty when it could not resolve one — a custom group name often cannot be
	// traced back (003.10), and an unattributed sighting is still evidence.
	ClientFRN string
	// Owner is the team or person the reporting agent read off client metadata.
	Owner string
	// AsyncChannel and KafkaTopic are the resources the group was seen on, as the
	// agent resolved them. Plain strings: a group may sit on a topic Franz does
	// not manage.
	AsyncChannel string
	KafkaTopic   string
	// Custom is false only when Group is exactly the default `<client>.<topic>`
	// form. See IsCustom.
	Custom bool

	ReportingAgent string
	ObservedAt     time.Time
	ReceivedAt     time.Time
}

// NewObservation validates one inbound sighting and derives Custom. receivedAt
// is Franz's clock, so an agent cannot backdate ingest; an unset observedAt
// falls back to it.
func NewObservation(
	realmID uuid.UUID,
	group, clientFRN, owner, asyncChannel, kafkaTopic, reportingAgent string,
	observedAt, receivedAt time.Time,
) (*Observation, error) {
	group = strings.TrimSpace(group)
	if group == "" {
		return nil, errs.InvalidField("group", "must not be empty")
	}
	for field, value := range map[string]string{
		"group": group, "client_frn": clientFRN, "owner": owner,
		"async_channel": asyncChannel, "kafka_topic": kafkaTopic,
	} {
		if len(value) > maxFieldLen {
			return nil, errs.InvalidField(field, "must be at most 1024 characters")
		}
	}
	if observedAt.IsZero() {
		observedAt = receivedAt
	}
	return &Observation{
		RealmID:        realmID,
		Group:          group,
		ClientFRN:      strings.TrimSpace(clientFRN),
		Owner:          strings.TrimSpace(owner),
		AsyncChannel:   strings.TrimSpace(asyncChannel),
		KafkaTopic:     strings.TrimSpace(kafkaTopic),
		Custom:         IsCustom(group, clientFRN, owner, kafkaTopic),
		ReportingAgent: reportingAgent,
		ObservedAt:     observedAt.UTC(),
		ReceivedAt:     receivedAt.UTC(),
	}, nil
}

// IsCustom reports whether group departs from the default `<client>.<topic>`
// naming convention (003.14 / 003.10).
//
// The client half is the *name* of the client, taken from clientFRN's name
// segment when it parses as an FRN, falling back to clientFRN verbatim (an agent
// may report a bare name) and then to owner. The topic half is likewise
// kafkaTopic's FRN name segment, or kafkaTopic verbatim.
//
// A sighting Franz cannot attribute to any client is custom: there is no
// convention to hold it to, and reporting it as conventional would claim an
// attribution that was never made.
func IsCustom(group, clientFRN, owner, kafkaTopic string) bool {
	client := nameOf(clientFRN)
	if client == "" {
		client = nameOf(owner)
	}
	topic := nameOf(kafkaTopic)
	if client == "" || topic == "" {
		return true
	}
	return strings.TrimSpace(group) != client+"."+topic
}

// nameOf reduces an FRN — in either the prefixed or the prefix-less form — to
// its name segment, and leaves any other string alone.
func nameOf(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if f, err := frn.Parse(s); err == nil {
		return f.Name()
	}
	return s
}
