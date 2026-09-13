package consumergroup_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/consumergroup"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// TestIsCustom pins the naming convention 003.14 / 003.10 describe: a group is
// conventional only when its name is exactly `<client>.<topic>`.
func TestIsCustom(t *testing.T) {
	tests := []struct {
		name       string
		group      string
		clientFRN  string
		owner      string
		kafkaTopic string
		want       bool
	}{
		{
			name:      "the default form off a client FRN is not custom",
			group:     "billing.billing-events-0",
			clientFRN: "default:client:billing", kafkaTopic: "billing-events-0",
			want: false,
		},
		{
			name:      "a prefixed client FRN reduces to the same name",
			group:     "billing.billing-events-0",
			clientFRN: "frn:default:client:billing", kafkaTopic: "billing-events-0",
			want: false,
		},
		{
			name:      "a topic FRN reduces to its name half too",
			group:     "billing.billing-events-0",
			clientFRN: "default:client:billing", kafkaTopic: "default:kafka-topic:billing-events-0",
			want: false,
		},
		{
			name:  "a bare client name is accepted as reported",
			group: "billing.billing-events-0",
			// An agent that resolved a name but not an FRN still gets the
			// convention applied to it.
			clientFRN: "billing", kafkaTopic: "billing-events-0",
			want: false,
		},
		{
			name:  "owner stands in when no client FRN was resolved",
			group: "payments.orders-0", owner: "payments", kafkaTopic: "orders-0",
			want: false,
		},
		{
			name:      "an operator-chosen name is custom",
			group:     "legacy-batch-reader",
			clientFRN: "default:client:billing", kafkaTopic: "billing-events-0",
			want: true,
		},
		{
			name:      "the right shape on the wrong topic is still custom",
			group:     "billing.other-topic",
			clientFRN: "default:client:billing", kafkaTopic: "billing-events-0",
			want: true,
		},
		{
			name: "an unattributable sighting is custom — there is no convention " +
				"to hold it to",
			group: "billing.billing-events-0", kafkaTopic: "billing-events-0",
			want: true,
		},
		{
			name:  "a sighting with no topic is custom for the same reason",
			group: "billing.billing-events-0", clientFRN: "default:client:billing",
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := consumergroup.IsCustom(tc.group, tc.clientFRN, tc.owner, tc.kafkaTopic)
			if got != tc.want {
				t.Fatalf("IsCustom = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewObservation(t *testing.T) {
	realmID := uuid.New()
	observed := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	received := time.Date(2026, 9, 1, 10, 0, 5, 0, time.UTC)

	o, err := consumergroup.NewObservation(realmID, "billing.orders-0",
		"default:client:billing", "payments-team", "orders", "orders-0",
		"odradek-prod", observed, received)
	if err != nil {
		t.Fatal(err)
	}
	if o.Custom {
		t.Error("the default form must not be flagged custom")
	}
	if !o.ObservedAt.Equal(observed) || !o.ReceivedAt.Equal(received) {
		t.Errorf("timestamps = (%v, %v)", o.ObservedAt, o.ReceivedAt)
	}
	if o.ReportingAgent != "odradek-prod" {
		t.Errorf("ReportingAgent = %q", o.ReportingAgent)
	}
}

// An agent that omits observed_at gets Franz's clock, not the Unix epoch — the
// row must never sort to the far past and be pruned on the next nightly sweep.
func TestNewObservationDefaultsObservedAt(t *testing.T) {
	received := time.Date(2026, 9, 1, 10, 0, 5, 0, time.UTC)
	o, err := consumergroup.NewObservation(uuid.New(), "g", "", "", "", "t",
		"agent", time.Time{}, received)
	if err != nil {
		t.Fatal(err)
	}
	if !o.ObservedAt.Equal(received) {
		t.Fatalf("ObservedAt = %v, want the received-at clock %v", o.ObservedAt, received)
	}
}

func TestNewObservationRejections(t *testing.T) {
	now := time.Now()

	if _, err := consumergroup.NewObservation(uuid.New(), "   ", "", "", "", "t",
		"agent", now, now); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("blank group kind = %v", errs.KindOf(err))
	}
	if _, err := consumergroup.NewObservation(uuid.New(), "g", "", "", "",
		strings.Repeat("x", 1025), "agent", now, now); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("over-long topic kind = %v", errs.KindOf(err))
	}
}
