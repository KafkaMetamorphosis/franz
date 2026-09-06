package provider

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
)

func TestPhaseValid(t *testing.T) {
	for _, p := range []Phase{PhaseProvisioning, PhaseReady, PhaseDegraded, PhaseError, PhaseStopped, PhaseRemoved} {
		if !p.Valid() {
			t.Errorf("%q should be valid", p)
		}
	}
	for _, p := range []Phase{"", "WAT", "ready"} {
		if p.Valid() {
			t.Errorf("%q should be invalid", p)
		}
	}
}

func TestProvisioningLabels(t *testing.T) {
	got := ProvisioningLabels(map[string]string{
		"franz.provisioning/deployment-type": "local-docker",
		"franz.provisioning/kafka-version":   "3.7.0",
		"env":                                "prod",
		"franz.affinity/selector":            "x",
	})
	if len(got) != 2 {
		t.Fatalf("got %v, want only the two franz.provisioning/* keys", got)
	}
	if got["franz.provisioning/deployment-type"] != "local-docker" {
		t.Errorf("value not carried: %v", got)
	}
}

func TestNewEventRejectsBadPhase(t *testing.T) {
	_, err := NewEvent(uuid.New(), uuid.New(), frn.FRN{}, "NONSENSE", true, "", "prov-1", "", time.Now())
	if errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("bad phase = %v, want INVALID_ARGUMENT", err)
	}
	ev, err := NewEvent(uuid.New(), uuid.New(), frn.FRN{}, PhaseReady, true, "ok", "prov-1", "r@1", time.Unix(1, 0))
	if err != nil || ev.Phase != PhaseReady || ev.ReportingAgent != "prov-1" {
		t.Fatalf("valid event: %+v %v", ev, err)
	}
}
