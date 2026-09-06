package agent

import (
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

func testRealm() realm.Realm {
	return realm.Realm{ID: uuid.New(), Slug: "default", Name: "Default"}
}

func TestNew(t *testing.T) {
	a, err := New(testRealm(), "provisioner-1", TypeClusterProvider, map[string]string{"team": "infra"}, nil, "hash")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.Status != StatusActive || a.FRN.String() != "frn:default:agent:provisioner-1" {
		t.Errorf("bad agent: %+v", a)
	}
	if a.TokenHash != "hash" {
		t.Errorf("token hash = %q", a.TokenHash)
	}
}

func TestNewValidation(t *testing.T) {
	r := testRealm()
	if _, err := New(r, "Bad Name", TypeCustom, nil, nil, "h"); err == nil {
		t.Error("bad name should fail")
	}
	if _, err := New(r, "ok", Type("BOGUS"), nil, nil, "h"); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("bad type = %v", err)
	}
	if _, err := New(r, "ok", "", nil, nil, "h"); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("unspecified type = %v", err)
	}
}

func TestSetType(t *testing.T) {
	a, _ := New(testRealm(), "a", TypeCustom, nil, nil, "h")
	if err := a.SetType(TypeTelemetryAgent); err != nil || a.Type != TypeTelemetryAgent {
		t.Fatalf("SetType: %v type=%v", err, a.Type)
	}
	if err := a.SetType("NOPE"); err == nil {
		t.Error("SetType invalid should fail")
	}
}

func TestStatusMachine(t *testing.T) {
	a, _ := New(testRealm(), "a", TypeCustom, nil, nil, "h")

	if err := a.Pause(); err != nil || a.Status != StatusPaused {
		t.Fatalf("Pause: %v", err)
	}
	if err := a.Pause(); err != nil {
		t.Fatalf("Pause idempotent: %v", err)
	}
	if err := a.Resume(); err != nil || a.Status != StatusActive {
		t.Fatalf("Resume: %v", err)
	}
	if err := a.Delete(); err != nil || a.Status != StatusDeleted {
		t.Fatalf("Delete: %v", err)
	}
	for _, op := range []func() error{a.Pause, a.Resume, a.Delete, a.EnsureMutable} {
		if errs.KindOf(op()) != errs.FailedPrecondition {
			t.Error("op on deleted agent should be FAILED_PRECONDITION")
		}
	}
}

func TestRotateToken(t *testing.T) {
	a, _ := New(testRealm(), "a", TypeCustom, nil, nil, "h1")
	a.RotateToken("h2")
	if a.TokenHash != "h2" {
		t.Errorf("token hash = %q", a.TokenHash)
	}
}
