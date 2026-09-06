package cluster

import (
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

func testRealm() realm.Realm {
	return realm.Realm{ID: uuid.New(), Slug: "default", Name: "Default"}
}

func plain(url string) []ConnectionString {
	return []ConnectionString{{BootstrapURLs: []string{url}, Type: ConnectionPlaintext}}
}

func TestNew(t *testing.T) {
	c, err := New(testRealm(), "east-1", plain("b1:9092"), map[string]string{"env": "prod"}, nil, "agent-x")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.State != StateActive {
		t.Errorf("state = %q, want ACTIVE", c.State)
	}
	if c.FRN.String() != "frn:default:kafka-cluster:east-1" {
		t.Errorf("frn = %q", c.FRN.String())
	}
	if c.Configuration == nil || c.Labels == nil {
		t.Error("maps should be non-nil")
	}
}

func TestNewValidation(t *testing.T) {
	r := testRealm()
	if _, err := New(r, "Bad Name", plain("b:9092"), nil, nil, ""); err == nil {
		t.Error("bad name should fail")
	}
	if _, err := New(r, "ok", nil, nil, nil, ""); err == nil {
		t.Error("empty connection_strings should fail")
	}
	if _, err := New(r, "ok", []ConnectionString{{Type: ConnectionPlaintext}}, nil, nil, ""); err == nil {
		t.Error("connection string without urls should fail")
	}
	if _, err := New(r, "ok", []ConnectionString{{BootstrapURLs: []string{"b:9092"}, Type: "SASL_SSL"}}, nil, nil, ""); err == nil {
		t.Error("unsupported connection type should fail")
	}
}

func TestNewDefaultsConnectionType(t *testing.T) {
	c, err := New(testRealm(), "east-1", []ConnectionString{{BootstrapURLs: []string{"b:9092"}}}, nil, nil, "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.ConnectionStrings[0].Type != ConnectionPlaintext {
		t.Errorf("type = %q, want default PLAINTEXT", c.ConnectionStrings[0].Type)
	}
}

func TestStateMachine(t *testing.T) {
	c, _ := New(testRealm(), "east-1", plain("b:9092"), nil, nil, "")

	if err := c.Pause(); err != nil || c.State != StatePaused {
		t.Fatalf("Pause: %v state=%v", err, c.State)
	}
	if err := c.Pause(); err != nil { // idempotent
		t.Fatalf("Pause idempotent: %v", err)
	}
	if err := c.Resume(); err != nil || c.State != StateActive {
		t.Fatalf("Resume: %v state=%v", err, c.State)
	}
	if err := c.Delete(); err != nil || c.State != StateDeleted {
		t.Fatalf("Delete: %v state=%v", err, c.State)
	}

	// every op on a deleted cluster is FAILED_PRECONDITION
	for _, op := range []func() error{c.Pause, c.Resume, c.Delete, c.EnsureMutable} {
		err := op()
		if e, ok := errs.As(err); !ok || e.Kind != errs.FailedPrecondition {
			t.Errorf("op on deleted cluster = %v, want FAILED_PRECONDITION", err)
		}
	}
}

func TestSetConnectionStrings(t *testing.T) {
	c, _ := New(testRealm(), "east-1", plain("b:9092"), nil, nil, "")
	if err := c.SetConnectionStrings(nil); err == nil {
		t.Error("empty should fail")
	}
	if err := c.SetConnectionStrings(plain("b2:9092")); err != nil {
		t.Fatalf("SetConnectionStrings: %v", err)
	}
	if c.ConnectionStrings[0].BootstrapURLs[0] != "b2:9092" {
		t.Errorf("not replaced: %v", c.ConnectionStrings)
	}
}
