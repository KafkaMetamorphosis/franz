package channel

import (
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

func testRealm() realm.Realm {
	return realm.Realm{ID: uuid.New(), Slug: "default", Name: "Default"}
}

func validPolicy() accesspolicy.Policy {
	return accesspolicy.Policy{Statements: []accesspolicy.Statement{
		{Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{ClientFRN: "frn:default:client:x"}, Permissions: []accesspolicy.Permission{accesspolicy.Read}},
	}}
}

func TestNew(t *testing.T) {
	c, err := New(testRealm(), "billing-events", TypeKafkaTopic, 3, map[string]string{"team": "billing"}, validPolicy())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.State != StateActive || c.ChannelPartitions != 3 {
		t.Errorf("bad channel: %+v", c)
	}
	if c.FRN.String() != "frn:default:async-channel:billing-events" {
		t.Errorf("frn = %q", c.FRN.String())
	}
	if c.ShardName(0) != "billing-events-0" || c.ShardName(2) != "billing-events-2" {
		t.Errorf("shard names wrong")
	}
}

func TestNewValidation(t *testing.T) {
	r := testRealm()
	if _, err := New(r, "Bad Name", TypeKafkaTopic, 1, nil, accesspolicy.Policy{}); err == nil {
		t.Error("bad name should fail")
	}
	if _, err := New(r, "ok", "QUEUE", 1, nil, accesspolicy.Policy{}); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("bad type → %v", err)
	}
	if _, err := New(r, "ok", TypeKafkaTopic, 0, nil, accesspolicy.Policy{}); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("channel_partitions 0 → %v", err)
	}
	bad := accesspolicy.Policy{Statements: []accesspolicy.Statement{{Effect: "", Permissions: []accesspolicy.Permission{accesspolicy.Read}, Principal: accesspolicy.Principal{ClientFRN: "x"}}}}
	if _, err := New(r, "ok", TypeKafkaTopic, 1, nil, bad); errs.KindOf(err) != errs.InvalidArgument {
		t.Errorf("bad policy at create → %v", err)
	}
}

func TestStateMachine(t *testing.T) {
	c, _ := New(testRealm(), "c", TypeKafkaTopic, 1, nil, accesspolicy.Policy{})
	if err := c.Pause(); err != nil || c.State != StatePaused {
		t.Fatalf("pause: %v", err)
	}
	if err := c.Pause(); err != nil {
		t.Fatalf("pause idempotent: %v", err)
	}
	if err := c.Resume(); err != nil || c.State != StateActive {
		t.Fatalf("resume: %v", err)
	}
	if err := c.Delete(); err != nil || c.State != StateDeleted {
		t.Fatalf("delete: %v", err)
	}
	for _, op := range []func() error{c.Pause, c.Resume, c.Delete, c.EnsureMutable} {
		if errs.KindOf(op()) != errs.FailedPrecondition {
			t.Error("op on deleted channel should be FAILED_PRECONDITION")
		}
	}
	if err := c.SetLabels(map[string]string{"a": "b"}); errs.KindOf(err) != errs.FailedPrecondition {
		t.Error("SetLabels on deleted → want FAILED_PRECONDITION")
	}
	if err := c.SetAccessPolicy(validPolicy()); errs.KindOf(err) != errs.FailedPrecondition {
		t.Error("SetAccessPolicy on deleted → want FAILED_PRECONDITION")
	}
}

func TestSetAccessPolicyValidates(t *testing.T) {
	c, _ := New(testRealm(), "c", TypeKafkaTopic, 1, nil, accesspolicy.Policy{})
	bad := accesspolicy.Policy{Statements: []accesspolicy.Statement{{Effect: accesspolicy.Allow, Principal: accesspolicy.Principal{}, Permissions: []accesspolicy.Permission{accesspolicy.Read}}}}
	if err := c.SetAccessPolicy(bad); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("principal-less statement → %v", err)
	}
	if err := c.SetAccessPolicy(validPolicy()); err != nil {
		t.Fatalf("valid policy → %v", err)
	}
	if len(c.AccessPolicy.Statements) != 1 {
		t.Error("policy not stored")
	}
}
