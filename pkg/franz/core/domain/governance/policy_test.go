package governance_test

import (
	"testing"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// testIndicator is a registered indicator a definition can be validated against.
func testIndicator(unit indicator.Unit, appliesTo indicator.Entity) *indicator.Indicator {
	return &indicator.Indicator{
		Name: "disk-used", Unit: unit, AppliesTo: appliesTo,
		StalenessThreshold: time.Hour, StalenessSpec: "1h",
	}
}

func action(kind governance.ActionKind, args ...string) governance.Action {
	return governance.Action{Kind: kind, Args: args}
}

func definition(entity indicator.Entity, actions ...governance.Action) governance.Definition {
	return governance.Definition{
		Indicator: "disk-used",
		Matcher:   governance.Matcher{Entity: entity},
		Limit:     governance.Limit{Operator: governance.OpGreaterThan, Value: "100"},
		Actions:   actions,
	}
}

// TestValidateAgainstWhitelist walks the 003.8 write-whitelist matrix: every row
// it admits must be accepted, and a representative sample of what it excludes
// must be rejected.
func TestValidateAgainstWhitelist(t *testing.T) {
	tests := []struct {
		name   string
		entity indicator.Entity
		action governance.Action
		// accepted policies leave wantKind unread; a rejection names the exact
		// Kind, because "rejected somehow" is not the contract 003.8 states.
		accepted bool
		wantKind errs.Kind
	}{
		// --- KAFKA_TOPIC, admitted --------------------------------------
		{"topic partitions UPDATE_FIELD", indicator.EntityKafkaTopic,
			action(governance.ActionUpdateField, "partitions", "12"), true, 0},
		{"topic partitions INCREASE with cap", indicator.EntityKafkaTopic,
			action(governance.ActionIncreaseFieldBy, "partitions", "2", "max=64"), true, 0},
		{"topic replication_factor UPDATE_FIELD", indicator.EntityKafkaTopic,
			action(governance.ActionUpdateField, "replication_factor", "3"), true, 0},
		{"topic config UPDATE_FIELD", indicator.EntityKafkaTopic,
			action(governance.ActionUpdateField, "topic_configuration.retention.ms", "3600000"), true, 0},
		{"topic config INCREASE_FIELD_BY", indicator.EntityKafkaTopic,
			action(governance.ActionIncreaseFieldBy, "topic_configuration.retention.ms", "50%"), true, 0},
		{"topic config DECREASE_FIELD_BY", indicator.EntityKafkaTopic,
			action(governance.ActionDecreaseFieldBy, "topic_configuration.retention.ms", "1000", "min=1"), true, 0},
		{"topic consumption UPDATE_FIELD", indicator.EntityKafkaTopic,
			action(governance.ActionUpdateField, "consumption", "DISABLED"), true, 0},

		// --- KAFKA_TOPIC, excluded --------------------------------------
		// 003.8: "`KAFKA_TOPIC.state` is not writable ... `KafkaTopic`
		// `SET_STATUS` is rejected." It is a plain whitelist miss, so it is
		// INVALID_ARGUMENT like every other out-of-whitelist action — not a
		// deferred capability.
		{"topic state SET_STATUS is not writable", indicator.EntityKafkaTopic,
			action(governance.ActionSetStatus, "PAUSED"), false, errs.InvalidArgument},
		{"topic partitions DECREASE is rejected", indicator.EntityKafkaTopic,
			action(governance.ActionDecreaseFieldBy, "partitions", "1"), false, errs.InvalidArgument},
		{"topic brokers is a cluster field", indicator.EntityKafkaTopic,
			action(governance.ActionUpdateField, "brokers", "3"), false, errs.InvalidArgument},
		{"topic config key must be a real Kafka key", indicator.EntityKafkaTopic,
			action(governance.ActionUpdateField, "topic_configuration.not.a.key", "1"), false, errs.InvalidArgument},

		// --- ASYNC_CHANNEL ----------------------------------------------
		{"channel state SET_STATUS", indicator.EntityAsyncChannel,
			action(governance.ActionSetStatus, "PAUSED"), true, 0},
		{"channel plain label ADD_LABEL", indicator.EntityAsyncChannel,
			action(governance.ActionAddLabel, "tier", "gold"), true, 0},
		{"channel plain label REMOVE_LABEL", indicator.EntityAsyncChannel,
			action(governance.ActionRemoveLabel, "tier"), true, 0},
		{"channel replication_factor is a topic field", indicator.EntityAsyncChannel,
			action(governance.ActionUpdateField, "replication_factor", "3"), false, errs.InvalidArgument},

		// --- KAFKA_CLUSTER ----------------------------------------------
		{"cluster state SET_STATUS", indicator.EntityKafkaCluster,
			action(governance.ActionSetStatus, "PAUSED"), true, 0},
		{"cluster brokers INCREASE_FIELD_BY", indicator.EntityKafkaCluster,
			action(governance.ActionIncreaseFieldBy, "brokers", "1", "max=9"), true, 0},
		{"cluster disk_size UPDATE_FIELD", indicator.EntityKafkaCluster,
			action(governance.ActionUpdateField, "disk_size", "500Gi"), true, 0},
		{"cluster config UPDATE_FIELD", indicator.EntityKafkaCluster,
			action(governance.ActionUpdateField, "cluster_configuration.partitions", "6"), true, 0},
		{"cluster disk_size arithmetic is not whitelisted", indicator.EntityKafkaCluster,
			action(governance.ActionIncreaseFieldBy, "disk_size", "10Gi"), false, errs.InvalidArgument},
		{"cluster config arithmetic is not whitelisted", indicator.EntityKafkaCluster,
			action(governance.ActionIncreaseFieldBy, "cluster_configuration.partitions", "1"), false, errs.InvalidArgument},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			def := definition(tc.entity, tc.action)
			err := def.ValidateAgainst(testIndicator(indicator.UnitCount, tc.entity))
			if tc.accepted {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				return
			}
			if got := errs.KindOf(err); err == nil || got != tc.wantKind {
				t.Fatalf("kind = %v (err %v), want %v", got, err, tc.wantKind)
			}
		})
	}
}

// TestValidateAgainstDeferredActions covers the whitelist rows 003.8 allows but
// this deliverable cannot honour: they are rejected at write with
// FAILED_PRECONDITION rather than accepted and silently ignored later.
func TestValidateAgainstDeferredActions(t *testing.T) {
	tests := []struct {
		name   string
		entity indicator.Entity
		action governance.Action
	}{
		{"channel_partitions is a staged re-shard", indicator.EntityAsyncChannel,
			action(governance.ActionIncreaseFieldBy, "channel_partitions", "1")},
		{"affinity label triggers re-placement", indicator.EntityAsyncChannel,
			action(governance.ActionAddLabel, "franz.affinity/region", "eu")},
		{"antiaffinity label triggers re-placement", indicator.EntityAsyncChannel,
			action(governance.ActionRemoveLabel, "franz.antiaffinity/rack")},
		{"franz.taint drains a cluster", indicator.EntityKafkaCluster,
			action(governance.ActionAddLabel, "franz.taint", "drain")},
		{"a Kafka Topic has no label map", indicator.EntityKafkaTopic,
			action(governance.ActionAddLabel, "tier", "gold")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			def := definition(tc.entity, tc.action)
			err := def.ValidateAgainst(testIndicator(indicator.UnitCount, tc.entity))
			if got := errs.KindOf(err); got != errs.FailedPrecondition {
				t.Fatalf("kind = %v (err %v), want FailedPrecondition", got, err)
			}
		})
	}
}

// TestValidateAgainstArity pins the args count each ActionKind takes (003.8
// "Action"), including the arithmetic kinds' optional third cap arg.
func TestValidateAgainstArity(t *testing.T) {
	tests := []struct {
		name   string
		entity indicator.Entity
		action governance.Action
		ok     bool
	}{
		{"ADD_LABEL needs two args", indicator.EntityAsyncChannel,
			action(governance.ActionAddLabel, "tier"), false},
		{"ADD_LABEL rejects three args", indicator.EntityAsyncChannel,
			action(governance.ActionAddLabel, "tier", "gold", "extra"), false},
		{"REMOVE_LABEL needs one arg", indicator.EntityAsyncChannel,
			action(governance.ActionRemoveLabel, "tier", "gold"), false},
		{"SET_STATUS needs one arg", indicator.EntityAsyncChannel,
			action(governance.ActionSetStatus), false},
		{"UPDATE_FIELD needs two args", indicator.EntityKafkaTopic,
			action(governance.ActionUpdateField, "partitions"), false},
		{"INCREASE_FIELD_BY takes two", indicator.EntityKafkaCluster,
			action(governance.ActionIncreaseFieldBy, "brokers", "1"), true},
		{"INCREASE_FIELD_BY takes an optional cap", indicator.EntityKafkaCluster,
			action(governance.ActionIncreaseFieldBy, "brokers", "1", "max=9"), true},
		{"INCREASE_FIELD_BY rejects a fourth arg", indicator.EntityKafkaCluster,
			action(governance.ActionIncreaseFieldBy, "brokers", "1", "max=9", "x"), false},
		{"an empty arg is rejected", indicator.EntityAsyncChannel,
			action(governance.ActionAddLabel, "tier", "  "), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			def := definition(tc.entity, tc.action)
			err := def.ValidateAgainst(testIndicator(indicator.UnitCount, tc.entity))
			if tc.ok && err != nil {
				t.Fatalf("want accepted, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("want rejected, got nil")
			}
		})
	}
}

// TestValidateAgainstAppliesTo covers the 003.8 invariant that an indicator's
// applies_to and a policy's matcher.entity must agree, and that an unregistered
// indicator is rejected at write.
func TestValidateAgainstAppliesTo(t *testing.T) {
	channelAction := action(governance.ActionSetStatus, "PAUSED")

	t.Run("agreeing entity is accepted", func(t *testing.T) {
		def := definition(indicator.EntityAsyncChannel, channelAction)
		if err := def.ValidateAgainst(
			testIndicator(indicator.UnitCount, indicator.EntityAsyncChannel)); err != nil {
			t.Fatalf("want accepted, got %v", err)
		}
	})

	t.Run("cluster indicator cannot drive a channel matcher", func(t *testing.T) {
		def := definition(indicator.EntityAsyncChannel, channelAction)
		err := def.ValidateAgainst(testIndicator(indicator.UnitCount, indicator.EntityKafkaCluster))
		if errs.KindOf(err) != errs.InvalidArgument {
			t.Fatalf("kind = %v (err %v), want InvalidArgument", errs.KindOf(err), err)
		}
	})

	t.Run("unknown indicator is a failed precondition", func(t *testing.T) {
		def := definition(indicator.EntityAsyncChannel, channelAction)
		err := def.ValidateAgainst(nil)
		if errs.KindOf(err) != errs.FailedPrecondition {
			t.Fatalf("kind = %v (err %v), want FailedPrecondition", errs.KindOf(err), err)
		}
	})

	t.Run("empty indicator name is rejected", func(t *testing.T) {
		def := definition(indicator.EntityAsyncChannel, channelAction)
		def.Indicator = ""
		if errs.KindOf(def.ValidateAgainst(nil)) != errs.InvalidArgument {
			t.Fatal("want InvalidArgument for an unnamed indicator")
		}
	})

	t.Run("actions must not be empty", func(t *testing.T) {
		def := definition(indicator.EntityAsyncChannel)
		err := def.ValidateAgainst(testIndicator(indicator.UnitCount, indicator.EntityAsyncChannel))
		if errs.KindOf(err) != errs.InvalidArgument {
			t.Fatalf("want InvalidArgument, got %v", err)
		}
	})

	t.Run("limit value must parse in the indicator's unit", func(t *testing.T) {
		def := definition(indicator.EntityAsyncChannel, channelAction)
		def.Limit.Value = "not-a-boolean"
		err := def.ValidateAgainst(testIndicator(indicator.UnitBoolean, indicator.EntityAsyncChannel))
		if errs.KindOf(err) != errs.InvalidArgument {
			t.Fatalf("want InvalidArgument, got %v", err)
		}
	})
}

// TestRequireCapOnPartitionIncrease pins the one place 003.8 makes a cap
// mandatory: raising a topic's partition count is irreversible (003.6) and there
// is no cooldown (OQ2), so the cap is the only bound.
func TestRequireCapOnPartitionIncrease(t *testing.T) {
	withoutCap := definition(indicator.EntityKafkaTopic,
		action(governance.ActionIncreaseFieldBy, "partitions", "2"))
	if errs.KindOf(withoutCap.ValidateAgainst(
		testIndicator(indicator.UnitCount, indicator.EntityKafkaTopic))) != errs.InvalidArgument {
		t.Fatal("INCREASE_FIELD_BY on partitions must require a cap")
	}

	withCap := definition(indicator.EntityKafkaTopic,
		action(governance.ActionIncreaseFieldBy, "partitions", "2", "max=64"))
	if err := withCap.ValidateAgainst(
		testIndicator(indicator.UnitCount, indicator.EntityKafkaTopic)); err != nil {
		t.Fatalf("a capped partition increase must be accepted: %v", err)
	}
}

// TestLimitTriggers pins the comparator per unit family (003.8 step 3).
func TestLimitTriggers(t *testing.T) {
	tests := []struct {
		name  string
		unit  indicator.Unit
		op    governance.Operator
		limit string
		value string
		want  bool
	}{
		{"count above", indicator.UnitCount, governance.OpGreaterThan, "100", "150", true},
		{"count equal is not above", indicator.UnitCount, governance.OpGreaterThan, "100", "100", false},
		{"count at or above", indicator.UnitCount, governance.OpGreaterThanOrEqual, "100", "100", true},
		{"count below", indicator.UnitCount, governance.OpLessThan, "3", "2", true},
		{"count not equal", indicator.UnitCount, governance.OpNotEqual, "3", "4", true},
		{"bytes binary suffix", indicator.UnitBytes, governance.OpGreaterThan, "150Gi", "200Gi", true},
		{"bytes SI vs binary", indicator.UnitBytes, governance.OpLessThan, "1Gi", "1G", true},
		{"bytes trailing B", indicator.UnitBytes, governance.OpEqual, "512", "512B", true},
		{"duration days", indicator.UnitDuration, governance.OpGreaterThan, "90d", "91d", true},
		{"duration mixed grammar", indicator.UnitDuration, governance.OpLessThan, "1h", "30m", true},
		{"boolean true above false", indicator.UnitBoolean, governance.OpGreaterThan, "false", "true", true},
		{"boolean equality", indicator.UnitBoolean, governance.OpEqual, "true", "true", true},
		{"percent is numeric", indicator.UnitPercent, governance.OpGreaterThanOrEqual, "90", "90.5", true},
		{"unknown unit falls back to numeric", indicator.Unit("widgets"),
			governance.OpGreaterThan, "1", "2", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limit := governance.Limit{Operator: tc.op, Value: tc.limit}
			got, err := limit.Triggers(tc.unit, tc.value)
			if err != nil {
				t.Fatalf("Triggers: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Triggers = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestLimitTriggersSurfacesUnparseableValues: a value that cannot be compared is
// an error, not a silent false — a policy that cannot be evaluated must be
// visible.
func TestLimitTriggersSurfacesUnparseableValues(t *testing.T) {
	limit := governance.Limit{Operator: governance.OpGreaterThan, Value: "100"}
	if _, err := limit.Triggers(indicator.UnitCount, "not-a-number"); err == nil {
		t.Fatal("an unparseable value must surface as an error")
	}
}

// TestOrderIsDeterministic pins the 003.8 conflict order: weight descending,
// then name ascending. Two equal-weight policies must resolve by name, which is
// the "Done when" check.
func TestOrderIsDeterministic(t *testing.T) {
	policies := []*governance.Policy{
		{Name: "zulu", Weight: 5},
		{Name: "alpha", Weight: 5},
		{Name: "mike", Weight: 10},
		{Name: "bravo", Weight: 1},
	}
	governance.Order(policies)

	want := []string{"mike", "alpha", "zulu", "bravo"}
	for i, name := range want {
		if policies[i].Name != name {
			t.Fatalf("position %d = %q, want %q", i, policies[i].Name, name)
		}
	}
}

// TestMarkFiredStampsUTC keeps last_fired_at normalised, so two policies fired in
// one pass carry the same instant regardless of the caller's location.
func TestMarkFiredStampsUTC(t *testing.T) {
	p := &governance.Policy{Name: "p"}
	if p.LastFiredAt != nil {
		t.Fatal("a policy starts unfired")
	}
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.FixedZone("x", 3600))
	p.MarkFired(at)
	if p.LastFiredAt == nil || !p.LastFiredAt.Equal(at) {
		t.Fatalf("LastFiredAt = %v, want %v", p.LastFiredAt, at)
	}
	if p.LastFiredAt.Location() != time.UTC {
		t.Fatalf("LastFiredAt location = %v, want UTC", p.LastFiredAt.Location())
	}
}

// TestNewAssignsFRN checks the identity half of a policy.
func TestNewAssignsFRN(t *testing.T) {
	r := realm.Realm{ID: realm.DefaultID, Slug: realm.DefaultSlug}
	def := definition(indicator.EntityAsyncChannel, action(governance.ActionSetStatus, "PAUSED"))

	p, err := governance.New(r, "pause-idle", def, 5, true)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.FRN.Name() != "pause-idle" {
		t.Errorf("FRN name = %q", p.FRN.Name())
	}
	if p.FRN.Type() != "policy" {
		t.Errorf("FRN type = %q, want policy", p.FRN.Type())
	}
	if p.RealmID != r.ID {
		t.Errorf("RealmID = %s", p.RealmID)
	}
	if _, err := governance.New(r, "Not A Name", def, 0, true); err == nil {
		t.Error("an invalid name must be rejected")
	}
}
