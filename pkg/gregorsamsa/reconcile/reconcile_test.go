package reconcile_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/reconcile"
)

// --- harness -------------------------------------------------------------

type recorder struct {
	mu      sync.Mutex
	reports []reconcile.Report
	err     error
}

func (r *recorder) Report(_ context.Context, report reconcile.Report) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.reports = append(r.reports, report)
	return nil
}

func (r *recorder) last() reconcile.Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.reports) == 0 {
		return reconcile.Report{}
	}
	return r.reports[len(r.reports)-1]
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reports)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// harness builds a reconciler over one fake broker reachable at "east-1:9092".
func harness(t *testing.T) (*reconcile.Reconciler, *kafkaadmin.Mem, *recorder) {
	t.Helper()
	broker := kafkaadmin.NewMem()
	rec := &recorder{}
	r := reconcile.New(
		kafkaadmin.MemFactory(map[string]*kafkaadmin.Mem{"east-1:9092": broker}),
		rec, quiet(), nil)
	t.Cleanup(r.Close)
	return r, broker, rec
}

func setAssignment(frn, topicName string, generation int64, partitions, rf int32, config map[string]string) assign.Assignment {
	return assign.Assignment{
		Change:            assign.ChangeSet,
		PartitionFRN:      frn,
		Generation:        generation,
		AsyncChannel:      "billing-events",
		TopicName:         topicName,
		ClusterName:       "east-1",
		ClusterFRN:        "frn:default:kafka-cluster:east-1",
		BootstrapServers:  []string{"east-1:9092"},
		DesiredConfig:     config,
		Partitions:        partitions,
		ReplicationFactor: rf,
	}
}

func removeAssignment(frn, topicName string, generation int64, reason assign.Reason) assign.Assignment {
	return assign.Assignment{
		Change:           assign.ChangeRemoved,
		Reason:           reason,
		PartitionFRN:     frn,
		Generation:       generation,
		TopicName:        topicName,
		ClusterName:      "east-1",
		BootstrapServers: []string{"east-1:9092"},
	}
}

func world(assignments ...assign.Assignment) map[string]assign.Assignment {
	out := map[string]assign.Assignment{}
	for _, a := range assignments {
		out[a.PartitionFRN] = a
	}
	return out
}

const frn0 = "frn:default:kafka-topic:billing-events-0"

// --- SET -----------------------------------------------------------------

func TestSetCreatesAnAbsentTopic(t *testing.T) {
	r, broker, rec := harness(t)
	a := setAssignment(frn0, "billing-events-0", 1, 3, 1, map[string]string{"retention.ms": "604800000"})

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}

	got := rec.last()
	if got.Outcome != reconcile.OutcomeCreated {
		t.Fatalf("outcome = %s (%s), want CREATED", got.Outcome, got.Message)
	}
	if got.Generation != 1 || got.PartitionFRN != frn0 {
		t.Errorf("report = %+v", got)
	}
	created := broker.Topics["billing-events-0"]
	if created == nil || created.Partitions != 3 || created.ReplicationFactor != 1 {
		t.Fatalf("broker topic = %+v", created)
	}
	if created.Config["retention.ms"] != "604800000" {
		t.Errorf("config not applied: %+v", created.Config)
	}
	if got.Applied == nil || got.Applied.Partitions != 3 {
		t.Errorf("applied_config missing on a successful report: %+v", got.Applied)
	}
}

func TestSetOnAMatchingTopicIsANoopWithNoKafkaWrite(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
		Name: "billing-events-0", Partitions: 3, ReplicationFactor: 1,
		Config: map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"},
	}
	a := setAssignment(frn0, "billing-events-0", 1, 3, 1, map[string]string{"retention.ms": "604800000"})

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if got := rec.last().Outcome; got != reconcile.OutcomeNoop {
		t.Fatalf("outcome = %s, want NOOP", got)
	}
	if len(broker.Writes) != 0 {
		t.Fatalf("a matching topic must produce no Kafka write, got %v", broker.Writes)
	}
	// cleanup.policy is not in desired_config, so it must be left alone.
	if broker.Topics["billing-events-0"].Config["cleanup.policy"] != "delete" {
		t.Error("a config key Franz did not specify was reset")
	}
}

func TestResyncOfAnUnchangedFleetIssuesNoKafkaCallAtAll(t *testing.T) {
	r, broker, rec := harness(t)
	a := setAssignment(frn0, "billing-events-0", 1, 3, 1, nil)

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	callsAfterFirst := len(broker.Calls)
	reportsAfterFirst := rec.count()

	// Same world again — a reconnect resync.
	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if len(broker.Calls) != callsAfterFirst {
		t.Fatalf("resync issued %d extra Kafka calls, want 0 (calls: %v)",
			len(broker.Calls)-callsAfterFirst, broker.Calls[callsAfterFirst:])
	}
	if rec.count() != reportsAfterFirst {
		t.Fatalf("resync issued %d extra reports, want 0", rec.count()-reportsAfterFirst)
	}
}

func TestSetAltersConfigAndRaisesPartitions(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
		Name: "billing-events-0", Partitions: 3, ReplicationFactor: 1,
		Config: map[string]string{"retention.ms": "1000"},
	}
	a := setAssignment(frn0, "billing-events-0", 2, 6, 1, map[string]string{"retention.ms": "604800000"})

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if got := rec.last(); got.Outcome != reconcile.OutcomeUpdated {
		t.Fatalf("outcome = %s (%s), want UPDATED", got.Outcome, got.Message)
	}
	after := broker.Topics["billing-events-0"]
	if after.Partitions != 6 {
		t.Errorf("partitions = %d, want 6", after.Partitions)
	}
	if after.Config["retention.ms"] != "604800000" {
		t.Errorf("retention not altered: %+v", after.Config)
	}
	if !contains(broker.Writes, "CreatePartitions") || !contains(broker.Writes, "AlterConfigs") {
		t.Errorf("writes = %v, want both CreatePartitions and AlterConfigs", broker.Writes)
	}
}

func TestSetRefusesAPartitionDecrease(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
		Name: "billing-events-0", Partitions: 6, ReplicationFactor: 1, Config: map[string]string{},
	}
	a := setAssignment(frn0, "billing-events-0", 2, 3, 1, nil)

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	got := rec.last()
	if got.Outcome != reconcile.OutcomeError {
		t.Fatalf("outcome = %s, want ERROR", got.Outcome)
	}
	if !strings.Contains(got.Message, "cannot reduce partition count from 6 to 3") {
		t.Errorf("message = %q", got.Message)
	}
	if len(broker.Writes) != 0 {
		t.Errorf("a refused reconcile must not write: %v", broker.Writes)
	}
	if got.Applied == nil || got.Applied.Partitions != 6 {
		t.Errorf("applied_config should carry the state that was read: %+v", got.Applied)
	}
}

func TestSetRefusesAReplicationFactorChange(t *testing.T) {
	for _, tc := range []struct {
		name            string
		actual, desired int32
	}{
		{name: "decrease", actual: 3, desired: 1},
		{name: "increase", actual: 1, desired: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, broker, rec := harness(t)
			broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
				Name: "billing-events-0", Partitions: 3,
				ReplicationFactor: tc.actual, Config: map[string]string{},
			}
			a := setAssignment(frn0, "billing-events-0", 2, 3, tc.desired, nil)

			if err := r.Sync(context.Background(), world(a)); err != nil {
				t.Fatal(err)
			}
			got := rec.last()
			if got.Outcome != reconcile.OutcomeError {
				t.Fatalf("outcome = %s, want ERROR", got.Outcome)
			}
			if !strings.Contains(got.Message, "replication factor") {
				t.Errorf("message = %q, want it to name the replication factor", got.Message)
			}
			if len(broker.Writes) != 0 {
				t.Errorf("a refused reconcile must not write: %v", broker.Writes)
			}
		})
	}
}

func TestSetReportsErrorWhenKafkaFails(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Fail["CreateTopic"] = errors.New("not enough brokers")
	a := setAssignment(frn0, "billing-events-0", 1, 3, 1, nil)

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	got := rec.last()
	if got.Outcome != reconcile.OutcomeError || !strings.Contains(got.Message, "not enough brokers") {
		t.Fatalf("report = %+v", got)
	}
}

// An ERROR is never settled: the next resync retries it (005 ADR §1.6).
func TestAnErroredPartitionIsRetriedOnResync(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Fail["CreateTopic"] = errors.New("no leader")
	a := setAssignment(frn0, "billing-events-0", 1, 3, 1, nil)

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if rec.last().Outcome != reconcile.OutcomeError {
		t.Fatalf("expected the first attempt to fail")
	}

	delete(broker.Fail, "CreateTopic")
	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if got := rec.last(); got.Outcome != reconcile.OutcomeCreated {
		t.Fatalf("outcome = %s (%s), want CREATED on retry", got.Outcome, got.Message)
	}
}

func TestUnreachableClusterErrorsEveryPartitionOnIt(t *testing.T) {
	rec := &recorder{}
	r := reconcile.New(
		func(context.Context, []string) (kafkaadmin.Admin, error) {
			return nil, errors.New("dial tcp: connection refused")
		}, rec, quiet(), nil)
	t.Cleanup(r.Close)

	a := setAssignment(frn0, "billing-events-0", 1, 3, 1, nil)
	b := setAssignment("frn:default:kafka-topic:billing-events-1", "billing-events-1", 1, 3, 1, nil)
	if err := r.Sync(context.Background(), world(a, b)); err != nil {
		t.Fatal(err)
	}
	if rec.count() != 2 {
		t.Fatalf("got %d reports, want one ERROR per partition on the cluster", rec.count())
	}
	for _, got := range rec.reports {
		if got.Outcome != reconcile.OutcomeError || !strings.Contains(got.Message, "unreachable") {
			t.Errorf("report = %+v", got)
		}
	}
}

// --- REMOVED -------------------------------------------------------------

func TestRemoveDeletesAnEmptyUnconsumedTopic(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
		Name: "billing-events-0", Partitions: 3, ReplicationFactor: 1, Config: map[string]string{},
	}
	a := removeAssignment(frn0, "billing-events-0", 5, "")

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if got := rec.last(); got.Outcome != reconcile.OutcomeDeleted {
		t.Fatalf("outcome = %s (%s), want DELETED", got.Outcome, got.Message)
	}
	if _, still := broker.Topics["billing-events-0"]; still {
		t.Fatal("topic was not deleted")
	}
}

func TestRemoveIsIdempotentWhenTheTopicIsAlreadyGone(t *testing.T) {
	r, broker, rec := harness(t)
	a := removeAssignment(frn0, "billing-events-0", 5, "")

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if got := rec.last(); got.Outcome != reconcile.OutcomeDeleted {
		t.Fatalf("outcome = %s, want DELETED", got.Outcome)
	}
	// No safety check should even have run — there is nothing to protect.
	if contains(broker.Calls, "ListOffsets") || contains(broker.Calls, "ListConsumerGroups") {
		t.Errorf("safety checks ran against an absent topic: %v", broker.Calls)
	}
}

func TestRemoveRefusesATopicHoldingUnconsumedData(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
		Name: "billing-events-0", Partitions: 2, ReplicationFactor: 1, Config: map[string]string{},
	}
	broker.Offsets["billing-events-0"] = []kafkaadmin.PartitionOffsets{
		{Partition: 0, Earliest: 0, Latest: 45201},
		{Partition: 1, Earliest: 7, Latest: 7},
	}
	a := removeAssignment(frn0, "billing-events-0", 5, "")

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	got := rec.last()
	if got.Outcome != reconcile.OutcomeError {
		t.Fatalf("outcome = %s, want ERROR", got.Outcome)
	}
	if !strings.Contains(got.Message, "unconsumed data") ||
		!strings.Contains(got.Message, "partition 0: earliest=0 latest=45201") {
		t.Errorf("message = %q, want it to name the check and the specifics", got.Message)
	}
	if _, still := broker.Topics["billing-events-0"]; !still {
		t.Fatal("a refused delete must leave the topic in place")
	}
	if contains(broker.Writes, "DeleteTopic") {
		t.Fatal("DeleteTopic was issued despite the failed safety check")
	}
}

func TestRemoveRefusesATopicWithCommittedConsumerOffsets(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
		Name: "billing-events-0", Partitions: 1, ReplicationFactor: 1, Config: map[string]string{},
	}
	// Drained (earliest == latest), but two groups still hold commitments.
	broker.Offsets["billing-events-0"] = []kafkaadmin.PartitionOffsets{{Partition: 0, Earliest: 99, Latest: 99}}
	broker.GroupOffsets["payments-worker"] = map[string][]int32{"billing-events-0": {0}}
	broker.GroupOffsets["audit"] = map[string][]int32{"billing-events-0": {0}}
	broker.GroupOffsets["unrelated"] = map[string][]int32{"other-topic-0": {0}}
	a := removeAssignment(frn0, "billing-events-0", 5, "")

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	got := rec.last()
	if got.Outcome != reconcile.OutcomeError {
		t.Fatalf("outcome = %s, want ERROR", got.Outcome)
	}
	if !strings.Contains(got.Message, "active consumers (groups: audit, payments-worker)") {
		t.Errorf("message = %q", got.Message)
	}
	if _, still := broker.Topics["billing-events-0"]; !still {
		t.Fatal("a refused delete must leave the topic in place")
	}
}

func TestRemoveDeletesWhenOnlyUnrelatedGroupsExist(t *testing.T) {
	r, broker, rec := harness(t)
	broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
		Name: "billing-events-0", Partitions: 1, ReplicationFactor: 1, Config: map[string]string{},
	}
	broker.GroupOffsets["unrelated"] = map[string][]int32{"other-topic-0": {0}}
	a := removeAssignment(frn0, "billing-events-0", 5, "")

	if err := r.Sync(context.Background(), world(a)); err != nil {
		t.Fatal(err)
	}
	if got := rec.last(); got.Outcome != reconcile.OutcomeDeleted {
		t.Fatalf("outcome = %s (%s), want DELETED", got.Outcome, got.Message)
	}
}

// --- PAUSED / SCOPE_LOSS --------------------------------------------------

func TestPausedAndScopeLossTouchNothing(t *testing.T) {
	cases := []struct {
		name string
		a    assign.Assignment
	}{
		{name: "paused", a: assign.Assignment{
			Change: assign.ChangePaused, PartitionFRN: frn0, Generation: 4,
			TopicName: "billing-events-0", ClusterName: "east-1",
			BootstrapServers: []string{"east-1:9092"},
		}},
		{name: "scope loss", a: removeAssignment(frn0, "billing-events-0", 4, assign.ReasonScopeLoss)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, broker, rec := harness(t)
			broker.Topics["billing-events-0"] = &kafkaadmin.Topic{
				Name: "billing-events-0", Partitions: 3, ReplicationFactor: 1, Config: map[string]string{},
			}

			if err := r.Sync(context.Background(), world(tc.a)); err != nil {
				t.Fatal(err)
			}
			if len(broker.Calls) != 0 {
				t.Errorf("Kafka was contacted: %v", broker.Calls)
			}
			if rec.count() != 0 {
				t.Errorf("a dropped partition must not be reported: %+v", rec.reports)
			}
			if _, still := broker.Topics["billing-events-0"]; !still {
				t.Error("the topic must be left exactly as it was")
			}
		})
	}
}

// After a PAUSED the partition is forgotten, so a later SET reconciles it afresh.
func TestResumeAfterPauseReconcilesAgain(t *testing.T) {
	r, broker, rec := harness(t)
	set := setAssignment(frn0, "billing-events-0", 1, 3, 1, nil)
	if err := r.Sync(context.Background(), world(set)); err != nil {
		t.Fatal(err)
	}
	paused := assign.Assignment{
		Change: assign.ChangePaused, PartitionFRN: frn0, Generation: 2,
		TopicName: "billing-events-0", ClusterName: "east-1", BootstrapServers: []string{"east-1:9092"},
	}
	if err := r.Sync(context.Background(), world(paused)); err != nil {
		t.Fatal(err)
	}

	before := rec.count()
	resumed := setAssignment(frn0, "billing-events-0", 3, 3, 1, nil)
	if err := r.Sync(context.Background(), world(resumed)); err != nil {
		t.Fatal(err)
	}
	if rec.count() != before+1 {
		t.Fatalf("got %d new reports, want 1 after a resume", rec.count()-before)
	}
	if got := rec.last(); got.Outcome != reconcile.OutcomeNoop || got.Generation != 3 {
		t.Fatalf("report = %+v, want NOOP at generation 3", got)
	}
	_ = broker
}

// --- multi-cluster --------------------------------------------------------

func TestClustersAreReconciledIndependently(t *testing.T) {
	east, west := kafkaadmin.NewMem(), kafkaadmin.NewMem()
	rec := &recorder{}
	r := reconcile.New(kafkaadmin.MemFactory(map[string]*kafkaadmin.Mem{
		"east-1:9092": east, "west-1:9092": west,
	}), rec, quiet(), nil)
	t.Cleanup(r.Close)

	a := setAssignment(frn0, "billing-events-0", 1, 3, 1, nil)
	b := setAssignment("frn:default:kafka-topic:billing-events-1", "billing-events-1", 1, 3, 1, nil)
	b.ClusterName = "west-1"
	b.BootstrapServers = []string{"west-1:9092"}

	if err := r.Sync(context.Background(), world(a, b)); err != nil {
		t.Fatal(err)
	}
	if _, ok := east.Topics["billing-events-0"]; !ok {
		t.Error("east-1 did not get its topic")
	}
	if _, ok := west.Topics["billing-events-1"]; !ok {
		t.Error("west-1 did not get its topic")
	}
	if _, wrong := east.Topics["billing-events-1"]; wrong {
		t.Error("a partition landed on the wrong cluster")
	}
	if got := len(r.Admins()); got != 2 {
		t.Errorf("cached admins = %d, want one per cluster", got)
	}
}

// --- helpers --------------------------------------------------------------

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func TestConfigDrift(t *testing.T) {
	cases := []struct {
		name            string
		desired, actual map[string]string
		want            map[string]string
	}{
		{name: "no desired keys", desired: nil, actual: map[string]string{"a": "1"}, want: nil},
		{
			name:    "value differs",
			desired: map[string]string{"retention.ms": "1000"},
			actual:  map[string]string{"retention.ms": "2000"},
			want:    map[string]string{"retention.ms": "1000"},
		},
		{
			name:    "key absent on the broker",
			desired: map[string]string{"retention.ms": "1000"},
			actual:  map[string]string{},
			want:    map[string]string{"retention.ms": "1000"},
		},
		{
			name:    "keys franz did not specify are never included",
			desired: map[string]string{"retention.ms": "1000"},
			actual:  map[string]string{"retention.ms": "1000", "cleanup.policy": "compact"},
			want:    nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reconcile.ConfigDrift(tc.desired, tc.actual)
			if len(got) != len(tc.want) {
				t.Fatalf("ConfigDrift() = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("ConfigDrift() = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
