// Package reconcile makes the real Kafka topics match the partition assignments
// Franz streams (005 ADR §1.4). It holds the in-memory desired world keyed by
// partition FRN, diffs each incoming set against what it last applied, and
// issues AdminClient calls only where the topic actually diverges — a full
// resync of an unchanged fleet performs zero Kafka calls.
//
// There is no periodic drift loop: the control plane is the only thing that
// triggers a reconcile. Divergence introduced outside Franz shows up in the
// telemetry sweep (`kafka.topic.state`), not as a silent repair.
package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/assign"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
)

// Outcome is what one reconcile attempt achieved (005 ADR §1.5).
type Outcome string

const (
	OutcomeCreated Outcome = "CREATED"
	OutcomeUpdated Outcome = "UPDATED"
	OutcomeNoop    Outcome = "NOOP"
	OutcomeDeleted Outcome = "DELETED"
	OutcomeError   Outcome = "ERROR"
)

// Report is one outcome, ready to send to Franz.
type Report struct {
	PartitionFRN string
	Generation   int64
	Outcome      Outcome
	Message      string
	// Applied is the state read back off the broker, nil when no read succeeded.
	Applied *kafkaadmin.Topic
}

// Reporter delivers an outcome to Franz.
type Reporter interface {
	Report(ctx context.Context, r Report) error
}

// Observer is called right after a partition reconciles, with the admin for its
// cluster, so the telemetry sweep can publish a fresh sample without waiting for
// the next sweep (005 ADR §2.2 "Cadence"). Optional.
type Observer func(ctx context.Context, a assign.Assignment, admin kafkaadmin.Admin)

// applied is what the reconciler last did for one partition.
type applied struct {
	assignment assign.Assignment
	outcome    Outcome
}

// Reconciler converges Kafka to the assignments Franz streams.
type Reconciler struct {
	factory  kafkaadmin.Factory
	reporter Reporter
	log      *slog.Logger
	observer Observer

	mu      sync.Mutex
	applied map[string]applied      // partitionFRN -> last attempt
	admins  map[string]*cachedAdmin // clusterName -> AdminClient
	locks   map[string]*sync.Mutex  // clusterName -> serialises calls on one broker
}

type cachedAdmin struct {
	admin     kafkaadmin.Admin
	bootstrap []string
}

// New builds a reconciler. observer may be nil.
func New(factory kafkaadmin.Factory, reporter Reporter, log *slog.Logger, observer Observer) *Reconciler {
	return &Reconciler{
		factory:  factory,
		reporter: reporter,
		log:      log,
		observer: observer,
		applied:  map[string]applied{},
		admins:   map[string]*cachedAdmin{},
		locks:    map[string]*sync.Mutex{},
	}
}

// Close releases every cached AdminClient.
func (r *Reconciler) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, c := range r.admins {
		c.admin.Close()
		delete(r.admins, name)
	}
}

// Partitions returns the SET assignments the reconciler is currently managing —
// the topics the telemetry sweep observes. Partitions the agent stopped managing
// (PAUSED, scope loss) and deleted ones are not included.
func (r *Reconciler) Partitions() []assign.Assignment {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]assign.Assignment, 0, len(r.applied))
	for _, a := range r.applied {
		if a.assignment.Change == assign.ChangeSet {
			out = append(out, a.assignment)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartitionFRN < out[j].PartitionFRN })
	return out
}

// Admins returns the cached AdminClient of every cluster the reconciler has
// touched, keyed by cluster name — the connections the telemetry sweep reuses.
func (r *Reconciler) Admins() map[string]kafkaadmin.Admin {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]kafkaadmin.Admin, len(r.admins))
	for name, c := range r.admins {
		out[name] = c.admin
	}
	return out
}

// Sync converges to the given world: every in-scope partition, keyed by
// partition FRN, exactly as the stream last described it.
//
// Partitions are processed sequentially per cluster (one AdminClient call at a
// time, so the reconciler never races itself on a broker) and clusters proceed
// in parallel (005 ADR §1.7).
func (r *Reconciler) Sync(ctx context.Context, world map[string]assign.Assignment) error {
	r.forgetAbsent(world)

	byCluster := map[string][]assign.Assignment{}
	for _, a := range world {
		// PAUSED and scope-loss REMOVED are pure drops: stop managing the
		// partition, touch nothing, report nothing.
		if a.Change == assign.ChangePaused || a.IsScopeLoss() {
			r.forget(a.PartitionFRN)
			continue
		}
		if a.Change != assign.ChangeSet && a.Change != assign.ChangeRemoved {
			continue // unknown change from a newer Franz; ignore rather than guess
		}
		if r.settled(a) {
			continue
		}
		byCluster[a.ClusterName] = append(byCluster[a.ClusterName], a)
	}

	var wg sync.WaitGroup
	for clusterName, assignments := range byCluster {
		sort.Slice(assignments, func(i, j int) bool {
			return assignments[i].TopicName < assignments[j].TopicName
		})
		wg.Add(1)
		go func(clusterName string, assignments []assign.Assignment) {
			defer wg.Done()
			r.syncCluster(ctx, clusterName, assignments)
		}(clusterName, assignments)
	}
	wg.Wait()
	return ctx.Err()
}

// syncCluster reconciles one cluster's partitions in order, holding that
// cluster's lock for the whole run.
func (r *Reconciler) syncCluster(ctx context.Context, clusterName string, assignments []assign.Assignment) {
	lock := r.clusterLock(clusterName)
	lock.Lock()
	defer lock.Unlock()

	for _, a := range assignments {
		if ctx.Err() != nil {
			return
		}
		admin, err := r.adminFor(ctx, a)
		if err != nil {
			// The whole cluster is unreachable; every partition on it fails
			// (005 ADR §1.6).
			r.finish(ctx, a, Report{
				PartitionFRN: a.PartitionFRN, Generation: a.Generation,
				Outcome: OutcomeError,
				Message: fmt.Sprintf("kafka cluster %q unreachable: %v", clusterName, err),
			}, nil)
			continue
		}

		var report Report
		if a.Change == assign.ChangeRemoved {
			report = r.remove(ctx, admin, a)
		} else {
			report = r.set(ctx, admin, a)
		}
		r.finish(ctx, a, report, admin)
	}
}

// set creates the topic if it is absent, otherwise alters it to match
// (005 ADR §1.4 "SET").
func (r *Reconciler) set(ctx context.Context, admin kafkaadmin.Admin, a assign.Assignment) Report {
	existing, err := admin.DescribeTopic(ctx, a.TopicName)
	if err != nil {
		return errorReport(a, fmt.Sprintf("describe topic: %v", err), nil)
	}

	if existing == nil {
		if err := admin.CreateTopic(ctx, a.TopicName, a.Partitions, a.ReplicationFactor, a.DesiredConfig); err != nil {
			return errorReport(a, fmt.Sprintf("create topic: %v", err), nil)
		}
		return r.readBack(ctx, admin, a, OutcomeCreated)
	}

	// Neither reduction is something Gregor Samsa may do: a partition decrease is
	// destructive and Kafka offers no primitive for it, and an RF change needs a
	// partition-reassignment plan (005 ADR OQ3, a future part).
	if a.ReplicationFactor > 0 && existing.ReplicationFactor != a.ReplicationFactor {
		return errorReport(a, fmt.Sprintf(
			"cannot change replication factor from %d to %d (needs a partition reassignment plan)",
			existing.ReplicationFactor, a.ReplicationFactor), existing)
	}
	if a.Partitions < existing.Partitions {
		return errorReport(a, fmt.Sprintf("cannot reduce partition count from %d to %d",
			existing.Partitions, a.Partitions), existing)
	}

	changed := false
	if a.Partitions > existing.Partitions {
		if err := admin.CreatePartitions(ctx, a.TopicName, a.Partitions); err != nil {
			return errorReport(a, fmt.Sprintf("create partitions: %v", err), existing)
		}
		changed = true
	}
	if drift := configDrift(a.DesiredConfig, existing.Config); len(drift) > 0 {
		if err := admin.AlterConfigs(ctx, a.TopicName, drift); err != nil {
			return errorReport(a, fmt.Sprintf("alter configs: %v", err), existing)
		}
		changed = true
	}

	outcome := OutcomeNoop
	if changed {
		outcome = OutcomeUpdated
	}
	return r.readBack(ctx, admin, a, outcome)
}

// remove deletes the topic behind the two hard safety checks (005 ADR §1.4
// "REMOVED"). Either check failing means no delete and an ERROR report naming
// the check and its specifics.
func (r *Reconciler) remove(ctx context.Context, admin kafkaadmin.Admin, a assign.Assignment) Report {
	existing, err := admin.DescribeTopic(ctx, a.TopicName)
	if err != nil {
		return errorReport(a, fmt.Sprintf("describe topic: %v", err), nil)
	}
	if existing == nil {
		// Already gone — idempotent success, no checks to run.
		return Report{PartitionFRN: a.PartitionFRN, Generation: a.Generation, Outcome: OutcomeDeleted,
			Message: "topic was already absent"}
	}

	offsets, err := admin.ListOffsets(ctx, a.TopicName)
	if err != nil {
		return errorReport(a, fmt.Sprintf("deletion safety check (unconsumed data): %v", err), existing)
	}
	for _, p := range offsets {
		if p.HasData() {
			return errorReport(a, fmt.Sprintf(
				"topic has unconsumed data (partition %d: earliest=%d latest=%d)",
				p.Partition, p.Earliest, p.Latest), existing)
		}
	}

	groups, err := admin.ListConsumerGroups(ctx)
	if err != nil {
		return errorReport(a, fmt.Sprintf("deletion safety check (consumer groups): %v", err), existing)
	}
	var committed []string
	for _, group := range groups {
		partitions, err := admin.ListConsumerGroupOffsets(ctx, group, a.TopicName)
		if err != nil {
			return errorReport(a, fmt.Sprintf(
				"deletion safety check (consumer group %q offsets): %v", group, err), existing)
		}
		if len(partitions) > 0 {
			committed = append(committed, group)
		}
	}
	if len(committed) > 0 {
		sort.Strings(committed)
		return errorReport(a, "topic has active consumers (groups: "+strings.Join(committed, ", ")+")", existing)
	}

	if err := admin.DeleteTopic(ctx, a.TopicName); err != nil {
		return errorReport(a, fmt.Sprintf("delete topic: %v", err), existing)
	}
	return Report{PartitionFRN: a.PartitionFRN, Generation: a.Generation, Outcome: OutcomeDeleted}
}

// readBackAttempts / readBackDelay bound the wait for a freshly-created topic to
// show up in metadata. CreateTopics returns as soon as the controller accepts
// it, so the very next describe can legitimately still say "absent" — that is
// propagation, not failure, and reporting ERROR for it would flap the row.
const (
	readBackAttempts = 20
	readBackDelay    = 250 * time.Millisecond
)

// readBack re-describes the topic so the report carries what the broker actually
// accepted. A failed read-back downgrades the outcome to ERROR: Franz must not
// mark a shard READY on a state nobody confirmed.
func (r *Reconciler) readBack(
	ctx context.Context, admin kafkaadmin.Admin, a assign.Assignment, outcome Outcome,
) Report {
	verb := strings.ToLower(string(outcome))
	for attempt := range readBackAttempts {
		after, err := admin.DescribeTopic(ctx, a.TopicName)
		if err != nil {
			return errorReport(a, fmt.Sprintf("read back topic after %s: %v", verb, err), nil)
		}
		if after != nil {
			return Report{
				PartitionFRN: a.PartitionFRN, Generation: a.Generation,
				Outcome: outcome, Applied: after,
			}
		}
		if attempt == readBackAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return errorReport(a, "cancelled while reading back topic after "+verb, nil)
		case <-time.After(readBackDelay):
		}
	}
	return errorReport(a, "topic still absent after "+verb+
		" and waiting for it to appear in cluster metadata", nil)
}

// finish records the attempt, sends the report, and fires the observer.
func (r *Reconciler) finish(ctx context.Context, a assign.Assignment, report Report, admin kafkaadmin.Admin) {
	r.remember(a, report.Outcome)

	if err := r.reporter.Report(ctx, report); err != nil {
		// The report is lost, but the attempt is not: forget the partition so the
		// next resync reconciles and reports it again.
		r.forget(a.PartitionFRN)
		r.log.Warn("report failed", "partition", a.PartitionFRN, "outcome", report.Outcome, "err", err)
		return
	}
	level := slog.LevelInfo
	if report.Outcome == OutcomeError {
		level = slog.LevelWarn
	}
	r.log.Log(ctx, level, "reconciled",
		"partition", a.PartitionFRN, "topic", a.TopicName, "cluster", a.ClusterName,
		"generation", a.Generation, "outcome", report.Outcome, "detail", report.Message)

	if r.observer != nil && admin != nil {
		r.observer(ctx, a, admin)
	}
}

// --- desired-state bookkeeping ------------------------------------------

// settled reports whether this exact desired state was already applied
// successfully, so there is nothing to do and no Kafka call to make. A previous
// ERROR is never settled — the op retries on the next resync (005 ADR §1.6).
func (r *Reconciler) settled(a assign.Assignment) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	last, ok := r.applied[a.PartitionFRN]
	return ok && last.outcome != OutcomeError && last.assignment.SameDesiredState(a)
}

func (r *Reconciler) remember(a assign.Assignment, outcome Outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied[a.PartitionFRN] = applied{assignment: a, outcome: outcome}
}

func (r *Reconciler) forget(partitionFRN string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.applied, partitionFRN)
}

// forgetAbsent drops bookkeeping for partitions the world no longer mentions, so
// a partition that reappears is reconciled from scratch.
func (r *Reconciler) forgetAbsent(world map[string]assign.Assignment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for frn := range r.applied {
		if _, ok := world[frn]; !ok {
			delete(r.applied, frn)
		}
	}
}

// --- admin cache --------------------------------------------------------

// adminFor returns the cached AdminClient for the assignment's cluster, opening
// one on first use and reopening it if the cluster's bootstrap servers changed.
func (r *Reconciler) adminFor(ctx context.Context, a assign.Assignment) (kafkaadmin.Admin, error) {
	r.mu.Lock()
	cached, ok := r.admins[a.ClusterName]
	r.mu.Unlock()
	if ok && sameServers(cached.bootstrap, a.BootstrapServers) {
		return cached.admin, nil
	}
	if len(a.BootstrapServers) == 0 {
		return nil, fmt.Errorf("assignment carries no bootstrap servers")
	}

	admin, err := r.factory(ctx, a.BootstrapServers)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.admins[a.ClusterName]; ok {
		prev.admin.Close()
	}
	r.admins[a.ClusterName] = &cachedAdmin{admin: admin, bootstrap: a.BootstrapServers}
	return admin, nil
}

func (r *Reconciler) clusterLock(clusterName string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.locks[clusterName]; ok {
		return l
	}
	l := &sync.Mutex{}
	r.locks[clusterName] = l
	return l
}

// --- helpers ------------------------------------------------------------

func errorReport(a assign.Assignment, message string, applied *kafkaadmin.Topic) Report {
	return Report{
		PartitionFRN: a.PartitionFRN, Generation: a.Generation,
		Outcome: OutcomeError, Message: message, Applied: applied,
	}
}

// ConfigDrift returns the desired keys whose value on the broker differs. Keys
// Franz did not specify are never touched (005 ADR OQ4: least surprise).
func ConfigDrift(desired, actual map[string]string) map[string]string {
	return configDrift(desired, actual)
}

func configDrift(desired, actual map[string]string) map[string]string {
	var drift map[string]string
	for k, want := range desired {
		if got, ok := actual[k]; !ok || got != want {
			if drift == nil {
				drift = map[string]string{}
			}
			drift[k] = want
		}
	}
	return drift
}

func sameServers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
