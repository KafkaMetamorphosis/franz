package reconcile

import (
	"context"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/assign"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/docker"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/recipe"
)

type report struct {
	cluster string
	phase   Phase
	ok      bool
	ref     string
}

type recorder struct{ got []report }

func (r *recorder) Report(_ context.Context, c string, p Phase, ok bool, _, ref string) error {
	r.got = append(r.got, report{c, p, ok, ref})
	return nil
}
func (r *recorder) phases() []Phase {
	out := make([]Phase, len(r.got))
	for i, g := range r.got {
		out[i] = g.phase
	}
	return out
}

func setAssign(name string) assign.Assignment {
	return assign.Assignment{
		Change:        assign.ChangeSet,
		ClusterName:   name,
		ClusterFRN:    "frn:default:kafka-cluster:" + name,
		BootstrapURLs: []string{"localhost:9092"},
		Provisioning:  map[string]string{recipe.DeploymentTypeLabel: recipe.LocalDocker},
	}
}

func desiredOf(as ...assign.Assignment) map[string]assign.Assignment {
	m := map[string]assign.Assignment{}
	for _, a := range as {
		m[a.ClusterName] = a
	}
	return m
}

func newRC(t *testing.T, ready bool) (*Reconciler, *docker.MemDriver, *recorder) {
	t.Helper()
	drv := docker.NewMemDriver("agent-x")
	rep := &recorder{}
	rc := New("agent-x", "3.7.0", drv, rep, func(context.Context, string) (bool, string) {
		return ready, "probe"
	})
	rc.ProbeAttempts, rc.ProbeDelay = 1, 0 // no retry wait in unit tests
	return rc, drv, rep
}

func TestSyncCreatesAndReportsReady(t *testing.T) {
	rc, drv, rep := newRC(t, true)
	if err := rc.Sync(context.Background(), desiredOf(setAssign("local-1"))); err != nil {
		t.Fatal(err)
	}
	if running, ok := drv.Running("franz-local-1"); !ok || !running {
		t.Fatal("container not created + started")
	}
	if !drv.VolumeExists("franz-local-1-data") {
		t.Error("data volume missing")
	}
	want := []Phase{PhaseProvisioning, PhaseReady}
	if got := rep.phases(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("phases = %v, want %v", got, want)
	}
	if hasOp(drv, "pull apache/kafka:3.7.0") == false {
		t.Error("image not pulled")
	}
}

func TestSyncIdempotent(t *testing.T) {
	rc, drv, rep := newRC(t, true)
	ctx := context.Background()
	_ = rc.Sync(ctx, desiredOf(setAssign("local-1")))
	opsAfterFirst := len(drv.Ops)
	reportsAfterFirst := len(rep.got)

	// second identical sync: no docker ops, no new reports (phase unchanged)
	_ = rc.Sync(ctx, desiredOf(setAssign("local-1")))
	if len(drv.Ops) != opsAfterFirst {
		t.Errorf("re-sync performed docker ops: %v", drv.Ops[opsAfterFirst:])
	}
	if len(rep.got) != reportsAfterFirst {
		t.Errorf("re-sync re-reported: %v", rep.got[reportsAfterFirst:])
	}
}

func TestSyncRecreateOnHashChange(t *testing.T) {
	rc, drv, rep := newRC(t, true)
	ctx := context.Background()
	_ = rc.Sync(ctx, desiredOf(setAssign("local-1")))

	changed := setAssign("local-1")
	changed.Provisioning[recipe.KafkaVersionLabel] = "3.8.0"
	rep.got = nil
	if err := rc.Sync(ctx, desiredOf(changed)); err != nil {
		t.Fatal(err)
	}
	if !hasOp(drv, "remove franz-local-1") || !hasOp(drv, "create franz-local-1") {
		t.Fatalf("expected recreate, ops: %v", drv.Ops)
	}
	if hasOp(drv, "rmvol franz-local-1-data") {
		t.Error("recreate must keep the data volume")
	}
	if !drv.VolumeExists("franz-local-1-data") {
		t.Error("data volume was dropped on recreate")
	}
}

func TestSyncPausedAndRemoved(t *testing.T) {
	rc, drv, _ := newRC(t, true)
	ctx := context.Background()
	_ = rc.Sync(ctx, desiredOf(setAssign("local-1")))

	paused := setAssign("local-1")
	paused.Change = assign.ChangePaused
	_ = rc.Sync(ctx, desiredOf(paused))
	if running, _ := drv.Running("franz-local-1"); running {
		t.Error("paused cluster still running")
	}

	removed := setAssign("local-1")
	removed.Change = assign.ChangeRemoved
	_ = rc.Sync(ctx, desiredOf(removed))
	if _, ok := drv.Running("franz-local-1"); ok {
		t.Error("removed cluster container still present")
	}
	if drv.VolumeExists("franz-local-1-data") {
		t.Error("removed cluster volume still present")
	}
}

func TestSyncOrphanRemoval(t *testing.T) {
	rc, drv, _ := newRC(t, true)
	drv.Seed("franz-ghost", "frn:default:kafka-cluster:ghost", "oldhash", true, "franz-ghost-data")
	if err := rc.Sync(context.Background(), desiredOf(setAssign("local-1"))); err != nil {
		t.Fatal(err)
	}
	if _, ok := drv.Running("franz-ghost"); ok {
		t.Error("orphan container not removed")
	}
	if !drv.VolumeExists("franz-ghost-data") {
		t.Error("orphan removal should keep the volume (conservative)")
	}
}

func TestSyncDegradedWhenProbeFails(t *testing.T) {
	rc, _, rep := newRC(t, false)
	_ = rc.Sync(context.Background(), desiredOf(setAssign("local-1")))
	last := rep.got[len(rep.got)-1]
	if last.phase != PhaseDegraded || last.ok {
		t.Fatalf("last report = %+v, want DEGRADED / not reachable", last)
	}
}

func TestSyncRetriesProbeOnFreshBroker(t *testing.T) {
	drv := docker.NewMemDriver("agent-x")
	rep := &recorder{}
	calls := 0
	rc := New("agent-x", "3.7.0", drv, rep, func(context.Context, string) (bool, string) {
		calls++
		return calls >= 3, "probe" // fails twice, then ready
	})
	rc.ProbeAttempts, rc.ProbeDelay = 5, 0

	_ = rc.Sync(context.Background(), desiredOf(setAssign("local-1")))
	// PROVISIONING then READY — no DEGRADED in between
	if got := rep.phases(); len(got) != 2 || got[0] != PhaseProvisioning || got[1] != PhaseReady {
		t.Fatalf("phases = %v, want [PROVISIONING READY]", got)
	}
}

func TestSyncErrorOnBadAssignment(t *testing.T) {
	rc, drv, rep := newRC(t, true)
	bad := setAssign("local-1")
	bad.BootstrapURLs = nil
	_ = rc.Sync(context.Background(), desiredOf(bad))
	if len(rep.got) != 1 || rep.got[0].phase != PhaseError {
		t.Fatalf("reports = %v, want [ERROR]", rep.phases())
	}
	if len(drv.Ops) != 0 {
		t.Errorf("no docker ops expected for an unrenderable assignment: %v", drv.Ops)
	}
}

func hasOp(d *docker.MemDriver, op string) bool {
	for _, o := range d.Ops {
		if o == op {
			return true
		}
	}
	return false
}
