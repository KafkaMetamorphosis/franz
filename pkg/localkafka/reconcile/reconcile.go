// Package reconcile converges the running Docker containers to the set of
// cluster assignments Franz has pushed (ADR 004 §6). It is level-triggered and
// idempotent: a full re-sync must not disturb a correct broker.
package reconcile

import (
	"context"
	"strings"
	"time"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/assign"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/docker"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/recipe"
)

// freshProbeAttempts / freshProbeDelay give a just-started broker time to come
// up before the agent calls it DEGRADED (KRaft takes ~10–40s).
const (
	freshProbeAttempts = 12
	freshProbeDelay    = 5 * time.Second
)

// Phase mirrors the provider phase strings Franz stores.
type Phase string

const (
	PhaseProvisioning Phase = "PROVISIONING"
	PhaseReady        Phase = "READY"
	PhaseDegraded     Phase = "DEGRADED"
	PhaseError        Phase = "ERROR"
	PhaseStopped      Phase = "STOPPED"
	PhaseRemoved      Phase = "REMOVED"
)

// Reporter emits a status report for one cluster.
type Reporter interface {
	Report(ctx context.Context, clusterName string, phase Phase, reachable bool, message, recipeRef string) error
}

// Prober decides READY vs DEGRADED for a provisioned broker.
type Prober func(ctx context.Context, bootstrapURL string) (ready bool, message string)

// Reconciler holds the agent's convergence state. lastPhase dedups status
// reports so a periodic re-sync does not spam an unchanged phase.
type Reconciler struct {
	agentName      string
	defaultVersion string
	docker         docker.Driver
	report         Reporter
	probe          Prober

	// ProbeAttempts / ProbeDelay bound the READY wait for a just-started broker.
	ProbeAttempts int
	ProbeDelay    time.Duration

	lastPhase map[string]Phase
}

// New wires a reconciler.
func New(agentName, defaultVersion string, d docker.Driver, r Reporter, p Prober) *Reconciler {
	return &Reconciler{
		agentName:      agentName,
		defaultVersion: defaultVersion,
		docker:         d,
		report:         r,
		probe:          p,
		ProbeAttempts:  freshProbeAttempts,
		ProbeDelay:     freshProbeDelay,
		lastPhase:      map[string]Phase{},
	}
}

// Sync converges to desired — the full current assignment world.
func (rc *Reconciler) Sync(ctx context.Context, desired map[string]assign.Assignment) error {
	running, err := rc.docker.List(ctx, rc.agentName)
	if err != nil {
		return err
	}
	byName := make(map[string]docker.Container, len(running))
	for _, c := range running {
		byName[c.Name] = c
	}
	touched := map[string]bool{}

	for name, a := range desired {
		switch a.Change {
		case assign.ChangeRemoved:
			rc.handleRemoved(ctx, name, byName, touched)
		case assign.ChangePaused:
			rc.handlePaused(ctx, name, byName, touched)
		default: // ChangeSet
			rc.handleSet(ctx, a, byName, touched)
		}
	}

	// Orphans — our containers with no assignment at all. (A deleted cluster
	// arrives as a REMOVED assignment via deliverable 05, so this is rare:
	// hand-started containers, or a bug.) Remove the container, keep the volume.
	for _, c := range running {
		if touched[c.Name] {
			continue
		}
		_ = rc.docker.Remove(ctx, c.ID, "", false)
	}
	return nil
}

func (rc *Reconciler) handleRemoved(ctx context.Context, name string, byName map[string]docker.Container, touched map[string]bool) {
	cn := "franz-" + name
	if c, ok := byName[cn]; ok {
		touched[c.Name] = true
		_ = rc.docker.Remove(ctx, c.ID, cn+"-data", true)
	}
	rc.reportOnce(ctx, name, PhaseRemoved, false, "cluster deleted — substrate torn down", recipe.LocalDocker+"@removed")
	delete(rc.lastPhase, name)
}

func (rc *Reconciler) handlePaused(ctx context.Context, name string, byName map[string]docker.Container, touched map[string]bool) {
	cn := "franz-" + name
	if c, ok := byName[cn]; ok {
		touched[c.Name] = true
		if c.Running {
			_ = rc.docker.Stop(ctx, c.ID)
		}
	}
	rc.reportOnce(ctx, name, PhaseStopped, false, "cluster paused — container stopped", recipe.LocalDocker+"@paused")
}

func (rc *Reconciler) handleSet(ctx context.Context, a assign.Assignment, byName map[string]docker.Container, touched map[string]bool) {
	name := a.ClusterName
	spec, err := recipe.Render(rc.agentName, a, rc.defaultVersion)
	if err != nil {
		rc.reportOnce(ctx, name, PhaseError, false, err.Error(), recipe.LocalDocker+"@error")
		return
	}
	ref := spec.RecipeRef()
	warn := strings.Join(spec.Warnings, "; ")

	cur, exists := byName[spec.ContainerName]
	if exists {
		touched[cur.Name] = true
	}
	needCreate := !exists
	// For a SET assignment a hash mismatch or a not-running container both mean
	// "rebuild it" — the container crashed, was stopped by hand, or its spec
	// changed. The data volume is always kept.
	needRecreate := exists && (cur.RecipeHash != spec.Hash() || !cur.Running)
	justStarted := needCreate || needRecreate

	if needCreate || needRecreate {
		rc.reportOnce(ctx, name, PhaseProvisioning, false, withWarn("provisioning broker", warn), ref)
		if needRecreate {
			_ = rc.docker.Stop(ctx, cur.ID)
			_ = rc.docker.Remove(ctx, cur.ID, spec.VolumeName, false) // keep the data volume
		}
		if err := rc.docker.EnsureImage(ctx, spec.Image); err != nil {
			rc.reportOnce(ctx, name, PhaseError, false, "image pull: "+err.Error(), ref)
			return
		}
		id, err := rc.docker.Create(ctx, spec)
		if err != nil {
			rc.reportOnce(ctx, name, PhaseError, false, "create: "+err.Error(), ref)
			return
		}
		if err := rc.docker.Start(ctx, id); err != nil {
			rc.reportOnce(ctx, name, PhaseError, false, "start: "+err.Error(), ref)
			return
		}
	}

	ready, msg := rc.checkReady(ctx, spec.AdvertisedURL, justStarted)
	phase := PhaseReady
	if !ready {
		phase = PhaseDegraded
	}
	rc.reportOnce(ctx, name, phase, ready, withWarn(msg, warn), ref)
}

// checkReady probes the broker. For a just-started container it retries for a
// while so a normal boot goes PROVISIONING → READY without a transient DEGRADED.
func (rc *Reconciler) checkReady(ctx context.Context, url string, justStarted bool) (bool, string) {
	if rc.probe == nil {
		return true, "broker running"
	}
	attempts := 1
	if justStarted && rc.ProbeAttempts > 1 {
		attempts = rc.ProbeAttempts
	}
	var msg string
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return false, "cancelled while waiting for the broker"
			case <-time.After(rc.ProbeDelay):
			}
		}
		var ok bool
		if ok, msg = rc.probe(ctx, url); ok {
			return true, msg
		}
	}
	return false, msg
}

// reportOnce reports only when the phase changed since the last report for this
// cluster (ADR §6 "report each outcome" ⇒ each state transition).
func (rc *Reconciler) reportOnce(ctx context.Context, name string, phase Phase, reachable bool, message, ref string) {
	if rc.lastPhase[name] == phase {
		return
	}
	rc.lastPhase[name] = phase
	_ = rc.report.Report(ctx, name, phase, reachable, message, ref)
}

func withWarn(msg, warn string) string {
	if warn == "" {
		return msg
	}
	return msg + " (" + warn + ")"
}
