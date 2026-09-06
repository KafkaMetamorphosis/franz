// Package provider is the Cluster Provider interaction domain (004 ADR): the
// status an agent reports for a cluster it owns, and the assignment (desired
// state) Franz pushes to that agent. It is a leaf package — no dependency on
// the cluster entity, so cluster.Cluster can carry a *provider.Status.
package provider

import (
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
)

// Phase is the provider-reported lifecycle phase (004 ADR §4), distinct from
// KafkaCluster.state (operator intent, which an agent never writes).
type Phase string

const (
	PhaseProvisioning Phase = "PROVISIONING"
	PhaseReady        Phase = "READY"
	PhaseDegraded     Phase = "DEGRADED"
	PhaseError        Phase = "ERROR"
	PhaseStopped      Phase = "STOPPED"
	PhaseRemoved      Phase = "REMOVED"
)

// Valid reports whether p is a known phase.
func (p Phase) Valid() bool {
	switch p {
	case PhaseProvisioning, PhaseReady, PhaseDegraded, PhaseError, PhaseStopped, PhaseRemoved:
		return true
	default:
		return false
	}
}

// Event is one append-only status report for a cluster (`cluster_provider_event`).
type Event struct {
	ClusterID      uuid.UUID
	ClusterFRN     frn.FRN
	RealmID        uuid.UUID
	Phase          Phase
	Reachable      bool
	Message        string
	ReportingAgent string
	RecipeRef      string
	OccurredAt     time.Time
}

// NewEvent validates a report from an agent.
func NewEvent(
	clusterID, realmID uuid.UUID, clusterFRN frn.FRN,
	phase Phase, reachable bool, message, reportingAgent, recipeRef string,
	occurredAt time.Time,
) (*Event, error) {
	if !phase.Valid() {
		return nil, errs.InvalidField("phase", "must be one of PROVISIONING, READY, DEGRADED, ERROR, STOPPED, REMOVED")
	}
	return &Event{
		ClusterID:      clusterID,
		ClusterFRN:     clusterFRN,
		RealmID:        realmID,
		Phase:          phase,
		Reachable:      reachable,
		Message:        message,
		ReportingAgent: reportingAgent,
		RecipeRef:      recipeRef,
		OccurredAt:     occurredAt,
	}, nil
}

// Status is the "current provider status" projection — the latest Event for a
// cluster — surfaced on GetKafkaCluster (004 ADR §4).
type Status struct {
	Phase          Phase
	Reachable      bool
	Message        string
	RecipeRef      string
	ReportingAgent string
	ReportedAt     time.Time
}

// Change is how an assignment changed for the owning agent (004 ADR "New proto").
type Change string

const (
	ChangeSet     Change = "SET"
	ChangePaused  Change = "PAUSED"
	ChangeRemoved Change = "REMOVED"
)

// ConnectionString mirrors a cluster connection string for the agent's recipe
// (kept local so this package stays a leaf).
type ConnectionString struct {
	BootstrapURLs []string
	Type          string
}

// Assignment is the desired state of one cluster, pushed to its owning agent
// over WatchClusterAssignments.
type Assignment struct {
	Change            Change
	ClusterName       string
	ClusterFRN        frn.FRN
	ConnectionStrings []ConnectionString
	Configuration     map[string]string
	// Provisioning is the cluster's franz.provisioning/* labels (those keys only).
	Provisioning map[string]string
}

// ProvisioningPrefix is the reserved label prefix carrying provisioning intent
// (003.1 / 004 ADR §3).
const ProvisioningPrefix = "franz.provisioning/"

// ProvisioningLabels returns the subset of labels under ProvisioningPrefix.
func ProvisioningLabels(labels map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range labels {
		if len(k) > len(ProvisioningPrefix) && k[:len(ProvisioningPrefix)] == ProvisioningPrefix {
			out[k] = v
		}
	}
	return out
}
