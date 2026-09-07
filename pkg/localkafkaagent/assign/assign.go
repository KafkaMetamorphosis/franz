package assign

import franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"

// Change is the desired disposition of a cluster (mirrors
// ClusterAssignment.Change).
type Change string

const (
	ChangeSet     Change = "SET"
	ChangePaused  Change = "PAUSED"
	ChangeRemoved Change = "REMOVED"
)

// Assignment is one cluster the agent owns, decoded from the proto so the recipe
// / reconcile packages don't depend on generated types.
type Assignment struct {
	Change        Change
	ClusterName   string
	ClusterFRN    string
	BootstrapURLs []string
	Configuration map[string]string
	Provisioning  map[string]string
}

// FromProto decodes a ClusterAssignment stream message.
func FromProto(a *franzv1.ClusterAssignment) Assignment {
	out := Assignment{
		ClusterName:   a.GetClusterName(),
		ClusterFRN:    a.GetClusterFrn(),
		Configuration: a.GetClusterConfiguration(),
		Provisioning:  a.GetProvisioning(),
	}
	switch a.GetChange() {
	case franzv1.ClusterAssignment_CHANGE_PAUSED:
		out.Change = ChangePaused
	case franzv1.ClusterAssignment_CHANGE_REMOVED:
		out.Change = ChangeRemoved
	default:
		out.Change = ChangeSet
	}
	for _, cs := range a.GetConnectionStrings() {
		out.BootstrapURLs = append(out.BootstrapURLs, cs.GetBootstrapUrls()...)
	}
	return out
}

// BootstrapURL is the first bootstrap URL, or "" — the address the recipe
// advertises and the readiness probe dials.
func (a Assignment) BootstrapURL() string {
	if len(a.BootstrapURLs) == 0 {
		return ""
	}
	return a.BootstrapURLs[0]
}
