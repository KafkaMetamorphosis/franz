// Package recipe turns a cluster Assignment into a container Spec. Feature 1
// ships one recipe family — local-docker (ADR 004 §5): a single apache/kafka
// KRaft container per cluster.
package recipe

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/assign"
)

const (
	// DeploymentTypeLabel selects the recipe family (ADR 004 §3).
	DeploymentTypeLabel = "franz.provisioning/deployment-type"
	KafkaVersionLabel   = "franz.provisioning/kafka-version"
	// KafkaImageLabel overrides the container image with a full ref (tag,
	// digest, or registry mirror). Must be an apache/kafka-compatible image —
	// the recipe renders the KRaft env for that image. Takes precedence over
	// KafkaVersionLabel (ADR 004 §3, ADR-API-008).
	KafkaImageLabel = "franz.provisioning/kafka-image"
	BrokersLabel    = "franz.provisioning/brokers"

	// LocalDocker is the only deployment type this agent handles.
	LocalDocker = "local-docker"

	// Container labels (ADR 004 §5) — the agent's only persistent state.
	LabelManagedBy  = "franz.managed-by"
	LabelCluster    = "franz.cluster"
	LabelRecipeHash = "franz.recipe-hash"

	logDir = "/var/lib/kafka/data"
)

// Spec is a fully-rendered container definition — the input to the Docker driver
// and the thing that gets hashed.
type Spec struct {
	ClusterName   string
	ContainerName string
	Image         string
	// Env is sorted "KEY=VALUE" (deterministic for hashing).
	Env []string
	// HostPort is the published host port; container 9092 maps to it.
	HostPort int
	// VolumeName is the named data volume, mounted at logDir.
	VolumeName string
	Labels     map[string]string
	// AdvertisedURL is the bootstrap address clients use (for the readiness probe).
	AdvertisedURL string
	// Warnings are non-fatal recipe notes (e.g. brokers>1 ignored) for the
	// status message.
	Warnings []string
}

// Hash is the stable digest of the parts that define the running container.
// A change here means "recreate".
func (s Spec) Hash() string {
	h := sha256.New()
	fmt.Fprintf(h, "image=%s\n", s.Image)
	fmt.Fprintf(h, "port=%d\n", s.HostPort)
	fmt.Fprintf(h, "volume=%s@%s\n", s.VolumeName, logDir)
	for _, e := range s.Env { // already sorted
		fmt.Fprintf(h, "env=%s\n", e)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RecipeRef is "local-docker@<hash8>" for ReportClusterStatus.recipe_ref.
func (s Spec) RecipeRef() string { return LocalDocker + "@" + s.Hash()[:8] }

// configKeyAllowed reports whether a cluster_configuration key is safe to pass
// straight through as a broker setting. Unknown keys are dropped with a warning.
var configKeyAllowed = map[string]bool{
	"default.replication.factor":               true,
	"num.partitions":                           true,
	"log.retention.ms":                         true,
	"log.retention.bytes":                      true,
	"log.segment.bytes":                        true,
	"message.max.bytes":                        true,
	"compression.type":                         true,
	"min.insync.replicas":                      true,
	"offsets.topic.replication.factor":         true,
	"transaction.state.log.replication.factor": true,
	"transaction.state.log.min.isr":            true,
	"auto.create.topics.enable":                true,
}

// Render builds the local-docker Spec for an assignment. It returns an error
// only for a genuinely unrenderable assignment (wrong deployment type, no / bad
// bootstrap URL); brokers>1 and unknown config keys are warnings, not errors.
func Render(agentName string, a assign.Assignment, defaultVersion string) (Spec, error) {
	if dt := a.Provisioning[DeploymentTypeLabel]; dt != "" && dt != LocalDocker {
		return Spec{}, fmt.Errorf("unsupported %s=%q (this agent only handles %q)", DeploymentTypeLabel, dt, LocalDocker)
	}

	host, port, err := splitHostPort(a.BootstrapURL())
	if err != nil {
		return Spec{}, fmt.Errorf("bad bootstrap url %q: %w", a.BootstrapURL(), err)
	}

	version := a.Provisioning[KafkaVersionLabel]
	if version == "" {
		version = defaultVersion
	}
	image := a.Provisioning[KafkaImageLabel]
	if image == "" {
		image = "apache/kafka:" + version
	}

	var warnings []string
	if b := a.Provisioning[BrokersLabel]; b != "" && b != "1" {
		warnings = append(warnings, fmt.Sprintf("franz.provisioning/brokers=%s ignored — local-docker provisions a single node", b))
	}

	advertised := net.JoinHostPort(host, strconv.Itoa(port))
	env := map[string]string{
		"KAFKA_NODE_ID":                                  "1",
		"KAFKA_PROCESS_ROLES":                            "broker,controller",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
		"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
		"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://" + advertised,
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
		"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
		"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
		"KAFKA_LOG_DIRS":                                 logDir,
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		"KAFKA_NUM_PARTITIONS":                           "1",
		"CLUSTER_ID":                                     "franz-local-cluster-000",
	}
	for k, v := range a.Configuration {
		if !configKeyAllowed[k] {
			warnings = append(warnings, fmt.Sprintf("cluster_configuration key %q not applied (not in the local-docker allow-list)", k))
			continue
		}
		env["KAFKA_"+strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(k))] = v
	}

	envList := make([]string, 0, len(env))
	for k, v := range env {
		envList = append(envList, k+"="+v)
	}
	sort.Strings(envList)

	spec := Spec{
		ClusterName:   a.ClusterName,
		ContainerName: "franz-" + a.ClusterName,
		Image:         image,
		Env:           envList,
		HostPort:      port,
		VolumeName:    "franz-" + a.ClusterName + "-data",
		AdvertisedURL: advertised,
		Warnings:      warnings,
	}
	spec.Labels = map[string]string{
		LabelManagedBy:  agentName,
		LabelCluster:    a.ClusterFRN,
		LabelRecipeHash: spec.Hash(),
	}
	return spec, nil
}

func splitHostPort(url string) (string, int, error) {
	url = strings.TrimPrefix(strings.TrimPrefix(url, "PLAINTEXT://"), "//")
	if url == "" {
		return "", 0, fmt.Errorf("empty")
	}
	// take the first entry if it is a comma list
	if i := strings.IndexByte(url, ','); i >= 0 {
		url = url[:i]
	}
	host, portStr, err := net.SplitHostPort(url)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("bad port %q", portStr)
	}
	if host == "" {
		host = "localhost"
	}
	return host, port, nil
}
