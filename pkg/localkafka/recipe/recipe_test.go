package recipe

import (
	"slices"
	"strings"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/assign"
)

func base() assign.Assignment {
	return assign.Assignment{
		Change:        assign.ChangeSet,
		ClusterName:   "local-1",
		ClusterFRN:    "frn:default:kafka-cluster:local-1",
		BootstrapURLs: []string{"localhost:9092"},
		Provisioning:  map[string]string{DeploymentTypeLabel: LocalDocker},
	}
}

func envOf(spec Spec, key string) (string, bool) {
	for _, e := range spec.Env {
		if k, v, ok := strings.Cut(e, "="); ok && k == key {
			return v, true
		}
	}
	return "", false
}

func TestRenderLocalDocker(t *testing.T) {
	spec, err := Render("local-kafka-agent", base(), "3.7.0")
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if spec.Image != "apache/kafka:3.7.0" {
		t.Errorf("image = %q", spec.Image)
	}
	if spec.ContainerName != "franz-local-1" || spec.VolumeName != "franz-local-1-data" {
		t.Errorf("names: %+v", spec)
	}
	if spec.HostPort != 9092 {
		t.Errorf("host port = %d", spec.HostPort)
	}
	if adv, _ := envOf(spec, "KAFKA_ADVERTISED_LISTENERS"); adv != "PLAINTEXT://localhost:9092" {
		t.Errorf("advertised = %q", adv)
	}
	if roles, _ := envOf(spec, "KAFKA_PROCESS_ROLES"); roles != "broker,controller" {
		t.Errorf("roles = %q", roles)
	}
	if !slices.IsSorted(spec.Env) {
		t.Error("Env must be sorted for a stable hash")
	}
	if spec.Labels[LabelManagedBy] != "local-kafka-agent" ||
		spec.Labels[LabelCluster] != "frn:default:kafka-cluster:local-1" ||
		spec.Labels[LabelRecipeHash] != spec.Hash() {
		t.Errorf("labels: %v", spec.Labels)
	}
	if got := spec.RecipeRef(); !strings.HasPrefix(got, "local-docker@") || len(got) != len("local-docker@")+8 {
		t.Errorf("recipe ref = %q", got)
	}
}

func TestRenderVersionAndHash(t *testing.T) {
	a := base()
	s1, _ := Render("a", a, "3.7.0")

	a.Provisioning[KafkaVersionLabel] = "3.8.0"
	s2, _ := Render("a", a, "3.7.0")
	if s2.Image != "apache/kafka:3.8.0" {
		t.Fatalf("image = %q", s2.Image)
	}
	if s1.Hash() == s2.Hash() {
		t.Error("hash must change with the image tag")
	}

	// same inputs → same hash (agent name is not in the hash)
	s3, _ := Render("b", base(), "3.7.0")
	if s1.Hash() != s3.Hash() {
		t.Errorf("hash not stable: %s vs %s", s1.Hash(), s3.Hash())
	}
}

func TestRenderKafkaImageOverridesVersion(t *testing.T) {
	a := base()
	a.Provisioning[KafkaVersionLabel] = "3.8.0"
	a.Provisioning[KafkaImageLabel] = "registry.example.com/apache/kafka:3.9.0"

	s, err := Render("a", a, "3.7.0")
	if err != nil {
		t.Fatal(err)
	}
	if s.Image != "registry.example.com/apache/kafka:3.9.0" {
		t.Fatalf("image = %q, kafka-image should win over kafka-version", s.Image)
	}

	// a different image ref changes the hash → the container is recreated
	base1, _ := Render("a", base(), "3.7.0")
	if s.Hash() == base1.Hash() {
		t.Error("hash must change with the image ref")
	}
}

func TestRenderAllowlistAndBrokers(t *testing.T) {
	a := base()
	a.Configuration = map[string]string{"num.partitions": "6", "totally.unknown": "x"}
	a.Provisioning[BrokersLabel] = "3"

	spec, err := Render("a", a, "3.7.0")
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := envOf(spec, "KAFKA_NUM_PARTITIONS"); v != "6" {
		t.Errorf("allow-listed key not applied: %q", v)
	}
	if _, ok := envOf(spec, "KAFKA_TOTALLY_UNKNOWN"); ok {
		t.Error("unknown config key leaked into env")
	}
	joined := strings.Join(spec.Warnings, " ")
	if !strings.Contains(joined, "brokers=3") || !strings.Contains(joined, "totally.unknown") {
		t.Errorf("warnings = %v", spec.Warnings)
	}
}

func TestRenderErrors(t *testing.T) {
	wrong := base()
	wrong.Provisioning[DeploymentTypeLabel] = "k8s-strimzi"
	if _, err := Render("a", wrong, "3.7.0"); err == nil {
		t.Error("unsupported deployment type should error")
	}

	noURL := base()
	noURL.BootstrapURLs = nil
	if _, err := Render("a", noURL, "3.7.0"); err == nil {
		t.Error("missing bootstrap url should error")
	}

	badURL := base()
	badURL.BootstrapURLs = []string{"not-a-host-port"}
	if _, err := Render("a", badURL, "3.7.0"); err == nil {
		t.Error("bad bootstrap url should error")
	}
}
