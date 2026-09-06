package localkafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka"
)

// TestLocalDockerEndToEnd is the real-Docker smoke (deliverable 07.9). It is
// opt-in (FRANZ_AGENT_E2E=1) and needs: a running Franz (REST :8080, gRPC :9090)
// + Postgres, and a working Docker Engine. `make agent-e2e` sets it all up.
//
//	register agent -> start agent -> register cluster -> broker reaches READY ->
//	a Kafka client connects at the bootstrap URL and creates a topic ->
//	delete the cluster -> container + volume are gone.
func TestLocalDockerEndToEnd(t *testing.T) {
	if os.Getenv("FRANZ_AGENT_E2E") != "1" {
		t.Skip("set FRANZ_AGENT_E2E=1 (and run `make agent-e2e`) for the real-Docker smoke")
	}
	rest := envOr("FRANZ_REST_ENDPOINT", "http://localhost:8080")
	grpcEndpoint := envOr("FRANZ_ENDPOINT", "localhost:9090")
	requireDocker(t)
	requireFranz(t, rest)

	stamp := time.Now().UnixNano()
	agentName := fmt.Sprintf("e2e-agent-%d", stamp)
	clusterName := fmt.Sprintf("e2e-cluster-%d", stamp)
	bootstrap := "localhost:19092"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 1. register the agent, get its token
	var created struct {
		Agent struct{ Name string } `json:"agent"`
		Token string                `json:"token"`
	}
	post(t, rest+"/v1/kafka/agents", map[string]any{
		"name": agentName, "type": "AGENT_TYPE_CLUSTER_PROVIDER",
	}, &created)
	if created.Token == "" {
		t.Fatal("no token returned")
	}
	t.Cleanup(func() { _, _ = httpDo(http.MethodDelete, rest+"/v1/kafka/agents/"+agentName, nil) })

	// 2. start the agent
	t.Setenv("FRANZ_ENDPOINT", grpcEndpoint)
	t.Setenv("FRANZ_TOKEN", created.Token)
	t.Setenv("FRANZ_AGENT_NAME", agentName)
	cfg, err := localkafka.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	agent, err := localkafka.NewAgent(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	agentCtx, stopAgent := context.WithCancel(ctx)
	agentDone := make(chan struct{})
	go func() { _ = agent.Run(agentCtx); close(agentDone) }()
	t.Cleanup(func() {
		stopAgent()
		<-agentDone
		agent.Close()
		// belt-and-braces: make sure no container survives a failed run
		_ = exec.Command("docker", "rm", "-f", "franz-"+clusterName).Run()
		_ = exec.Command("docker", "volume", "rm", "-f", "franz-"+clusterName+"-data").Run()
	})

	// 3. register the cluster pointing at the agent
	post(t, rest+"/v1/kafka/clusters", map[string]any{
		"name":                   clusterName,
		"connection_strings":     []map[string]any{{"bootstrap_urls": []string{bootstrap}}},
		"cluster_provider_agent": agentName,
		"labels":                 map[string]string{"franz.provisioning/deployment-type": "local-docker"},
	}, nil)

	// 4. wait for provider status READY
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if time.Now().After(deadline) {
			t.Fatal("cluster did not reach READY in time")
		}
		var got struct {
			KafkaCluster struct {
				ProviderStatus struct{ Phase string } `json:"providerStatus"`
			} `json:"kafkaCluster"`
		}
		if err := getJSON(rest+"/v1/kafka/clusters/"+clusterName, &got); err == nil {
			if got.KafkaCluster.ProviderStatus.Phase == "CLUSTER_PROVIDER_PHASE_READY" {
				break
			}
		}
		time.Sleep(3 * time.Second)
	}

	// 5. a Kafka client connects at the declared bootstrap URL and creates a topic
	kcl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		t.Fatal(err)
	}
	defer kcl.Close()
	adm := kadm.NewClient(kcl)
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, "e2e-topic"); err != nil {
		t.Fatalf("create topic against the provisioned broker: %v", err)
	}
	topics, err := adm.ListTopics(ctx)
	if err != nil || !topics.Has("e2e-topic") {
		t.Fatalf("topic not visible: %v", err)
	}

	// 6. delete the cluster; container + volume must go
	if _, err := httpDo(http.MethodDelete, rest+"/v1/kafka/clusters/"+clusterName, nil); err != nil {
		t.Fatal(err)
	}
	gone := time.Now().Add(90 * time.Second)
	for {
		if time.Now().After(gone) {
			t.Fatal("container/volume still present after cluster delete")
		}
		c := exec.Command("docker", "ps", "-aq", "-f", "name=franz-"+clusterName)
		out, _ := c.Output()
		if len(bytes.TrimSpace(out)) == 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
}

// --- helpers -------------------------------------------------------------

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func requireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker not available: %v", err)
	}
}

func requireFranz(t *testing.T, rest string) {
	t.Helper()
	resp, err := http.Get(rest + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Skipf("Franz not reachable at %s: %v", rest, err)
	}
	resp.Body.Close()
}

func post(t *testing.T, url string, body any, out any) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := httpDo(http.MethodPost, url, b)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	if out != nil {
		if err := json.Unmarshal(resp, out); err != nil {
			t.Fatalf("decode %s: %v (%s)", url, err, resp)
		}
	}
}

func getJSON(url string, out any) error {
	b, err := httpDo(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

func httpDo(method, url string, body []byte) ([]byte, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, url, r)
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return b, fmt.Errorf("%s %s -> %d: %s", method, url, resp.StatusCode, b)
	}
	return b, nil
}
