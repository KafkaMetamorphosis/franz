package gregorsamsa_test

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
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa"
	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
)

// TestGregorSamsaEndToEnd is the real-Docker smoke (deliverable 12.18). It is
// opt-in (FRANZ_GS_E2E=1) and needs: a running Franz (REST :8080, gRPC :9090)
// + Postgres, and a working Docker Engine. `make gregorsamsa-e2e` sets it all up.
//
//	register a scoped agent -> register a cluster carrying the matching
//	franz.placement/* labels -> seed a PENDING partition row (placement is
//	deliverable 13) -> start the agent -> the real Kafka topic appears and the row
//	flips to READY with reconciled_generation set -> edit the desired config ->
//	the topic is altered -> delete the channel -> the topic is safety-checked and
//	removed -> repeat on a topic holding records and prove the safety check
//	refuses to delete it.
func TestGregorSamsaEndToEnd(t *testing.T) {
	if os.Getenv("FRANZ_GS_E2E") != "1" {
		t.Skip("set FRANZ_GS_E2E=1 (and run `make gregorsamsa-e2e`) for the real-Docker smoke")
	}
	rest := envOr("FRANZ_REST_ENDPOINT", "http://localhost:8080")
	grpcEndpoint := envOr("FRANZ_ENDPOINT", "localhost:9090")
	dsn := envOr("FRANZ_TEST_DB_DSN", "postgres://franz:franz@localhost:5432/franz?sslmode=disable")
	requireDocker(t)
	requireFranz(t, rest)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	stamp := time.Now().UnixNano()
	var (
		agentName   = fmt.Sprintf("e2e-gs-%d", stamp)
		clusterName = fmt.Sprintf("e2e-gs-cluster-%d", stamp)
		channelName = fmt.Sprintf("e2e-gs-channel-%d", stamp)
		shardName   = channelName + "-0"
		scopeValue  = fmt.Sprintf("e2e-%d", stamp)
		bootstrap   = "localhost:19093"
		container   = fmt.Sprintf("franz-gs-e2e-%d", stamp)
	)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer pool.Close()

	// 1. a real single-node KRaft broker.
	startBroker(t, ctx, container, bootstrap)

	kcl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		t.Fatal(err)
	}
	defer kcl.Close()
	adm := kadm.NewClient(kcl)

	// 2. register the agent with a scope, and a cluster that satisfies it.
	var created struct {
		Token string `json:"token"`
	}
	post(t, rest+"/v1/kafka/agents", map[string]any{
		"name": agentName,
		"type": "AGENT_TYPE_RESOURCE_PROVIDER",
		"labels": map[string]string{
			"franz.placement-selector/env": scopeValue,
		},
	}, &created)
	if created.Token == "" {
		t.Fatal("no token returned from agent registration")
	}
	t.Cleanup(func() { _, _ = httpDo(http.MethodDelete, rest+"/v1/kafka/agents/"+agentName, nil) })

	post(t, rest+"/v1/kafka/clusters", map[string]any{
		"name":               clusterName,
		"connection_strings": []map[string]any{{"bootstrap_urls": []string{bootstrap}}},
		"labels":             map[string]string{"franz.placement/env": scopeValue},
	}, nil)

	post(t, rest+"/v1/async-channels", map[string]any{
		"name": channelName, "type": "CHANNEL_TYPE_KAFKA_TOPIC", "channel_partitions": 1,
	}, nil)

	// 3. seed the PENDING partition row directly — placement is deliverable 13.
	insertShard(t, ctx, pool, channelName, clusterName, shardName, 1, 1,
		`{"retention.ms": "604800000"}`)
	t.Cleanup(func() {
		_, _ = httpDo(http.MethodDelete, rest+"/v1/async-channels/"+channelName, nil)
		_, _ = pool.Exec(context.Background(), `DELETE FROM kafka_topic WHERE name=$1`, shardName)
		_, _ = httpDo(http.MethodDelete, rest+"/v1/kafka/clusters/"+clusterName, nil)
	})

	// 4. run the agent.
	t.Setenv("FRANZ_ENDPOINT", grpcEndpoint)
	t.Setenv("FRANZ_TOKEN", created.Token)
	t.Setenv("FRANZ_AGENT_NAME", agentName)
	t.Setenv("FRANZ_TELEMETRY_INTERVAL", "5s")
	cfg, err := gregorsamsa.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	agent, err := gregorsamsa.NewAgent(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), kafkaadmin.NewKadm)
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
	})

	// 5. the topic appears and the row flips to READY with the generation stamped.
	waitForTopic(t, ctx, adm, shardName, true)
	waitForShardState(t, ctx, pool, shardName, "READY")

	var generation, reconciled int64
	if err := pool.QueryRow(ctx,
		`SELECT generation, COALESCE(reconciled_generation, -1) FROM kafka_topic WHERE name=$1`,
		shardName).Scan(&generation, &reconciled); err != nil {
		t.Fatal(err)
	}
	if reconciled != generation {
		t.Fatalf("reconciled_generation = %d, want %d", reconciled, generation)
	}
	if got := topicConfig(t, ctx, adm, shardName)["retention.ms"]; got != "604800000" {
		t.Errorf("retention.ms on the real topic = %q, want 604800000", got)
	}

	// 6. editing the desired config re-drives the reconcile. Placement is the
	//    normal producer of a materialized_configuration change (deliverable 13),
	//    so the test writes the new desired state and then triggers a Franz-side
	//    mutation — SetConsumption — to bump the generation and push the delta.
	if _, err := pool.Exec(ctx, `
		UPDATE kafka_topic
		SET materialized_configuration = '{"retention.ms": "3600000"}'::jsonb, updated_at = now()
		WHERE name = $1`, shardName); err != nil {
		t.Fatal(err)
	}
	post(t, rest+"/v1/kafka/topics/"+shardName+":setConsumption", map[string]any{
		"consumption": "CONSUMPTION_DISABLED",
	}, nil)

	waitForCondition(t, ctx, 3*time.Minute, "retention altered on the broker", func() bool {
		return topicConfig(t, ctx, adm, shardName)["retention.ms"] == "3600000"
	})
	waitForCondition(t, ctx, 2*time.Minute, "the new generation to be confirmed", func() bool {
		var gen, rec int64
		if err := pool.QueryRow(ctx,
			`SELECT generation, COALESCE(reconciled_generation, -1) FROM kafka_topic WHERE name=$1`,
			shardName).Scan(&gen, &rec); err != nil {
			return false
		}
		return gen == rec && gen > generation
	})

	// 7. deleting the channel cascades DELETED to the shard, which reaches the
	//    agent as REMOVED. The topic is empty and unconsumed, so both safety
	//    checks pass and it is deleted for real.
	if _, err := httpDo(http.MethodDelete, rest+"/v1/async-channels/"+channelName, nil); err != nil {
		t.Fatalf("DeleteAsyncChannel: %v", err)
	}
	waitForTopic(t, ctx, adm, shardName, false)

	// 8. the deletion safety check: a topic that still holds records is never
	//    destroyed. Same flow, but a producer writes to it first.
	guardedChannel := channelName + "-guarded"
	guardedShard := guardedChannel + "-0"
	post(t, rest+"/v1/async-channels", map[string]any{
		"name": guardedChannel, "type": "CHANNEL_TYPE_KAFKA_TOPIC", "channel_partitions": 1,
	}, nil)
	insertShard(t, ctx, pool, guardedChannel, clusterName, guardedShard, 1, 1, `{}`)
	t.Cleanup(func() {
		_, _ = httpDo(http.MethodDelete, rest+"/v1/async-channels/"+guardedChannel, nil)
		_, _ = pool.Exec(context.Background(), `DELETE FROM kafka_topic WHERE name=$1`, guardedShard)
	})

	// The row was inserted behind Franz's back, so nudge the agent with a
	// Franz-side mutation that publishes a delta for it.
	post(t, rest+"/v1/kafka/topics/"+guardedShard+":setConsumption", map[string]any{
		"consumption": "CONSUMPTION_DISABLED",
	}, nil)
	waitForTopic(t, ctx, adm, guardedShard, true)

	if err := kcl.ProduceSync(ctx,
		&kgo.Record{Topic: guardedShard, Value: []byte("keep me")}).FirstErr(); err != nil {
		t.Fatalf("produce to %q: %v", guardedShard, err)
	}

	if _, err := httpDo(http.MethodDelete, rest+"/v1/async-channels/"+guardedChannel, nil); err != nil {
		t.Fatalf("DeleteAsyncChannel(guarded): %v", err)
	}
	waitForCondition(t, ctx, 3*time.Minute, "the refused delete to be reported", func() bool {
		var message string
		if err := pool.QueryRow(ctx,
			`SELECT last_reconcile_message FROM kafka_topic WHERE name=$1`, guardedShard).
			Scan(&message); err != nil {
			return false
		}
		return strings.Contains(message, "unconsumed data")
	})
	// And, crucially, the topic is still there.
	topics, err := adm.ListTopics(ctx, guardedShard)
	if err != nil {
		t.Fatal(err)
	}
	if detail, ok := topics[guardedShard]; !ok || detail.Err != nil {
		t.Fatalf("topic %q was deleted despite holding unconsumed records", guardedShard)
	}
}

// --- fixtures -------------------------------------------------------------

func startBroker(t *testing.T, ctx context.Context, container, bootstrap string) {
	t.Helper()
	_, port, err := splitHostPort(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{
		"run", "-d", "--name", container, "-p", port + ":9092",
		"-e", "KAFKA_NODE_ID=1",
		"-e", "KAFKA_PROCESS_ROLES=broker,controller",
		"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
		"-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + bootstrap,
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
		"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"-e", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
		"-e", "CLUSTER_ID=franz-gs-e2e-cluster-00",
		"apache/kafka:3.9.0",
	}
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("start broker: %v (%s)", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })

	waitForCondition(t, ctx, 3*time.Minute, "broker reachable", func() bool {
		cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
		if err != nil {
			return false
		}
		defer cl.Close()
		probe, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err = kadm.NewClient(cl).ListBrokers(probe)
		return err == nil
	})
}

func insertShard(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	channelName, clusterName, shardName string, partitions, rf int32, materialized string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO kafka_topic
			(id, realm_id, async_channel_id, kafka_cluster_id, name, frn,
			 topic_configuration, materialized_configuration, partitions,
			 replication_factor, state, consumption, generation)
		SELECT gen_random_uuid(), ac.realm_id, ac.id, kc.id, $3,
		       'default:kafka-topic:' || $3, '{}'::jsonb, $6::jsonb, $4, $5,
		       'PENDING', 'ENABLED', 1
		FROM async_channel ac
		JOIN kafka_cluster kc ON kc.realm_id = ac.realm_id AND kc.name = $2
		WHERE ac.name = $1`,
		channelName, clusterName, shardName, partitions, rf, materialized); err != nil {
		t.Fatalf("insert shard: %v", err)
	}
}

// --- waits ----------------------------------------------------------------

func waitForCondition(t *testing.T, ctx context.Context, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			t.Fatalf("context cancelled waiting for %s", what)
		}
		if ok() {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitForTopic(t *testing.T, ctx context.Context, adm *kadm.Client, topic string, want bool) {
	t.Helper()
	verb := "to appear"
	if !want {
		verb = "to be deleted"
	}
	waitForCondition(t, ctx, 3*time.Minute, "topic "+topic+" "+verb, func() bool {
		listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		topics, err := adm.ListTopics(listCtx, topic)
		if err != nil {
			return false
		}
		detail, present := topics[topic]
		return (present && detail.Err == nil) == want
	})
}

func waitForShardState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, shardName, want string) {
	t.Helper()
	var last string
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		var state, message string
		if err := pool.QueryRow(ctx,
			`SELECT state, last_reconcile_message FROM kafka_topic WHERE name=$1`, shardName).
			Scan(&state, &message); err == nil {
			if state == want {
				return
			}
			last = state + " (" + message + ")"
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("shard %s never reached %s; last seen %s", shardName, want, last)
}

func topicConfig(t *testing.T, ctx context.Context, adm *kadm.Client, topic string) map[string]string {
	t.Helper()
	configs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatalf("describe configs for %q: %v", topic, err)
	}
	out := map[string]string{}
	for _, rc := range configs {
		if rc.Name != topic || rc.Err != nil {
			continue
		}
		for _, c := range rc.Configs {
			if c.Value != nil {
				out[c.Key] = *c.Value
			}
		}
	}
	return out
}

// --- helpers --------------------------------------------------------------

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitHostPort(addr string) (host, port string, err error) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("no port in %q", addr)
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
