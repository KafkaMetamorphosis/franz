# 07 — local-kafka-docker-agent

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex unavailable (usage limit, resets 2026-09-28)
Depends on: [05](./05-agent-interaction-cluster-provider.md) · [06](./06-web-console-bootstrap.md)
Specs: `004-local-kafka-docker-agent` (the ADR)
Location: `franz/cmd/local-kafka-agent/` + `franz/pkg/localkafka/`

## Goal

The agent binary. Connects to Franz as a registered `CLUSTER_PROVIDER`, watches
its cluster assignments, renders the `local-docker` recipe, and brings a Kafka
broker up in Docker on the local machine — then keeps it converged and reports
status. Go, in the Franz module, Docker Engine API SDK, stateless.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 07.1 | Binary skeleton + config (`FRANZ_ENDPOINT`, `FRANZ_TOKEN`, `DOCKER_HOST`) + structured logging; gRPC client from `pkg/gen/go` with the Bearer-token interceptor | ADR §6 | ✅ | 2026-09-06 |
| 07.2 | Stream loop — `WatchClusterAssignments` with reconnect + backoff; on (re)connect treat the incoming full set as the world; maintain an in-memory desired map | ADR §1, §6 | ✅ | 2026-09-06 |
| 07.3 | Recipe engine (`pkg/localkafka/recipe`) — `local-docker`: intent + `connection_strings` + selected `cluster_configuration` → a container spec (`apache/kafka:<version>`, KRaft combined, `advertised.listeners` from the bootstrap URL); compute `franz.recipe-hash` | ADR §5 | ✅ | 2026-09-06 |
| 07.4 | Docker driver (`pkg/localkafka/docker`) — Engine API SDK: create/start/stop/remove, `ContainerInspect` for state, `ContainerList` by `franz.managed-by=<agent>` label; manage the data volume | ADR §6 | ✅ | 2026-09-06 |
| 07.5 | Reconcile (`pkg/localkafka/reconcile`) — per cluster: no container → create; hash mismatch → recreate; `PAUSED` → stop; `REMOVED` → stop + remove + volume; orphan container (no assignment) → remove. Idempotent; a full re-sync disturbs nothing correct | ADR §6, lifecycle table | ✅ | 2026-09-06 |
| 07.6 | Status reporting — `ReportClusterStatus` on each outcome: `PROVISIONING` → `READY` / `DEGRADED` / `ERROR` / `STOPPED` / `REMOVED`, with `recipe_ref` = `local-docker@<hash>` | ADR §4 | ✅ | 2026-09-06 |
| 07.7 | Readiness probe — how `READY` vs `DEGRADED` is decided (ADR OQ2 — start with a broker API `ListNodes` / metadata probe) | ADR OQ2 | ✅ | 2026-09-06 |
| 07.8 | `brokers > 1` → log a warning and provision a single node (ADR: deferred) | ADR §3 | ✅ | 2026-09-06 |
| 07.9 | Local end-to-end test — real Docker: register (via 07 or a fixture) → agent brings up a broker → a Kafka client connects at the declared `bootstrap_url` and creates a topic; delete → containers + volume gone | — | ✅ | 2026-09-06 |

## Done when

- On a clean machine with Docker: `register agent → register cluster (console) →`
  a broker is reachable at the cluster's `bootstrap_url` and shows `READY`.
- Editing `franz.provisioning/kafka-version` recreates the container on the new tag.
- Deleting the cluster removes the container and its volume.
- Killing + restarting the agent re-syncs without disturbing a healthy broker.

## Notes

- **Not** `gregor-samsa` — that is a Resource Provider (topics). This is a Cluster
  Provider (the substrate).
- Multi-broker, port-conflict handling, and non-bundled recipe distribution are
  ADR open questions, out of scope for Feature 1.

### Decisions (asked)

1. **Kafka client — `github.com/twmb/franz-go`** (pure Go, no CGO). Standing
   choice for future Go Kafka code. Readiness = `kgo.Client.Ping` (ApiVersions
   round-trip); the reconciler retries a fresh broker (≤12 × 5s) so a normal
   boot goes `PROVISIONING → READY` with no transient `DEGRADED`.
2. **07.9 tests — fake Docker for logic + local-only real smoke.**
   `recipe` / `reconcile` / `stream` are unit-tested against an in-memory fake
   Docker driver (in CI). `pkg/localkafka/e2e_test.go` is the real-Docker
   end-to-end (`make agent-e2e`, opt-in `FRANZ_AGENT_E2E=1`) — **not** in CI.
3. **Local dev — `make agent TOKEN=…`** (run after registering an agent in the
   console). `make dev` unchanged; `make agent-e2e` runs the real smoke.

### What landed

| Piece | Path |
|---|---|
| Binary | `cmd/local-kafka-agent/main.go` |
| Config + orchestrator + Bearer creds | `pkg/localkafka/{config.go,agent.go}` |
| Assignment value type | `pkg/localkafka/assign` |
| Stream loop (reconnect + backoff + debounce + resync) | `pkg/localkafka/stream` |
| `local-docker` recipe + hash + allow-list | `pkg/localkafka/recipe` |
| Docker driver — interface + Engine API + in-memory fake | `pkg/localkafka/docker` |
| Reconcile (create / recreate-on-hash / pause / remove / orphan) | `pkg/localkafka/reconcile` |
| Readiness probe (franz-go Ping) | `pkg/localkafka/probe` |
| Real-Docker e2e | `pkg/localkafka/e2e_test.go` |
| Make targets | `Makefile` — `agent`, `agent-e2e` |

Verified end to end on real Docker (`make agent-e2e`): register agent → agent
starts → register cluster → broker up in Docker → `PROVISIONING → READY` (~5 s
after boot) → a franz-go client connects at the declared `bootstrap_url` and
creates a topic → delete cluster → container + volume gone. Fake-driver unit
tests cover idempotent re-sync, recreate-keeps-volume, pause, orphan removal,
DEGRADED, probe-retry, and unrenderable-assignment → ERROR.

### Implementation notes

- **Status reports are deduped by phase** — a periodic re-sync only reports on a
  state transition (ADR §6 "report each outcome" ⇒ each transition), so the
  event log stays clean. An agent restart re-reports the current phase once.
- **Orphan containers** (our label, no assignment — rare, since a delete arrives
  as a `REMOVED` assignment) get the container removed, volume kept.
- **`CLUSTER_ID`** is a fixed value in the recipe env — the `apache/kafka` image
  formats storage on first boot; the data volume persists it across recreates.
