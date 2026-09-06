# 07 — local-kafka-docker-agent

Status: ⬜ not started
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
| 07.1 | Binary skeleton + config (`FRANZ_ENDPOINT`, `FRANZ_TOKEN`, `DOCKER_HOST`) + structured logging; gRPC client from `pkg/gen/go` with the Bearer-token interceptor | ADR §6 | ⬜ | |
| 07.2 | Stream loop — `WatchClusterAssignments` with reconnect + backoff; on (re)connect treat the incoming full set as the world; maintain an in-memory desired map | ADR §1, §6 | ⬜ | |
| 07.3 | Recipe engine (`pkg/localkafka/recipe`) — `local-docker`: intent + `connection_strings` + selected `cluster_configuration` → a container spec (`apache/kafka:<version>`, KRaft combined, `advertised.listeners` from the bootstrap URL); compute `franz.recipe-hash` | ADR §5 | ⬜ | |
| 07.4 | Docker driver (`pkg/localkafka/docker`) — Engine API SDK: create/start/stop/remove, `ContainerInspect` for state, `ContainerList` by `franz.managed-by=<agent>` label; manage the data volume | ADR §6 | ⬜ | |
| 07.5 | Reconcile (`pkg/localkafka/reconcile`) — per cluster: no container → create; hash mismatch → recreate; `PAUSED` → stop; `REMOVED` → stop + remove + volume; orphan container (no assignment) → remove. Idempotent; a full re-sync disturbs nothing correct | ADR §6, lifecycle table | ⬜ | |
| 07.6 | Status reporting — `ReportClusterStatus` on each outcome: `PROVISIONING` → `READY` / `DEGRADED` / `ERROR` / `STOPPED` / `REMOVED`, with `recipe_ref` = `local-docker@<hash>` | ADR §4 | ⬜ | |
| 07.7 | Readiness probe — how `READY` vs `DEGRADED` is decided (ADR OQ2 — start with a broker API `ListNodes` / metadata probe) | ADR OQ2 | ⬜ | |
| 07.8 | `brokers > 1` → log a warning and provision a single node (ADR: deferred) | ADR §3 | ⬜ | |
| 07.9 | Local end-to-end test — real Docker: register (via 07 or a fixture) → agent brings up a broker → a Kafka client connects at the declared `bootstrap_url` and creates a topic; delete → containers + volume gone | — | ⬜ | |

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
