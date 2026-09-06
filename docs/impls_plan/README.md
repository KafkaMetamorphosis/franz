# Franz — Implementation Plan

The Franz control plane, built one deliverable at a time. **Files are numbered in
execution order.** Each is one shippable unit with its own task list and
acceptance criteria. Update a file's **Status** and its tasks' **Landed**
(commit + date) as work progresses; keep the table below in sync.

Status: ⬜ not started · 🚧 in progress · ✅ done · ⛔ blocked

Spec references (`003-franz/003.x`, `002-monorepo-structure`, `001-ux`,
`004-local-kafka-docker-agent`, `DECISIONS.md`) point at the
**`KafkaMetamorphosis/docs`** repo. `franz/api/franz/v1/*.proto` is authoritative
for message/RPC shapes.

## Feature 1 — Local Kafka via a Docker Cluster Provider agent

Register a `CLUSTER_PROVIDER` agent in the console; register a Kafka Cluster
pointing at it; the agent brings that cluster up in Docker on the local machine
and reports it healthy. Design: **`004-local-kafka-docker-agent`** (ADR).

Locked by the ADR: server-streaming assignment feed + unary status report;
registration bearer token; provisioning intent via `franz.provisioning/*` labels
(no new proto field); `cluster_provider_event` append log; agent = Go in the
Franz module, Docker Engine API SDK, stateless (Docker labels are the store);
`local-docker` recipe = one `apache/kafka` KRaft container per cluster.

| # | Deliverable | Depends on | Status |
|---|---|---|---|
| [01](./01-project-scaffolding.md) | Project scaffolding | — | ✅ |
| [02](./02-domain-foundations.md) | Domain foundations (`003.1`) | 01 | ✅ |
| [03](./03-kafka-cluster.md) | Kafka Cluster | 02 | ✅ |
| [04](./04-agent-registry.md) | Agent registry | 02 | ✅ |
| [05](./05-agent-interaction-cluster-provider.md) | Agent interaction (Cluster Provider) | 02 · 03 · 04 | ✅ |
| [06](./06-web-console-bootstrap.md) | Web console bootstrap | 03 · 04 · 05 | ✅ |
| [07](./07-local-kafka-docker-agent.md) | local-kafka-docker-agent | 05 · 06 | ✅ |

## Rest of the control plane

| # | Deliverable | Depends on | Status |
|---|---|---|---|
| [08](./08-access-policy-engine.md) | Access-policy engine | 02 | ⬜ |
| [09](./09-kafka-topic.md) | Kafka Topic (read model) | 02 · 03 | ⬜ |
| [10](./10-async-channel.md) | Async Channel + access-policy wiring | 02 · 08 · 09 | ⬜ |
| [11](./11-placement.md) | Placement & selection | 03 · 10 | ⬜ |
| [12](./12-telemetry-ingest.md) | Telemetry ingest | 02 · 13 | ⬜ |
| [13](./13-governance.md) | Governance (non-placement actions) | 02 · 03 · 09 · 10 · 12 | ⬜ |
| [14](./14-client.md) | Client | 02 · 10 · 12 | ⬜ |
| [15](./15-migration-and-data-movement.md) | Migration & data movement | 09 · 10 · 11 | ⛔ |

## Decisions already locked (`DECISIONS.md` ADR-API-005)

| | Decision |
|---|---|
| Query layer | Hand-written `pgx/v5`; no ORM/codegen. Dynamic `List*` = parameterised `WHERE` in Go. |
| Lost updates | `SELECT … FOR UPDATE` in the update txn. No version column / client token. |
| Realm | Seed one `default` realm in `V1__init.sql`; a context resolver returns it until auth exists. |
| Config | `config.yaml` + `FRANZ_`-prefixed env overrides via `koanf`, wired through `fx`. |
| Placement | Absent selector → no candidates; unplaced shard → `PENDING`/`NULL` + retry sweep; placed shards never move silently. |
| Governance | Full write whitelist (`003.8`); conflict = `(weight desc, name asc)` last-wins; no anti-thrash; event-driven per sample. |
| Telemetry | Indicators pre-registered; samples + consumer-group obs are append-only 30-day time series. |
| Migration | Drain-based v1, no byte copy; data-copy mechanism still open. |
| Shard routing key | Deferred to a future SDK ADR. |
| API authz | `003.2` placeholder; stub allow-all interceptor near-term. |

## Blockers

| Blocked | On |
|---|---|
| **15** migration flow, and the real moves it unblocks (placed-shard relocation, cluster-delete-with-live-topics, re-shard execution, governance placement/taint actions) | `003.13` OQ1–2 — data-copy mechanism + RPC surface |
| Real API authorization | `003.2` model undecided (stub for now) |
| Control-plane event log | `003.11` OQ4 — design not started |
| SDK / client library (shard routing) | routing-key ADR not written |

## Testing strategy

- **Domain / usecases** — table-driven unit tests, no DB. Selector grammar,
  access-policy evaluation, state machines, config merge get exhaustive cases.
- **Adapters (postgres)** — integration tests against a real Postgres
  (docker-compose / testcontainers); run in CI.
- **grpc-gateway** — a few end-to-end tests per service through the REST gateway
  (status codes, `BadRequest` details, pagination).
- **Contract** — `buf breaking` against `main`; generated-code freshness check.

## Progress log

_(newest first — date · deliverable/task · note · commit)_

- 2026-09-06 · **07** local-kafka-docker-agent · `cmd/local-kafka-agent` +
  `pkg/localkafka/{assign,stream,recipe,docker,reconcile,probe}` — connects as a
  CLUSTER_PROVIDER, watches assignments, renders the `local-docker` recipe,
  brings up an apache/kafka KRaft container, converges + reports status.
  franz-go readiness probe. Fake-driver unit tests + a real-Docker e2e
  (`make agent-e2e`, opt-in). `make agent TOKEN=…`. Executed by claude.
- 2026-09-06 · **06** Web console bootstrap · Vite/React/TS console
  (`webconsole/`) — shell, Login stub, Agents + Kafka Clusters screens; typed
  REST client generated from a `buf`-emitted OpenAPI spec; TanStack Query;
  4s provider-status poll. Vitest + Playwright (scoped-down) smoke. Two new CI
  jobs. Executed by claude.
- 2026-09-06 · **05** Agent interaction (Cluster Provider) · agent-auth
  interceptor, `streamhub` connected-agent registry, `provider` domain +
  usecase, `clusters.Service` publishes assignment deltas, `ClusterProviderService`
  handler (WatchClusterAssignments stream + ReportClusterStatus),
  `cluster_provider_event` table + nightly prune, `provider_status` on
  `GetKafkaCluster`, `ListClusterProviderEvents`. bufconn e2e + postgres
  integration green. Executed by claude.
- 2026-09-06 · **04** Agent registry · `core/domain/agent` (type + status
  machine), `pkg/shared/token` (bearer-token mint/hash), `agents.Service`
  (Create mints token, RotateToken), `AgentService` gRPC+REST, `agent`
  migration. Unit + Postgres integration + REST e2e green. Executed by claude.
- 2026-09-06 · **03** Kafka Cluster · first full vertical slice — domain state
  machine, `ClusterRepository` (pgx, `SELECT … FOR UPDATE`), `clusters.Service`,
  `KafkaClusterService` gRPC+REST handler, `kafka_cluster` migration. Unit +
  Postgres integration tests green; REST e2e verified. Executed by claude.
- 2026-09-06 · **02** Domain foundations · FRN / naming / selector+glob / errs /
  pagetoken / errmap / fieldmask / postgres plumbing + boot migrations / realm +
  context / allow-all auth interceptor. Unit tests green; pg integration tests
  self-skip without `FRANZ_TEST_DB_DSN`. Executed by claude (codex over quota).
- 2026-09-06 · **01** Project scaffolding · Go module at `franz/`, buf codegen
  (edition 2024 + `use_opaque_api`), `fx` boot, `/healthz`, CI. `81cdaca` / `342f44e`.
