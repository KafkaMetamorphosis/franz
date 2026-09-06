# Changelog

All notable changes to Franz are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **`README.md`** — project overview + local-dev quickstart.
- **Resource management & agent provisioning schema** (impls_plan deliverable 08):
  - `Agent.provisioning_labels` — a new `ProvisioningLabelSpec` (`key`,
    `description`, `allowed_values`, `default_value`, `required`) carried on
    `CreateAgent` / `UpdateAgent` (mask `provisioning_labels`) and returned by
    `GetAgent` / `ListAgents`, stored as `agent.provisioning_labels jsonb`. It is
    **advisory** (ADR-API-008): Franz validates only its own well-formedness and
    never checks a `KafkaCluster`'s labels against it.
  - Web console: **edit pages** for Agent (`/agents/:name/edit` — type, labels,
    provisioning-label schema editor) and Kafka Cluster
    (`/kafka/clusters/:name/edit` — bootstrap URLs, labels, `cluster_configuration`,
    provider agent), each reached from an "Edit" button on the detail page. The
    `update_mask` carries only changed fields; a `409` offers reload-and-re-apply.
    Changing a cluster's provider agent is gated behind an explicit confirm.
  - Console cluster forms render **schema-driven provisioning fields**: pick a
    provider agent and its declared `franz.provisioning/*` labels appear,
    pre-filled with defaults and constrained to allowed values (falling back to
    the common local-docker keys when the agent advertises no schema).
  - `local-docker` recipe: `franz.provisioning/kafka-image` sets a full
    apache/kafka-compatible image ref (tag, digest, or registry mirror),
    precedence over `kafka-version`, feeding the recipe hash.
  - **`franz/local/`** — local-dev infrastructure: `docker-compose.yml`
    (Postgres + a `seed` one-shot) and `seed/*.sql`. `make deps` now applies the
    schema and seeds the `local-kafka-agent` registration (with its provisioning
    schema and a fixed public dev token) **before Franz starts**, so `make agent`
    connects with no console step. Replaces the `FRANZ_REGISTER=1` self-register
    path from deliverable 07 — `pkg/localkafka/register.go` and the `Register`
    config field are removed; the agent takes `FRANZ_TOKEN` only.

- **local-kafka-docker-agent** (impls_plan deliverable 07): `cmd/local-kafka-agent`
  — the first Cluster Provider agent. It registers with Franz, watches
  `WatchClusterAssignments` (reconnect + backoff, debounced into one reconcile),
  renders the `local-docker` recipe (a single `apache/kafka` KRaft container per
  cluster, `advertised.listeners` from the declared bootstrap URL,
  allow-listed `cluster_configuration` → broker env, `franz.recipe-hash` label),
  drives Docker via the Engine API SDK, and converges: create → recreate on hash
  change (keeping the data volume) → stop on `PAUSED` → remove + volume on
  `REMOVED` → drop orphans. Readiness is a `franz-go` `Ping`; a fresh broker is
  retried so a normal boot goes `PROVISIONING → READY` without a transient
  `DEGRADED`. Status is reported per state transition. `pkg/localkafka/{assign,
  stream,recipe,docker,reconcile,probe}`. Fake-Docker unit tests in CI; a
  real-Docker end-to-end (`make agent-e2e`, opt-in) verifies a client can
  connect and create a topic against the provisioned broker. New Make targets
  `agent` / `agent-e2e`. New deps: `github.com/twmb/franz-go`,
  `github.com/docker/docker`. Local dev: `make agent` uses the seeded dev token
  (see `franz/local/` above) — no console step.
- **Web console bootstrap** (impls_plan deliverable 06): `webconsole/` — a
  Vite + React + TypeScript operator console (separate static build, not
  embedded). App shell ported from the `001-ux` prototype; Login stub; **Agents**
  screens (list, register with one-time token reveal, detail with pause / resume
  / delete / rotate-token); **Kafka Clusters** screens (list, register with a
  provider-agent picker + `franz.provisioning/*` fields, detail showing intent
  state + live provider status + the event timeline, polled every 4s). The
  typed REST client is generated from the protos: `buf generate api` now also
  emits `api/openapi/franz.swagger.json`, which `webconsole` turns into
  `src/api/schema.d.ts` (`openapi-typescript` + `openapi-fetch`, wrapped by
  TanStack Query). Vitest component tests + a scoped-down Playwright smoke.
  Two new CI jobs (`webconsole`, `console-e2e`).
- **`Makefile`** for local development: `make dev` starts Postgres, the control
  plane, and the console together (Ctrl-C stops all); plus `make run`,
  `make console`, `make gen`, `make test`, `make e2e`, `make lint`.
- **Agent interaction — Cluster Provider** (impls_plan deliverable 05): the
  Franz side of the `004-local-kafka-docker-agent` contract.
  `core/domain/provider` (phase / status / assignment value objects,
  `franz.provisioning/*` label filter); an agent-auth gRPC interceptor
  (`adapters/in/grpcgateway/agentauth.go`, `WithAgentAuth`) that resolves
  `authorization: Bearer <token>` to the agent for `ClusterProviderService`
  calls only; an in-memory connected-agent stream registry
  (`adapters/streamhub`); `core/usecases/provider` (initial assignments,
  ownership-checked status intake, history); `clusters.Service` now publishes an
  assignment delta to the owning agent on every cluster create/update/pause/
  resume/delete; the `ClusterProviderService` handler
  (`WatchClusterAssignments` server-stream — full set on open then deltas — and
  `ReportClusterStatus`); a `cluster_provider_event` append table
  (`adapters/out/postgres/provider.go`) with a nightly 30-day prune; and
  `KafkaCluster.provider_status` + `ListClusterProviderEvents` on the console
  API. `pkg/internal/dbtest` serialises the DB integration tests.
- **Agent registry** (impls_plan deliverable 04): `core/domain/agent` (entity,
  `AgentType` organisational filter, `ACTIVE ↔ PAUSED → DELETED` status machine),
  `core/ports/{in,out}` + `core/usecases/agents`, a hand-written pgx adapter
  (`adapters/out/postgres/agent.go`, type filter pushed to SQL), and the
  `AgentService` gRPC + REST handler (`/v1/kafka/agents` with `:pause` /
  `:resume` / `:rotateToken`). `CreateAgent` mints a one-time bearer token
  (`pkg/shared/token`: `frnat_` + 32 random bytes; only the sha256 is stored);
  `RotateAgentToken` replaces it. New `agent` table with a `token_hash` column.
  Registration is inert — no connection, no work protocol (that is a later ADR).
- **Kafka Cluster** (impls_plan deliverable 03): the first full
  `domain → ports → postgres → grpc-gateway` vertical slice —
  `core/domain/cluster` (entity + `ACTIVE ↔ PAUSED → DELETED` state machine),
  `core/ports/in.KafkaClusterService` / `core/ports/out.ClusterRepository`,
  `core/usecases/clusters`, a hand-written pgx adapter
  (`adapters/out/postgres/cluster.go`, `Mutate` = `SELECT … FOR UPDATE` in one
  transaction), and the `KafkaClusterService` gRPC + REST handler
  (`adapters/in/grpcgateway/kafkacluster.go`, `/v1/kafka/clusters` with
  `:pause` / `:resume`). New `kafka_cluster` table (`migrations/V1__init.sql`).
  Soft delete; `(realm_id, name)` unconditionally unique; label-selector `List`
  with opaque pagination. `ListClusterProviderEvents` is left `Unimplemented`
  until deliverable 05.
- `pkg/shared/fieldmask`: `CanonicalPaths` helper; `update_mask` added to the
  immutable set.
- **Project scaffolding** (impls_plan deliverable 01): Go module
  `github.com/KafkaMetamorphosis/franz` rooted at `franz/`; hexagonal package
  skeleton (`cmd/franz`, `pkg/franz/core/{domain,usecases,ports}`,
  `pkg/franz/adapters/{in/grpcgateway,out/postgres}`, `pkg/franz/config`,
  `pkg/shared`); `buf` codegen to committed `pkg/gen/go`; `docker-compose.yml`
  (Postgres 16 + Flyway) with an empty `migrations/V1__init.sql`;
  `cmd/franz` `fx` application booting a gRPC server + grpc-gateway mux + a
  `GET /healthz` probe; `koanf` config (`config.yaml` + `FRANZ_` env overrides);
  GitHub Actions CI (`buf lint`/`buf breaking`, `go vet`/`build`/`test`,
  generated-code freshness check).
- **Domain foundations** (impls_plan deliverable 02): the `003.1` cross-cutting
  primitives every later deliverable reuses —
  the `FRN` (Franz Resource Name) value object (`core/domain/frn`) with a
  `Codec` for the configurable `resource_prefix` (default `frn`; `frn:` / `orn:`
  accepted as aliases; FRNs stored prefix-less) — ADR-API-007,
  resource-name validation (`core/domain/naming`),
  the label-selector grammar + matcher (`core/domain/selector`) and `*` / `\*`
  glob (`pkg/shared/glob`),
  a transport-free domain error vocabulary (`core/domain/errs`) with a
  gRPC-status + `google.rpc.BadRequest` mapper (`adapters/in/grpcgateway/errmap.go`),
  an opaque pagination-cursor codec (`pkg/shared/pagetoken`, default 50 / cap 1000),
  a `google.protobuf.FieldMask` apply helper (`pkg/shared/fieldmask`; empty-mask
  reject, `name` immutable, map/repeated wholesale),
  hand-written `pgx/v5` Postgres plumbing (`adapters/out/postgres/db.go`:
  pool, `WithTx`, embedded Flyway-compatible migrations run on boot when
  `db.auto_migrate` is set),
  the `Realm` value object + request-context plumbing (`core/domain/realm`) with
  a repository (`core/ports/out`, `adapters/out/postgres/realm.go`) and the
  seeded `default` realm in `migrations/V1__init.sql`,
  and an allow-all authenticator (`adapters/in/grpcgateway/interceptor.go`) that
  resolves the realm into context on every inbound path (gRPC unary + stream,
  gateway HTTP) — the `003.2` seam.

### Changed

- `config`: added `db.auto_migrate` (default `true`) and `resource_prefix`
  (default `frn`; an invalid value fails the boot).
- Proto: the resource-identifier field is `frn` (was `orn`); likewise
  `client_frn` / `resource_frn` / `cluster_frn`. `pkg/gen/go` regenerated.
- `grpcgateway.New` takes functional options; `WithAuthenticator` installs the
  realm interceptors.
- Repository reset from the Clojure implementation to the Go monorepo on the
  `go-monorepo` branch.
