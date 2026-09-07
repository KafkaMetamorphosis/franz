# Changelog

All notable changes to Franz are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Placement & selection** (impls_plan deliverable 13): Franz now decides which
  Kafka Cluster each async-channel shard lives on, and materialises the shard
  rows (`003.7`, ADR-API-009). New pure domain package
  `core/domain/placement` implements the selection algorithm — candidates
  (`state = ACTIVE` and labels satisfy `franz.affinity/selector`; an **absent**
  selector yields no candidates, so placement is opt-in) → drop
  `franz.antiaffinity/selector` matches → drop `drain`-tainted and untolerated
  `no-creation`-tainted clusters → order by (`franz.affinity/weight` desc, name
  asc) and take `min(franz.affinity/shard-size, |candidates|)` → distribute the
  channel's shards round-robin, the earlier clusters taking the remainder on an
  uneven split. Identical inputs always produce an identical assignment.
- **Shard materialisation**: `kafka_topic` rows are created by placement and only
  ever for a shard that has a concrete cluster — `partitions` /
  `replication_factor` / `materialized_configuration` are seeded from that
  cluster's `cluster_configuration` (keys `partitions` / `replication-factor`,
  which are now excluded from the config merge). A channel with no eligible
  cluster keeps zero shard rows and its create still succeeds. Triggers: channel
  create, a channel `franz.*` label change, and any cluster create / label /
  state change; a retry sweep (`FRANZ_PLACEMENT__SWEEP_INTERVAL`, default `30s`)
  is the safety net.
- **Misplaced marker**: `kafka_topic` gains `misplaced` / `misplaced_reason`
  (also on the `KafkaTopic` proto as additive fields 14 / 15). A placed shard
  whose cluster left the channel's affinity, went `PAUSED`/`DELETED`, or gained a
  `drain` taint is marked and **not moved** — relocation is the migration flow
  (`003.13`). The marker clears when the cluster matches again, and it never
  bumps `generation`.
- **Reserved placement labels are validated on write**: a malformed
  `franz.affinity/selector`, `franz.antiaffinity/selector`,
  `franz.affinity/shard-size`, `franz.affinity/weight`, `franz.taint` or
  `franz.taint/toleration` is rejected with `INVALID_ARGUMENT` on the channel or
  cluster create/update.
- Placement changes are pushed to the in-scope Resource Provider agents through
  the deliverable-12 notifier, so a materialised shard reaches a connected agent
  as a `SET` `PartitionAssignment` without a reconnect.
- **Gregor Samsa — Resource Provider agent** (impls_plan deliverable 12): the
  second agent-interaction contract and its reference implementation (005 ADR
  Parts 1 + 2). New gRPC `ResourceProviderService` (`agent_resource_provider.proto`,
  no REST gateway): `WatchPartitionAssignments` (server stream — full in-scope
  set on open, deltas after) and `ReportPartitionReconciliation` (unary,
  generation-gated — a stale report is acknowledged but does not move the row to
  `READY`). **Scope** is a pure server-side conjunction of exact label pairs —
  a cluster is in scope iff it carries `franz.placement/<k>=<v>` for every
  `franz.placement-selector/<k>=<v>` on the agent; an empty selector matches
  **nothing**. Scope is dynamic: agent-selector, cluster-label and channel/shard
  changes emit `SET` for newly-in-scope partitions and `REMOVED` (`SCOPE_LOSS`)
  for departed ones. `kafka_topic` gains `reconciled_generation` /
  `last_reconcile_message`; `RecordReconciliation` maps `CREATED`/`UPDATED`/`NOOP`
  → `READY`, `DELETED` → `DELETED`, `ERROR` → `ERROR`. The `gregorsamsa` agent
  (`cmd/gregorsamsa`, plain packages, no `fx`) holds one cached Kafka
  `AdminClient` (franz-go `kadm`, with an in-memory fake) per in-scope cluster,
  reconciles sequentially per cluster / parallel across clusters (create · alter
  · increase-only partitions · RF-decrease → `ERROR`), guards deletion with two
  hard checks (unconsumed data, committed consumer offsets), and sweeps
  topic/broker telemetry (default 60s + a sample right after each reconcile).
  **Telemetry**: additive `StreamIndicatorSamples` client-streaming RPC on
  `TelemetryService`; a minimal `indicator_sample` append table (30-day nightly
  prune) that deliverable 14 will adopt. `agentauth.go` widened to the Resource
  Provider + Telemetry services. `streamhub` generalised to a typed `fanout[T]`.
  `pkg/franz/core/domain/{scope,indicator}`, `domain/topic/reconcile.go`,
  `usecases/{resourceprovider,telemetry}`,
  `adapters/{in/grpcgateway/{resourceprovider,telemetry},out/postgres/indicator}`,
  `pkg/gregorsamsa/*`. `Makefile`: `gregorsamsa` + `gregorsamsa-e2e`
  (`FRANZ_GS_E2E=1`, real Docker — written, not run in CI). `local/seed/` now
  registers a `local-1` Kafka Cluster (wired to `local-kafka-agent`,
  `franz.placement/env=local`) alongside the `gregor-samsa` agent
  (`franz.placement-selector/env=local`), so the whole local loop —
  `make dev` + `make agent` + `make gregorsamsa` — has a cluster in scope with
  no console step.
- **Async Channel console screens** (impls_plan deliverable 19): the web console
  can now manage Async Channels end to end — `/async-channels` list (name + FRN,
  type, channel partitions, labels, state), `/async-channels/register`
  (name, the sole `kafka-topic` type, `channel_partitions`, labels),
  `/async-channels/:name` detail (declared intent, pause / resume / delete) and
  `/async-channels/:name/edit`. Editing is **labels-only**, matching
  `UpdateAsyncChannel`'s mask — `type` is immutable, `channel_partitions` is a
  staged re-shard, and the access policy has its own RPC, so none of the three is
  ever presented as editable or sent in a mask. The sidebar's disabled
  "Async Channels" placeholder becomes a real **Channels** link (Clients stays
  disabled for deliverable 16 / 17), and Home gains an Async Channels stat and
  service card. **Access-policy UI is deferred to deliverable 17**:
  `CreateAsyncChannel` is called without `access_policy` (an empty policy is a
  closed, zero-trust channel), and the detail page shows a non-interactive
  placeholder note plus a "0 of N placed" shard note instead of the
  policy / clients / generated-topics panels — shard rows are materialised by
  placement (deliverable 13, ADR-API-009). `useChannels` / `useChannel` /
  `useCreateChannel` / `useUpdateChannel` / `useChannelLifecycle` hooks,
  `CHANNEL_TYPES` + `channelTypeLabel` / `channelStateLabel`,
  `src/pages/channels/*`. New vitest suites for list / register / edit and a
  Playwright smoke (`e2e/channels.spec.ts`). No proto or backend change.
- **Async Channel** (impls_plan deliverable 10): the customer-facing `AsyncChannel`
  entity (003.4) — abstract, no Kafka config of its own. `CreateAsyncChannel`
  records **only the channel row** and its `channel_partitions` count; the shard
  `kafka_topic` rows are materialised by **placement**, not at create
  (ADR-API-009). CRUD + `PauseAsyncChannel` / `ResumeAsyncChannel` (cascade to
  any shards) + `DeleteAsyncChannel` (cascade); `UpdateAsyncChannel` masks
  `labels` only. Embedded **access-policy document** (003.5) — `Effect` /
  `Principal` / `Permission` / `Statement` — validated on write (`effect`
  set, `permissions` non-empty, `principal` has a criterion), replaced wholesale
  via `SetAccessPolicy`. `ListChannelClients` returns `UNIMPLEMENTED` (the
  evaluation engine + the two client-access views ship with deliverable 15).
  `pkg/franz/core/domain/{accesspolicy,channel}`, `usecases/channels`,
  `adapters/{out/postgres/channel,in/grpcgateway/asyncchannel}`. `async_channel`
  table extended in `V1__init.sql`. No proto change.
- **Kafka Topic** (impls_plan deliverable 09): the `KafkaTopic` entity — one
  shard of an Async Channel, tracking reconciliation with the real topic. Franz
  owns every field; the only client mutation is **`SetConsumption`**, which
  drains (`DISABLED` → `traffic_share` 0) or restores a shard and re-normalises
  the owning channel's shares to an equal `percent` split across the `ENABLED`
  ones. `GetKafkaTopic` / `ListKafkaTopics` (filter by `async_channel` /
  `kafka_cluster`, paginated) — no Create / Update / Delete RPC. The config
  merge (`cluster_configuration ⊕ topic_configuration`) is materialised and
  **frozen** at create time — a later cluster-config edit does not touch
  existing shards. The `partitions`-increase-only and immutability invariants
  are enforced in the domain, ready for governance / re-shard. The
  `DeleteKafkaCluster` guard now counts real `kafka_topic` rows
  (`stub.NoTopicGuard` retired from the wiring). `pkg/franz/core/domain/topic`,
  `usecases/topics`, `adapters/{out/postgres/topic,in/grpcgateway/kafkatopic}`.
  `V1__init.sql` gains `kafka_topic` and a minimal `async_channel` stub
  (extended by deliverable 10). No proto change.
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
    (Postgres + a `seed` one-shot + **pgAdmin** at `http://localhost:5050`, with
    the Franz DB pre-registered) and `seed/*.sql`. `make deps` now applies the
    schema and seeds the `local-kafka-agent` registration (with its provisioning
    schema and a fixed public dev token) **before Franz starts**, so `make agent`
    connects with no console step. Replaces the `FRANZ_REGISTER=1` self-register
    path from deliverable 07 — `pkg/localkafkaagent/register.go` and the `Register`
    config field are removed; the agent takes `FRANZ_TOKEN` only.

- **local-kafka-docker-agent** (impls_plan deliverable 07): `cmd/localkafkaagent`
  — the first Cluster Provider agent. It registers with Franz, watches
  `WatchClusterAssignments` (reconnect + backoff, debounced into one reconcile),
  renders the `local-docker` recipe (a single `apache/kafka` KRaft container per
  cluster, `advertised.listeners` from the declared bootstrap URL,
  allow-listed `cluster_configuration` → broker env, `franz.recipe-hash` label),
  drives Docker via the Engine API SDK, and converges: create → recreate on hash
  change (keeping the data volume) → stop on `PAUSED` → remove + volume on
  `REMOVED` → drop orphans. Readiness is a `franz-go` `Ping`; a fresh broker is
  retried so a normal boot goes `PROVISIONING → READY` without a transient
  `DEGRADED`. Status is reported per state transition. `pkg/localkafkaagent/{assign,
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

- **Cluster & agent configuration model** (impls_plan deliverable 11,
  **ADR-API-010**, supersedes ADR-API-008):
  - `KafkaCluster.cluster_configuration` stays a `map<string,string>` — the single
    home for a cluster's Kafka config (topic-config defaults + `partitions` /
    `replication-factor` + `kafka-version`, Franz-friendly keys).
  - `KafkaCluster` gains typed `brokers` (int32) + `disk_size` (string), carried
    on `Create` / `Update` (mask paths) and the provider assignment.
  - **Removed** `Agent.provisioning_labels` / `ProvisioningLabelSpec` (proto
    message + fields, the `agent.provisioning_labels` jsonb column,
    `ValidateProvisioningLabels`, the console's `ProvisioningLabelEditor` /
    `ProvisioningFields`). Agents advertise console defaults as plain
    `franz.default-kafka-config/*` labels instead — **never enforced**.
  - `franz.provisioning/*` cluster labels retired; `kafka-image` and
    `deployment-type` dropped (image is the agent's choice; the agent *is* the
    recipe family).
  - `agent_cluster_provider.proto` `ClusterAssignment`: `provisioning` map removed
    (`reserved 6`), typed `brokers` / `disk_size` added. The `local-docker` recipe
    reads `kafka-version` from `cluster_configuration` and translates
    Franz-friendly keys (`partitions` → `num.partitions`, …) to broker config.
  - Console: a **Cluster configuration** form section (pre-filled `key = value`
    textarea + Kafka-version select + brokers / disk-size inputs) and a plain
    **Labels** section; the "Provisioning intent" section and the
    provisioning-label schema editor are gone.
  - Console e2e (`webconsole/e2e/console.spec.ts`) follows the reworked forms: it
    drives the generic `LabelEditor` to advertise
    `franz.default-kafka-config/partitions` on a Cluster Provider agent and
    asserts the Kafka Cluster form pre-fills `cluster_configuration` from it. The
    `deployment-type` / `kafka-image` / provisioning-schema steps are gone.
  - **Fix:** `LabelEditor` now commits a key/value pair that was typed but not
    "Add label"-ed when focus leaves the widget (e.g. straight to Save /
    Register). Previously that pair was silently dropped, so a label added on the
    Kafka Cluster / Agent form without pressing "Add label" was not saved.
- Renamed the agent's Go paths for consistency: `cmd/local-kafka-agent` →
  `cmd/localkafkaagent`, `pkg/localkafka` → `pkg/localkafkaagent` (import path
  and package clause follow). The registered agent **name** `local-kafka-agent`
  (FRN, `FRANZ_AGENT_NAME` default, `franz.role` label) is unchanged.
- `config`: added `db.auto_migrate` (default `true`) and `resource_prefix`
  (default `frn`; an invalid value fails the boot).
- Proto: the resource-identifier field is `frn` (was `orn`); likewise
  `client_frn` / `resource_frn` / `cluster_frn`. `pkg/gen/go` regenerated.
- `grpcgateway.New` takes functional options; `WithAuthenticator` installs the
  realm interceptors.
- Repository reset from the Clojure implementation to the Go monorepo on the
  `go-monorepo` branch.
