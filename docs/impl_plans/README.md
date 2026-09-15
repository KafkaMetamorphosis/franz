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

## Conventions for every new deliverable

Two things go missing if they aren't scoped up front, because nothing forces
them to exist later — 14 and 15 both shipped without either, and the gap
wasn't caught until someone asked "can I see this in the UI?" (see
[20](./20-governance-ui.md)'s Notes):

1. **Console UI.** If the deliverable adds anything an operator would
   otherwise only reach through a raw gRPC/REST call, its task list either
   builds the screens or explicitly names the follow-up deliverable that will
   (the way 19 and 20 do for 10 and 14/15). A backend-only deliverable with no
   UI plan at all — not even a forward reference — is a planning gap, not a
   deferral.
2. **A local seed.** If the deliverable's feature needs data to exist before
   it does anything visible (a registered indicator, a labeled cluster, a
   linked agent), its task list adds or extends a `local/seed/*.sql` file so
   `make dev` demonstrates the feature with no hand-authored API calls first.
   Enforcement gaps count too — deliverable 15 made indicator pre-registration
   real and broke Gregor Samsa's publish path until `04-indicators.sql`
   backfilled it; a deliverable that *tightens* a precondition needs the seed
   at least as much as one that adds a new resource.

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
| [08](./08-resource-management-ui.md) | Resource management & agent provisioning schema | 03 · 04 · 06 · 07 | ✅ |

## Rest of the control plane

| # | Deliverable | Depends on | Status |
|---|---|---|---|
| [09](./09-kafka-topic.md) | Kafka Topic (read model) | 02 · 03 | ✅ |
| [10](./10-async-channel.md) | Async Channel + access-policy document | 02 · 09 | ✅ |
| [11](./11-cluster-and-agent-config.md) | Cluster & agent configuration model | 03 · 04 · 08 | ✅ |
| [12](./12-gregor-samsa.md) | Gregor Samsa (Resource Provider agent) | 03 · 04 · 05 · 09 · 10 · 11 | ✅ |
| [13](./13-placement.md) | Placement & selection | 03 · 10 · 11 · 12 | ✅ |
| [14](./14-governance.md) | Governance (Indicator registry + non-placement actions) | 02 · 03 · 09 · 10 | ✅ |
| [15](./15-telemetry-ingest.md) | Telemetry ingest | 02 · 14 | ✅ |
| [16](./16-client.md) | Client | 02 · 15 | ✅ |
| [17](./17-access-policy-and-channel-access.md) | Access-policy engine & channel-access views | 02 · 10 · 16 | ✅ |
| [18](./18-migration-and-data-movement.md) | Migration & data movement | 09 · 10 · 13 | ✅ |
| [19](./19-async-channel-ui.md) | Async Channel UI (console screens for 10) | 06 · 08 · 10 · 11 | ✅ |
| [20](./20-governance-ui.md) | Governance UI (console screens for 14 · 15) | 06 · 08 · 14 · 15 | ✅ |
| [21](./21-client-ui.md) | Client UI (console screens for 16) | 06 · 08 · 16 | ✅ |
| [22](./22-migration-ui.md) | Migration UI (console screens for 18) | 06 · 08 · 18 · 19 | ✅ |

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
| Real API authorization | `003.2` model undecided (stub for now) |
| Control-plane event log | `003.11` OQ4 — design not started |
| SDK / client library (shard routing) | routing-key ADR not written |
| Misplaced-shard auto-relocate; a `channel_partitions` **decrease** (re-shard down) | `18`'s migration flow exists but nothing yet drives it automatically for either case — see [18](./18-migration-and-data-movement.md)'s Notes |

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

- 2026-09-14 · **20** Governance UI · console screens for 14/15's Indicator
  registry and Policy engine — `/governance/indicators` and
  `/governance/policies`, list/register/detail/edit for both, following the
  06/08/19 page shape. `PolicyRegister`/`PolicyEdit` share a new
  `ActionEditor` component (kind-dependent arg shape, an optional
  `max=`/`min=` cap on the two arithmetic kinds, whitelist-violation errors
  rendered inline on the offending action row via `actions[N]...` field-path
  parsing) and `PolicyDetail` gets a Dry-run panel and the `PolicyAction`
  audit trail. Two design-doc inaccuracies were corrected against the real
  wire contract while building rather than assumed: `Indicator` has no
  `current_value`/FRN field (only `health`/`last_sample_at`), and
  `DryRunPolicy` has no resource/hypothetical-value input — it evaluates a
  full definition against the latest real sample per matched resource. A live
  smoke test against the real gateway caught a genuine bug before it shipped:
  `SET_STATUS`'s argument is the plain state name (`"PAUSED"`), not the
  proto-prefixed form (`"CHANNEL_STATE_PAUSED"`) — `ActionEditor` renders a
  real `<select>` for it instead of free text, so the mistake isn't reachable
  from the form. No backend change — pure console work against 14's already-
  shipped `GovernanceService`. 6 new vitest files (18 cases) +
  `e2e/governance.spec.ts`; `typecheck`/`lint`/`test`/`build` green. Executed
  by claude (claude-sonnet-5).
- 2026-09-14 · **18** Migration & data movement · the last blocker in the
  plan, and the one deliverable whose original task list turned out to
  describe the wrong mechanism. It started from "a shard migration flips
  `kafka_topic.kafka_cluster_id` through a staged state machine"; several
  `AskUserQuestion` rounds resolved 003.13's OQs (explicit operator RPC,
  drain-based only, fixed 1h deadline, `force=true` cluster-delete,
  per-cluster configurable concurrency limit) before the user corrected the
  premise directly: an Async Channel already spreads load over N Kafka
  Topics by `traffic_share`, and a topic's consumer is already
  ENABLED/DISABLED (deliverable 09) — migration is orchestration of those
  existing primitives, not a new one. The shipped design: (1) create an
  ordinary new shard on the target cluster through the existing placement
  path; (2) cut over via the existing `SetConsumption(source, DISABLED)`,
  which already re-normalises `traffic_share`; (3) once two new Gregor Samsa
  indicators (`kafka.topic.drained`, `kafka.topic.consumer_connected` — built
  from existing `kafkaadmin.Admin` methods, no new agent RPC) confirm the
  source is idle, retire it through the **normal topic-delete path**, which
  the agent already reacts to. `shard_migration` is bookkeeping + a sweep
  driver, not a parallel state machine. Also shipped: `KafkaCluster.
  MaxConcurrentMigrations`, `DeleteKafkaClusterRequest.force`, a drain-taint
  auto-trigger on `clusters.Service.Update`, increase-only re-shard
  (`AsyncChannel.SetChannelPartitions`), and — per a follow-up
  `AskUserQuestion` on how far to take 18.4/18.8 — governance-action
  un-deferral (`franz.affinity/*` / `franz.antiaffinity/*` / `franz.taint`
  labels and a `channel_partitions` increase now reach the flow through a
  policy the same way an operator or the drain-taint trigger does).
  **Deliberately deferred**: misplaced-shard auto-relocate, and a
  `channel_partitions` *decrease* (either path needs the same
  drain-then-retire sequence run per removed shard, on nothing's
  initiative yet). Found and fixed a real pre-existing bug along the way:
  `persistChannel`'s UPDATE never wrote `channel_partitions` (harmless while
  the field was immutable; live once 18.7 made it maskable). codex never ran
  (quota, resets 2026-09-28) — claude implemented it directly. `go
  build/vet/test` (41 packages, Postgres integration active) green under
  `-p 1`; a `go test ./...` without `-p 1` intermittently fails several
  `grpcgateway` tests from cross-package concurrent access to the same live
  Postgres instance — confirmed pre-existing (not a regression) by isolated
  reruns; `-p 1` is the reliable way to verify this suite. `buf lint` clean.
- 2026-09-13 · **17** Access-policy engine & channel-access views ·
  `core/domain/accesspolicy/evaluate.go` — `Evaluator` (compiles a policy's
  label selectors once, then evaluates many clients cheaply), `Evaluate`
  resolving `003.5`'s algorithm (DENY always wins regardless of document
  order; `client_frn` glob matches the prefix-less stored FRN, never a
  rendered one; both views return only rows with ≥1 effective permission).
  `AsyncChannelService.ListChannelClients` (forward) and
  `ClientService.ListClientChannelAccess` (reverse) both ship, each fetching
  one page of the underlying resource and filtering in Go — no bound, mirrors
  `ClusterRepo.List`'s existing selector pattern. Four design points (matched_by
  semantics, client_frn prefix form, row inclusion, statement-cap deferral)
  were resolved via `AskUserQuestion` rather than assumed, per explicit user
  instruction — see the tracker for the Q&A.
  `local/seed/06-access-policy-demo.sql` gives the two seeded clients
  asymmetric access to a demo channel; `docs/impl_plans/21-client-ui.md`'s
  "Channel access" panel un-deferred now that the RPC exists. Executed by
  claude (codex out of quota).
- 2026-09-13 · **16** Client · `core/domain/client` (no Type/Role/Status field —
  003.10 is explicit), full `ClientService` CRUD (gRPC+REST), and the two
  observed-consumer-group reads (`ListObservedConsumerGroups`/
  `ListConsumerGroupObservations`), scoped by the client's FRN over
  deliverable 15's `ObservedConsumerGroupRepository`. Deletion is a real row
  removal (Client has no state column to soft-delete into) backed by a new
  `deleted_client_frn` ledger table so a name is never reusable, matching
  every other entity's "name/FRN never freed" rule by a different mechanism.
  `ListClientChannelAccess` stays `Unimplemented` pending 17's access-policy
  engine. Per the new UI/seed convention: `local/seed/05-clients.sql` seeds
  two example clients, and **21 — Client UI** was scoped (not built) since 16
  ships no console screens. Executed by claude (codex out of quota).
- 2026-09-13 · **plan** · inserted deliverable **20 — Governance UI**, scoping
  the console screens 14 (Indicator registry, Policy engine) and 15 (telemetry
  ingest) never got — raised while answering "is it possible to see indicators
  in the UI?" (no). Also added `local/seed/04-indicators.sql`, registering the
  13 structural indicators Gregor Samsa publishes (005 ADR §2.1), since 15's
  pre-registration enforcement means Gregor Samsa's sweep now fails every
  publish against a fresh local database without it.
- 2026-09-13 · **15** Telemetry ingest · the two inbound agent streams become
  real: `PublishIndicatorSamples` / `StreamIndicatorSamples` now enforce
  pre-registration (`FAILED_PRECONDITION` on an unknown indicator, no
  auto-creation), `resource_entity` / `value`-unit validation, atomic batches
  (all-or-nothing — the response carries only a count), current-value /
  `last_sample_at` / derived `health` maintenance, and the synchronous
  ingest → `GovernanceEvaluator.Evaluate` hook (an out-of-order sample stores
  but triggers nothing, a failing evaluation never fails ingest —
  `003.14` OQ4 resolved for the simple option). `ReportConsumerGroups`
  implemented against a new `observed_consumer_group` append table (30-day
  prune); `custom` is derived by Franz from the `<client>.<topic>` convention,
  never accepted from the wire. New categorical unit family (`string`/`enum`)
  for `005` §2.1's `kafka.topic.state` / `...controller_id`. The
  `ListObservedConsumerGroups` / `ListConsumerGroupObservations` **handlers**
  wait on deliverable 16 (`ClientService`); their repository methods ship
  here. codex out of quota → claude (architect agent) implemented it.
- 2026-09-10 · **14** Governance · Indicator registry (`GovernanceService`
  Indicator CRUD, `health` derived / `applies_to` immutable) + Policy engine:
  `core/domain/governance` (Policy / whitelist matrix / per-action caps —
  003.8 OQ1 resolved as an optional `max=` / `min=` third arg clamping the
  resulting value), `usecases/governance` (CRUD, `DryRunPolicy`, the
  event-driven `Evaluator` + `NoopEvaluator`, the non-placement action applier,
  `(weight desc, name asc)` conflict order), pgx `IndicatorRepo` / `PolicyRepo`
  / `PolicyActionRepo`, the `GovernanceService` gRPC+REST handler,
  `indicator` / `policy` / `policy_action` tables, nightly `policy_action`
  prune. Placement actions rejected at write (need 16/18). No proto change.
  `go build/vet/test` (35 pkg, incl. Postgres integration + eval e2e),
  `buf lint`, console typecheck/build — green. Nothing calls the evaluator
  until deliverable 15. codex out of quota → claude.

- 2026-09-07 · **plan** · **swapped deliverables 14 ↔ 15**: Governance is now 14,
  Telemetry ingest is 15. They had a mutual dependency (`14.6` ingest→eval hook
  needs `15.4` eval; `15.2` policy validation needs `14.1` Indicator registry).
  Broke the cycle by moving the **Indicator registry** into Governance (`003.8`
  already lists Indicator CRUD on `GovernanceService`). New order: Governance
  (self-contained, ships the registry + policy engine + a bare evaluation entry
  point) → Telemetry ingest (feeds samples, maintains current value / health,
  calls Governance's evaluation). `Depends on` + cross-refs updated in
  `16-client.md`, `02-domain-foundations.md`, `12-gregor-samsa.md`.

- 2026-09-07 · **13** Placement & selection · pure `domain/placement` selection
  (affinity / anti-affinity / taints → weight+name order → shard-size cap →
  deterministic round-robin); `placement.Service` materialises `kafka_topic`
  rows on channel + cluster changes (ADR-API-009 — this deliverable owns row
  creation), 30s fx retry sweep, `misplaced` marker (column + additive
  `KafkaTopic.misplaced` / `misplaced_reason` proto fields, marker only — no
  move), notifies Gregor Samsa via `Notifier.ShardsChanged`. `Materialize` now
  drops the `partitions` / `replication-factor` seed keys from the merge
  (003.6). Closes 003.7 OQ1 (round-robin remainder) + OQ5 (configurable sweep +
  event triggers). `go build/vet/test` (32 pkg), `buf lint`, console checks —
  green. codex out of quota → claude.
- 2026-09-07 · **19** Async Channel UI · console screens for deliverable 10 —
  `/async-channels` list / register / detail / edit, the `useChannels` /
  `useChannel` / `useCreateChannel` / `useUpdateChannel` / `useChannelLifecycle`
  hooks, `CHANNEL_TYPES` + channel enum labels, the sidebar's Async Channels
  placeholder replaced by a real `Channels` link, and an Async Channels stat +
  service card on Home. Edit is **labels-only** (`type` / `channel_partitions` /
  access policy are not maskable). **Access-policy UI deferred to deliverable
  17** — no policy editor, no `access_policy` on create (empty = closed channel),
  no client-access panel; the detail page carries a placeholder note and a
  "0 of N placed" shard note (placement is 13). No proto change. 7 new vitest
  cases + `e2e/channels.spec.ts`; typecheck / lint / test / build / e2e green.
- 2026-09-07 · **11** Cluster & agent configuration model · **ADR-API-010**
  (supersedes 008). `cluster_configuration` stays a map; `KafkaCluster` +typed
  `brokers` / `disk_size`; `Agent.provisioning_labels` / `ProvisioningLabelSpec`
  removed (proto + column + domain + console editor); agents advertise
  `franz.default-kafka-config/*` label defaults (unenforced); `franz.provisioning/*`
  retired; `kafka-image` / `deployment-type` dropped. `ClusterAssignment.provisioning`
  → `reserved`, typed `brokers` / `disk_size`. Recipe translates Franz-friendly
  config keys. Console: one Cluster-configuration section + plain Labels section.
  `go build/vet/test`, `buf lint`, webconsole `typecheck`/`lint`/`test`/`build`,
  REST smokes — all green. codex out of quota → claude.
- 2026-09-07 · **plan** · deliverable **11 — Cluster & agent configuration
  model** reworked after design review. `cluster_configuration` **stays a
  `map<string,string>`** (not labels); `KafkaCluster` gains typed `brokers` /
  `disk_size`; `kafka-image` + `deployment-type` dropped; `kafka-version` moves
  into the config map; `franz.provisioning/*` retired. Agents advertise console
  defaults as `franz.default-kafka-config/*` labels (unenforced).
  **ADR-API-008's structured `Agent.provisioning_labels` schema is removed**
  (superseded by ADR-API-010). Agent→cluster watch scoping
  (`franz.placement/*` ↔ `franz.placement-selector/*`) moves to deliverable 12.
- 2026-09-06 · **plan** · inserted deliverable 11 ahead of Gregor Samsa;
  renumbered the former 11–17 to **12–18**. All cross-references, `Depends on`,
  and internal task IDs updated. No shipped deliverable (01–10) affected.
- 2026-09-06 · **plan** · added the **Gregor Samsa (Resource Provider agent)**
  deliverable; design `docs/005-gregor-samsa` — multi-cluster,
  `franz.placement-selector/*` ↔ `franz.placement/*` scoping; push-driven
  `WatchPartitionAssignments` server stream + per-partition generation-gated
  reports; deletion safety checks kept; Part 2 telemetry over `TelemetryService`.
- 2026-09-06 · **10** Async Channel + access-policy document ·
  `pkg/franz/core/domain/{accesspolicy,channel}`, `channels.NewService`
  (Create = one `async_channel` row, no shards — **ADR-API-009**; Get / List /
  Update-labels / Delete-Pause-Resume cascade to any shards), postgres
  `ChannelRepo`, `AsyncChannelService` REST handler (`ListChannelClients` →
  `Unimplemented`, ships with 15). `access_policy` validated on write, no cap.
  `async_channel` table extended. No proto change. Plan updated: shard
  materialisation moved to deliverable 13. codex out of quota → claude.
- 2026-09-06 · **09** Kafka Topic (read model) · `pkg/franz/core/domain/topic`
  (state machine + `Consumption` + `TrafficShare` + config `Materialize`),
  `topics.NewService` (Get / List / SetConsumption with equal-split
  re-normalisation), postgres `TopicRepo` (joined reads, one-txn
  `MutateChannelShards`), `KafkaTopicService` REST handler. `kafka_topic` +
  a minimal `async_channel` stub in `V1__init.sql`. `NoTopicGuard` replaced by
  the real postgres count. No proto change. codex out of quota → claude.
- 2026-09-06 · **plan** · split the access-policy work along the validate/evaluate
  seam. The standalone **09 — Access-policy engine** is removed: the policy
  *document* (types + write validation) folds into **10 — Async Channel**; the
  *engine* (principal matching, evaluation, and the `ListChannelClients` /
  `ListClientChannelAccess` views) becomes a new **15 — Access-policy engine &
  channel-access views**, sequenced after Client — both views iterate Clients so
  the engine had no exercisable consumer before then. Former 10–15 shift to
  09–14; migration stays 16. Cross-references + `Depends on` updated.
- 2026-09-06 · **08** Resource management & agent provisioning schema ·
  `Agent.provisioning_labels` (advisory schema, ADR-API-008) through proto →
  deliverable-04 backend → console. Console edit pages for Agent + Kafka Cluster
  (`/…/edit`), schema-driven provisioning fields on the cluster forms,
  re-assignment confirm gate, 409 reload-and-re-apply. `local-docker` recipe
  gains `franz.provisioning/kafka-image`. **`franz/local/`** added — a
  docker-compose + DB seed that installs the `local-kafka-agent` registration
  (schema + fixed dev token) before Franz starts, replacing the `FRANZ_REGISTER`
  self-register path from 07 (`pkg/localkafkaagent/register.go` removed). codex out of
  quota → claude implemented it all.
- 2026-09-06 · **plan** · inserted deliverable **08 — resource management in the
  console**; renumbered the former 08–15 to **09–16**. All cross-references and
  `Depends on` columns updated. No code or shipped deliverable (01–07) affected.
- 2026-09-06 · **07** local-kafka-docker-agent · `cmd/localkafkaagent` +
  `pkg/localkafkaagent/{assign,stream,recipe,docker,reconcile,probe}` — connects as a
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
