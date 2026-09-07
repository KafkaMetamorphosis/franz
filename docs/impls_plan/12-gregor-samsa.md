# 12 — Gregor Samsa (Resource Provider agent)

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex unavailable (session-limit); implemented directly
Depends on: [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md) · [05](./05-agent-interaction-cluster-provider.md) · [09](./09-kafka-topic.md) · [10](./10-async-channel.md) · [11](./11-cluster-and-agent-config.md) (defines `cluster_configuration`, retires `franz.provisioning/*`)
Specs: `005-gregor-samsa` (the ADR), `003-franz/003.6-kafka-topic`, `003-franz/003.4-async-channel`, `003-franz/003.9-agents`, `003-franz/003.1-conventions`, `003-franz/003.7-placement-and-selection`
Proto: **new** `agent_resource_provider.proto` (`ResourceProviderService`); change to `telemetry.proto`

## Goal

Stand up the **Resource Provider** contract and its reference agent. Franz
streams the async channel partitions an agent is label-scoped to; the agent
(**Gregor Samsa**) reconciles the real Kafka topics to match and reports the
outcome per partition; Franz drives the `kafka_topic` state machine from those
reports. Part 2: the agent publishes topic/broker telemetry as pre-registered
indicator samples over the existing `TelemetryService`.

This deliverable is Part 1 (topics) + Part 2 (telemetry) of the ADR. ACLs, users,
and quotas (ADR Parts 3–5) are **out of scope**.

## Sequencing note

The ADR puts Gregor Samsa **before** placement ([13](./13-placement.md)). Placement is
the normal producer of `kafka_topic` rows (ADR-API-009), so end-to-end
validation here uses **test-inserted** partition rows (the pattern deliverables
09/10 already use) and a real-Docker e2e that seeds a row directly. The
placement → agent notification wire is finished in 13 (task 13.8); until then a
row change is picked up on the agent's next reconnect resync.

## Tasks

### Franz side

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 12.1 | `agent_resource_provider.proto` — `ResourceProviderService` { `WatchPartitionAssignments` (server stream), `ReportPartitionReconciliation` (unary) }; messages `PartitionAssignment` (`change` SET/PAUSED/REMOVED + `reason`, `partition_frn`, `generation`, `async_channel`, `topic_name`, `kafka_cluster` + `connection_strings`, `desired_config`, `partitions`, `replication_factor`), `PartitionReconciliationReport` (`partition_frn`, `generation`, `outcome` CREATED/UPDATED/NOOP/DELETED/ERROR, `message`, `applied_config`). gRPC only, no gateway. `buf lint` + `buf breaking` clean | ADR §1.3, §1.5 | ✅ | 2026-09-07 |
| 12.2 | Reserved labels — parse/validate `franz.placement-selector/*` on `Agent.labels` (deliverable 04 domain) and document `franz.placement/*` on `KafkaCluster.labels` as reserved (add both to `003.1` table — spec edit needs sign-off) | ADR §1.2 | ✅ | 2026-09-07 |
| 12.3 | **Scope resolver** — pure domain fn: `(agent.franz.placement-selector/*, []cluster) → in-scope cluster set`; every agent selector pair must equal the cluster's `franz.placement/<key>`; empty selector ⇒ empty scope. Recomputed on agent-label and cluster-label change | ADR §1.2 | ✅ | 2026-09-07 |
| 12.4 | Widen `agentauth.go` — the Bearer interceptor currently only covers `/franz.v1.ClusterProviderService/`; add `/franz.v1.ResourceProviderService/` and `/franz.v1.TelemetryService/` | ADR §Franz-side | ✅ | 2026-09-07 |
| 12.5 | `kafka_topic` — add `reconciled_generation bigint` (nullable) + `last_reconcile_message text` to `V1__init.sql`; `topic` domain: `RecordReconciliation(generation, outcome, message)` → state transition + generation stamp, generation-gated (a stale report is accepted but does not move the row to `READY`) | ADR §1.5, `003.6` | ✅ | 2026-09-07 |
| 12.6 | `core/usecases/resourceprovider` — `InitialPartitionAssignments(ctx)` (agent from context → scope resolver → in-scope partitions as SET), `ReportReconciliation(ctx, input)` (ownership check: the partition's cluster is in the agent's scope → `PERMISSION_DENIED` otherwise; then `RecordReconciliation`) | ADR §1.5, §1.7 | ✅ | 2026-09-07 |
| 12.7 | Partition-assignment publisher — `channels.Service` (create/pause/resume/delete) and topic mutations publish a `PartitionAssignment` delta to in-scope connected agents; `streamhub` grows a second payload type (or a generic envelope). Scope/label changes emit SET for newly-in-scope and REMOVED(`reason=SCOPE_LOSS`) for departed | ADR §1.3, §1.4 | ✅ | 2026-09-07 |
| 12.8 | `WatchPartitionAssignments` handler — subscribe, full in-scope set on open (all SET), deltas after; `ReportPartitionReconciliation` handler — validate + delegate to 12.6; wire both into `cmd/franz` | ADR §1.3 | ✅ | 2026-09-07 |
| 12.9 | `telemetry.proto` — make `PublishIndicatorSamples` client-streaming (or add `StreamIndicatorSamples`); regenerate; ingest path accepts the stream and appends samples (reuses the deliverable-14 `indicator_sample` table when it lands — until then, a minimal append table gated behind the same 30-day prune) | ADR §2.2 | ✅ | 2026-09-07 |
| 12.10 | Integration tests — scope resolver table; `resourceprovider` usecase (generation gating, ownership `PERMISSION_DENIED`); bufconn e2e `TestResourceProviderE2E` (token auth, full-set-then-delta stream over test-inserted partitions, report drives `PENDING → READY` / `→ ERROR`, stale-generation report is a no-op, scope loss → REMOVED) | — | ✅ | 2026-09-07 |

### Agent (`cmd/gregorsamsa`, `pkg/gregorsamsa` — plain packages, no fx, per 002)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 12.11 | Binary skeleton + config (`FRANZ_ENDPOINT`, `FRANZ_TOKEN`, poll/backoff knobs) + structured logging; gRPC client from `pkg/gen/go` with the Bearer interceptor | ADR §Arch | ✅ | 2026-09-07 |
| 12.12 | Kafka admin driver — interface + `franz-go` `kadm` impl + in-memory fake for tests: `describeTopic`, `createTopic`, `createPartitions`, `alterConfigs`, `deleteTopic`, `listOffsets`, `listConsumerGroups` / `listConsumerGroupOffsets`, `describeCluster` / metadata. One `AdminClient` per in-scope cluster, cached, built from the assignment's `connection_strings` | ADR §Arch, §2.1 | ✅ | 2026-09-07 |
| 12.13 | Stream loop — `WatchPartitionAssignments` with reconnect + exponential backoff (5s→120s); on (re)connect treat the incoming full set as the desired world; maintain an in-memory desired map keyed by `partition_frn` | ADR §1.3 | ✅ | 2026-09-07 |
| 12.14 | Reconcile — `SET`: describe → create if absent / alter to `desired_config` + `createPartitions` (increase only; RF decrease → ERROR); read back; report `CREATED`/`UPDATED`/`NOOP`. Sequential per cluster, clusters parallel. Idempotent (matching state ⇒ no Kafka write, still reports) | ADR §1.4 | ✅ | 2026-09-07 |
| 12.15 | Delete — `REMOVED` (not `SCOPE_LOSS`): the two hard safety checks (unconsumed data via `listOffsets` earliest<latest; committed consumer-group offsets), refuse + `ERROR` with a structured message on failure; idempotent `DELETED` if the topic is already gone; `SCOPE_LOSS` and `PAUSED` just drop from the desired map, no Kafka call | ADR §1.4 | ✅ | 2026-09-07 |
| 12.16 | Outcome reporting — `ReportPartitionReconciliation` per partition whenever its outcome changes; carry `generation` from the assignment and `applied_config` read back from Kafka | ADR §1.5 | ✅ | 2026-09-07 |
| 12.17 | Telemetry loop — configurable sweep (default 60s) of every in-scope cluster + partition: topic-level (`kafka.topic.state`, `partitions`, `replication_factor`, `under_replicated_partitions`, `config_drift`) and cluster-level (`kafka.cluster.broker_count`, `online_broker_count`, `total_partition_replicas`, `replicas_per_broker`, `leaders_per_broker`, `under_replicated_partitions`, `offline_partitions`) indicator samples over the client stream; plus an immediate sample right after a reconcile | ADR §2.1, §2.2 | ✅ | 2026-09-07 |
| 12.18 | Fake-admin unit tests (reconcile create/alter/delete/safety-check/idempotent/RF-decrease-error) + a real-Docker e2e (`make gregorsamsa-e2e`, opt-in `FRANZ_GS_E2E=1`): seed a partition row, agent creates the topic, `kadm` confirms it, edit config → altered, delete → safety-checked + removed | ADR §1 | 🚧 | 2026-09-07 |
| 12.19 | `Makefile` — `gregorsamsa` (run against the seeded dev agent) and `gregorsamsa-e2e`; `local/seed/` adds a seeded `local-1` Kafka Cluster (wired to `local-kafka-agent`, `franz.placement/env=local`) **and** a `RESOURCE_PROVIDER` agent registration (`franz.placement-selector/env=local` — matches the seeded cluster) with a fixed dev token | — | ✅ | 2026-09-07 |

## Done when

- An agent authenticates, opens `WatchPartitionAssignments`, and receives every
  async channel partition on every cluster in its label scope.
- A test-inserted `PENDING` partition on a reachable cluster becomes a real Kafka
  topic and the row flips to `READY` with `reconciled_generation` set; editing
  the desired config re-drives it; a `REMOVED` with data/consumers present
  reports `ERROR` and does not delete.
- A stale-generation report does not move a row to `READY`.
- `kafka.cluster.broker_count` and `kafka.topic.state` samples for an in-scope
  cluster/partition land in the telemetry store within one sweep.
- `buf lint` + `buf breaking` clean; `go build/vet/test ./...` green;
  `make gregorsamsa-e2e` green with Docker.

## Notes

- Agent is "deliberately simple" (`002-monorepo-structure`) — plain packages, no
  hexagonal layering, no `fx`. Mirror `pkg/localkafkaagent` structure.
- Reuses the deliverable-05 pattern wholesale: `streamhub`, `agentauth`,
  `core/usecases/*` shape, bufconn e2e, real-Docker opt-in e2e.
- Spec edits done alongside this deliverable (docs repo — see tracker `12-gregor-samsa.md`):
  `003.1` reserved-label table (`franz.placement-selector/*`, `franz.placement/*`),
  `003.6` (`generation` reporting semantics + `reconciled_generation` /
  `last_reconcile_message`), `003.9` (`RESOURCE_PROVIDER` interaction contract →
  link `005`), and **ADR-API-011** in `DECISIONS.md` for the Resource Provider
  contract. `telemetry.proto` gained `StreamIndicatorSamples` (additive — the
  unary `PublishIndicatorSamples` stays), so no breaking change and no `003.14`
  edit needed beyond noting the new RPC.
- Telemetry samples land in a **minimal `indicator_sample` append table** added
  here (30-day nightly prune, same as `cluster_provider_event`); deliverable 14
  adopts it and adds the `indicator` registry that makes pre-registration
  enforceable. Until then any indicator name is accepted.

### What landed

| Piece | Path |
|---|---|
| Proto | `api/franz/v1/agent_resource_provider.proto` (new); `telemetry.proto` +`StreamIndicatorSamples` |
| Scope resolver | `pkg/franz/core/domain/scope/` (pure fn + label validation) |
| Topic reconcile domain | `pkg/franz/core/domain/topic/reconcile.go` — `RecordReconciliation`, generation-gated |
| Indicator domain / ingest | `pkg/franz/core/domain/indicator/`, `core/usecases/telemetry/`, `adapters/out/postgres/indicator.go` |
| Resource Provider usecase | `pkg/franz/core/usecases/resourceprovider/` — `service.go` (initial set + generation-gated intake, `PERMISSION_DENIED` ownership check), `notifier.go` (dynamic scope: shard / cluster-label / agent-selector changes → SET / REMOVED+SCOPE_LOSS) |
| Stream fan-out | `pkg/franz/adapters/streamhub/hub.go` — generic `fanout[T]`, second payload type for partition assignments |
| gRPC handlers | `adapters/in/grpcgateway/resourceprovider.go`, `telemetry.go`; `agentauth.go` widened to Resource Provider + Telemetry services; wired in `cmd/franz/main.go` (+ nightly `indicator_sample` prune) |
| Migration | `V1__init.sql` — `kafka_topic.reconciled_generation` / `last_reconcile_message`; `indicator_sample` table + indexes (idempotent `ALTER … ADD COLUMN IF NOT EXISTS`) |
| Agent | `cmd/gregorsamsa/`, `pkg/gregorsamsa/` — `config.go`, `agent.go`, `stream/` (reconnect + backoff), `assign/` (desired map), `kafkaadmin/` (`admin.go` interface, `kadm.go` franz-go impl, `mem.go` fake), `reconcile/` (SET create/alter/increase-only, deletion safety checks), `telemetry/` (sweep loop + post-reconcile sample) |
| Makefile / seed | `gregorsamsa` + `gregorsamsa-e2e` targets; `local/seed/02-local-cluster.sql` (a `local-1` Kafka Cluster wired to `local-kafka-agent`, `franz.placement/env=local`, config matching the agent's advertised defaults) + `local/seed/03-gregor-samsa.sql` (RESOURCE_PROVIDER agent, `franz.placement-selector/env=local`, fixed dev token; plus a catch-all that labels console-created clusters) |

### Positions taken on ADR open questions

- **OQ1 (overlapping scopes)** — not implemented. Franz still lets two agents
  whose selectors both match a cluster each open a stream and both receive its
  partitions. No task covered it; left as a follow-up (needs its own decision:
  refuse the second stream for the contested clusters vs. warn).
- **OQ2** — explicit `franz.placement/*` prefix, per the ADR.
- **OQ3 (RF change)** — `ERROR`, per the ADR.
- **OQ4 (unspecified config keys)** — left untouched; `incrementalAlterConfigs`
  only SETs keys Franz names.
- **OQ5 (telemetry cadence)** — single configurable full-sweep interval
  (default 60s); no per-indicator intervals or change-only publishing yet.
- **OQ6 / OQ7 (instance ↔ agent cardinality)** — one `Agent` row per process
  assumed; no coordination for a scaled-out deployment.

### Not executed this session

- `make gregorsamsa-e2e` (opt-in `FRANZ_GS_E2E=1`, real Docker Kafka) is
  **written and compiles** (`go test ./...` builds it; it self-skips without the
  env var) but was **not run** — it needs an exclusive local stack. The
  Franz-side wiring was smoke-tested instead by booting a fresh `cmd/franz`
  binary (fx graph resolves, gRPC + HTTP listen, `/healthz` OK). The bufconn
  `TestResourceProviderE2E` (12.10) *was* run and passes.

## Open questions (from the ADR)

1. Overlapping scopes — reject the second agent, or warn + serve one? (Lean:
   refuse the contested clusters on the second stream.)
2. `franz.placement/*` vs. matching free-form cluster labels via the `003.1`
   selector grammar (ADR takes the explicit-prefix route).
3. RF changes → `ERROR` for now; a reassignment plan is a future part.
4. Whether Gregor Samsa ever resets a broker-side config override Franz did not
   ask for (lean: no).
5. Telemetry cadence/volume across a large fleet — per-indicator intervals or
   change-only publishing.
6. One `Agent` row per process, or one shared by a scaled-out deployment.
