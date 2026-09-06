# 05 — Agent interaction (Cluster Provider)

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex unavailable (usage limit, resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md) · [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md)
Specs: `004-local-kafka-docker-agent` (the ADR), `003-franz/003.9-agents`, `003-franz/003.3-kafka-cluster`
Proto: **new** `agent_cluster_provider.proto` (`ClusterProviderService`); changes to `agent.proto`

## Goal

The Franz side of the Cluster Provider contract: agent auth, a streaming feed of
the clusters an agent owns, a status-report intake, and the persistence + read
surface for that reported status.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 05.1 | Token in `CreateAgentResponse`, `RotateAgentToken`, `agent.token_hash` — **delivered by deliverable 04** (`pkg/shared/token`: `frnat_` + 32 random bytes, SHA-256 hash) | ADR §2 | ✅ | 2026-09-05 |
| 05.2 | Agent-auth interceptor — `authorization: Bearer` → agent identity in context; unknown / rotated / deleted-agent tokens → `UNAUTHENTICATED`; scoped to `/franz.v1.ClusterProviderService/*` | ADR §2 | ✅ | 2026-09-06 |
| 05.3 | `agent_cluster_provider.proto` (`ClusterProviderService`), `common.ClusterProviderPhase`, `kafka.{ClusterProviderStatus,ClusterProviderEvent,ListClusterProviderEvents}` — **already written** in the earlier proto pass; buf lint clean | ADR §New proto | ✅ | 2026-09-05 |
| 05.4 | Connected-agent registry (`pkg/franz/adapters/streamhub`) — per-agent subscriptions, fan-out, lagging-subscriber drop | ADR §1 | ✅ | 2026-09-06 |
| 05.5 | `WatchClusterAssignments` handler — subscribe, send full current set on open, then stream deltas; `clusters.Service` publishes on create/update/pause/resume/delete (+ REMOVED to a previous owner on re-assignment) | ADR §1 | ✅ | 2026-09-06 |
| 05.6 | `cluster_provider_event` append table + `ProviderEventRepo` (append, list-newest-first w/ opaque cursor, `LatestStatus` projection, `PruneOlderThan`); nightly 30-day prune goroutine in `cmd/franz` | ADR §4 | ✅ | 2026-09-06 |
| 05.7 | `ReportClusterStatus` handler — ownership check (`PERMISSION_DENIED`), phase validation, append event | ADR §4 | ✅ | 2026-09-06 |
| 05.8 | `KafkaCluster.provider_status` populated on `GetKafkaCluster`; `ListClusterProviderEvents` RPC implemented | ADR §4 | ✅ | 2026-09-06 |
| 05.9 | Integration tests — `ProviderEventRepo` (postgres) + a bufconn end-to-end (`TestClusterProviderE2E`): token auth, full-set-then-delta stream, `franz.provisioning/*` edit delta, status → `GetKafkaCluster`, history, ownership `PERMISSION_DENIED`, rotation invalidates old token, missing token `UNAUTHENTICATED` | — | ✅ | 2026-09-06 |

## Done when

- An agent authenticates with its registration token, opens the stream, receives
  its clusters, and a `ReportClusterStatus` shows as the cluster's current
  provider status via `GetKafkaCluster`.
- Editing a `franz.provisioning/*` label pushes a `SET` delta on the open stream.
- Rotating the token drops the old one.

## Notes

- `AgentStatus` stays `ACTIVE/PAUSED/DELETED`; the stream registry is not wired
  into it (ADR OQ4 — a derived "connected" flag is a later call).
- The interceptor here is agent-only; the console/API allow-all stub (02.10) is
  separate.

### What landed

| Concern | Package |
|---|---|
| Provider domain (Phase / Status / Event / Assignment / provisioning-label filter) | `pkg/franz/core/domain/provider` (leaf — `cluster.Cluster` carries `*provider.Status`, `cluster.ToAssignment()` maps) |
| Agent context helpers | `pkg/franz/core/domain/agent/context.go` |
| Agent-auth interceptor | `pkg/franz/adapters/in/grpcgateway/agentauth.go` (`WithAgentAuth` option) |
| Connected-agent stream hub | `pkg/franz/adapters/streamhub` |
| Provider usecase | `pkg/franz/core/usecases/provider` (InitialAssignments, ReportStatus, ListEvents) |
| `clusters.Service` publish-on-mutate | `pkg/franz/core/usecases/clusters/service.go` (`out.AssignmentPublisher` + `out.ProviderStatusReader` deps) |
| ClusterProviderService handler (gRPC only) | `pkg/franz/adapters/in/grpcgateway/clusterprovider.go` |
| `cluster_provider_event` repo | `pkg/franz/adapters/out/postgres/provider.go` |
| Migration | `migrations/V1__init.sql` — `cluster_provider_event` |
| Nightly prune | `cmd/franz/main.go` (`startProviderEventPrune`, 30-day window) |
| DB-test serialisation helper | `pkg/internal/dbtest` |

### Assumptions (revisit if wrong)

1. **Initial assignment set includes DELETED clusters as `CHANGE_REMOVED`** so an
   agent reconnecting after a delete tears the substrate down. A long-deleted
   cluster is re-sent on every connect (idempotent for the agent). Rough edge:
   prune acknowledged REMOVEDs from the initial set later.
2. **Lagging subscriber → channel closed → stream ends with `ABORTED`** ("lagged;
   reconnect"). The agent reconnects and full-resyncs (level-triggered, ADR §6).
3. **Agent-auth interceptor is method-prefixed** (`/franz.v1.ClusterProviderService/`).
   Every other RPC still gets the allow-all realm interceptor only.
4. **The base realm interceptor still runs on ClusterProvider calls** and sets the
   seeded default realm; the provider usecase ignores context-realm for
   agent-facing methods and scopes by `agent.RealmID` instead.
5. **`ListClusterProviderEvents.page.total_size` = 0** (best-effort).
6. **`cluster_provider_event` retention = 30 days** (shares the 003.14 default;
   ADR OQ6). Prune runs every 24h from boot; not distributed-lock-guarded (fine
   for a single Franz instance).
7. Token prefix is `frnat_` / SHA-256 (from deliverable 04), not the ADR's
   illustrative `frz_agt_` / argon2.
