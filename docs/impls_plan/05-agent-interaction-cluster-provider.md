# 05 — Agent interaction (Cluster Provider)

Status: ⬜ not started
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
| 05.1 | `agent.proto`: `token` (string) in `CreateAgentResponse`; new `RotateAgentToken` RPC + messages. `agent` table gains `token_hash text`. Token format `frz_agt_<random>`, stored hashed (argon2/bcrypt or SHA-256+salt) | ADR §2 | ⬜ | |
| 05.2 | Agent-auth interceptor — resolve `authorization: Bearer` metadata → agent identity in context; reject unknown / rotated tokens `UNAUTHENTICATED` | ADR §2 | ⬜ | |
| 05.3 | `agent_cluster_provider.proto` — `ClusterProviderService.WatchClusterAssignments` (server-stream of `WatchClusterAssignmentsResponse` → `ClusterAssignment`) + `ReportClusterStatus` (unary); `ClusterAssignment` = cluster_name / cluster_orn, `change` enum (`CHANGE_SET`/`CHANGE_PAUSED`/`CHANGE_REMOVED`), connection_strings, cluster_configuration, `franz.provisioning/*` labels; buf lint clean | ADR §New proto | ⬜ | |
| 05.4 | Connected-agent registry (in-memory) — track open streams per agent; fan out assignment changes | ADR §1 | ⬜ | |
| 05.5 | `WatchClusterAssignments` handler — on open, send the full current set for `cluster_provider_agent == me` (all `CHANGE_SET`); then push deltas on cluster create/update/pause/resume/delete affecting that agent | ADR §1 | ⬜ | |
| 05.6 | `cluster_provider_event` append table (`cluster_orn`, `phase`, `reachable`, `message`, `reporting_agent`, `recipe_ref`, `occurred_at`); nightly prune (30d) | ADR §4 | ⬜ | |
| 05.7 | `ReportClusterStatus` handler — validate the agent owns the cluster (`PERMISSION_DENIED` otherwise), append an event | ADR §4 | ⬜ | |
| 05.8 | Surface current provider status — latest event per `cluster_orn` — on `GetKafkaCluster` (a `provider_status` field) and a `ListClusterProviderEvents` history RPC | ADR §4 | ⬜ | |
| 05.9 | Integration tests — token auth + rotation, ownership check, stream gets full set then a delta on cluster edit, event history | — | ⬜ | |

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
