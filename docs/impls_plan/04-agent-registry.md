# 04 — Agent registry

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex unavailable (usage limit, resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md)
Specs: `003-franz/003.9-agents`, `003-franz/003.12-persistence-and-data-model`
Proto: `AgentService`

## Goal

The Agent registry — a control-plane record and its lifecycle. No work protocol
(that is a separate ADR). Same slice shape as 04, no dependents.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 04.1 | `agent` table — `labels jsonb`, `type text` + CHECK, `status text` + CHECK, `token_hash text`, `frn` unique, `(realm_id, name)` unique | `003.12` | ✅ | 2026-09-06 |
| 04.2 | Domain: `Agent`, `AgentType` (`CLUSTER_PROVIDER` / `RESOURCE_PROVIDER` / `TELEMETRY_AGENT` / `CUSTOM`), `AgentStatus` machine (`ACTIVE ↔ PAUSED → DELETED`); registration is inert (no connection, no work) | `003.9` | ✅ | 2026-09-06 |
| 04.3 | Repo + usecases: Create (mint token), Get, List (filter by `type`), Update (FieldMask; `type` mutable), Delete (soft), Pause, Resume, RotateToken | `003.9` | ✅ | 2026-09-06 |
| 04.4 | `AgentService` handlers + REST `/v1/kafka/agents` (+ `:pause` / `:resume` / `:rotateToken`) | proto | ✅ | 2026-09-06 |
| 04.5 | Integration tests — lifecycle, `type` filter, deleted agent rejects further ops, `cluster_provider_agent` in 03 is unaffected by agent delete | — | ✅ | 2026-09-06 |

## Done when

- CRUD + pause/resume through the gateway.
- `type` is a plain filter — it changes nothing about behaviour.
- Deleting an agent referenced by a cluster's `cluster_provider_agent` succeeds
  and leaves the dangling string in place.

## Notes

- No liveness / health, no `last_contact_at` — deliberately deferred (`003.9`).
- Endpoint namespace `/v1/kafka/agents` is an accepted open question; keep as-is.

### What landed

| Layer | Package |
|---|---|
| Domain entity + type/status machines | `pkg/franz/core/domain/agent` |
| Bearer-token mint/hash primitive | `pkg/shared/token` (`Generate`, `Hash`; `frnat_` prefix, sha256) |
| Driving / driven ports | `core/ports/in/agent.go`, `core/ports/out/agent.go` |
| Application service | `core/usecases/agents` (Create mints token; RotateToken) |
| Postgres adapter | `adapters/out/postgres/agent.go` (type filter pushed to SQL; `Mutate` = `SELECT … FOR UPDATE`) |
| gRPC + REST handler | `adapters/in/grpcgateway/agent.go` (`RegisterAgentService`) |
| Migration | `migrations/V1__init.sql` — `agent` (`token_hash` column) |

Verified end-to-end through the REST gateway against real Postgres: create →
one-time `token` + FRN, `?type=` filter, `:rotateToken` → new token, PATCH with
`update_mask=type`, `:pause` → 200, `AGENT_TYPE_UNSPECIFIED` create → 400, delete
→ 200. Integration test confirms deleting an agent leaves a cluster's
`cluster_provider_agent` string dangling (unvalidated link, `003.3`).

### Assumptions (revisit if wrong)

1. **`type` is mutable** via `UpdateAgent` mask (deliverable note + `003.9` OQ2).
2. **Pause / Resume idempotent**; `GetAgent` returns a soft-deleted agent (same as
   Kafka Cluster).
3. **Token**: opaque `frnat_` + 32 random bytes (base64url); only the sha256 hex
   is stored. No verification path yet — deliverable 05 consumes it.
4. **`ListAgents` has no label selector** — the proto only exposes a `type`
   filter, so that is all List does. `page.total_size` = 0 (best-effort).
5. **Delete while referenced by a cluster** → allowed, dangling string kept
   (`003.9` OQ3 resolved this way per `003.3`).
6. `RotateAgentToken` rejects a soft-deleted agent (`FAILED_PRECONDITION`).
