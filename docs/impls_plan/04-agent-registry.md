# 04 — Agent registry

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md)
Specs: `003-franz/003.9-agents`, `003-franz/003.12-persistence-and-data-model`
Proto: `AgentService`

## Goal

The Agent registry — a control-plane record and its lifecycle. No work protocol
(that is a separate ADR). Same slice shape as 04, no dependents.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 04.1 | `agent` table — `labels jsonb`, `type text` + CHECK, `status text` + CHECK, `frn` unique, `(realm_id, name)` unique | `003.12` | ⬜ | |
| 04.2 | Domain: `Agent`, `AgentType` (`CLUSTER_PROVIDER` / `RESOURCE_PROVIDER` / `TELEMETRY_AGENT` / `CUSTOM`), `AgentStatus` machine (`ACTIVE ↔ PAUSED → DELETED`); registration is inert (no connection, no work) | `003.9` | ⬜ | |
| 04.3 | Repo + usecases: Create, Get, List (filter by `type`), Update (FieldMask; `type` mutability — treat as mutable for now), Delete (soft), Pause, Resume | `003.9` | ⬜ | |
| 04.4 | `AgentService` handlers + REST `/v1/kafka/agents` (+ `:pause` / `:resume`) | proto | ⬜ | |
| 04.5 | Integration tests — lifecycle, `type` filter, deleted agent rejects further ops, `cluster_provider_agent` in 04 is unaffected by agent delete | — | ⬜ | |

## Done when

- CRUD + pause/resume through the gateway.
- `type` is a plain filter — it changes nothing about behaviour.
- Deleting an agent referenced by a cluster's `cluster_provider_agent` succeeds
  and leaves the dangling string in place.

## Notes

- No liveness / health, no `last_contact_at` — deliberately deferred (`003.9`).
- Endpoint namespace `/v1/kafka/agents` is an accepted open question; keep as-is.
