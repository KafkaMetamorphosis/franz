# 16 — Client

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex out of quota (resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md) · [15](./15-telemetry-ingest.md) (for observed groups)
Specs: `003-franz/003.10-clients`, `003-franz/003.14-telemetry-ingest`
Proto: `ClientService` (CRUD, `ListObservedConsumerGroups`, `ListConsumerGroupObservations`)

## Goal

The fleet-wide SDK identity. No role, no state, no permission of its own — the
channel access policy is the sole authority. Consumer groups are read-only
projections of telemetry.

`ListClientChannelAccess` (the reverse access view) is
**[17](./17-access-policy-and-channel-access.md)** — it needs the access-policy
engine, which lands right after this deliverable.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 16.1 | `client` table — `labels jsonb`, `frn` unique, `(realm_id, name)` unique (**realm-wide** namespace). No `state` column | `003.12` | ✅ | 2026-09-13 |
| 16.2 | Domain: `Client` — carries no permission; `name` realm-wide unique + immutable | `003.10` | ✅ | 2026-09-13 |
| 16.3 | Repo + usecases: Create, Get, List (selector), Update (labels), Delete (row removed; `name` / FRN **not** freed) | `003.10` | ✅ | 2026-09-13 |
| 16.4 | `ClientService` CRUD handlers + REST `/v1/clients` | proto | ✅ | 2026-09-13 |
| 16.5 | `ListObservedConsumerGroups` (current view: distinct `(group, topic)` latest sighting) + `ListConsumerGroupObservations` (raw sightings, time range) — reads the deliverable 15 tables | `003.10`, `003.14` | ✅ | 2026-09-13 |
| 16.6 | Integration tests — realm-wide uniqueness, delete keeps the FRN reserved, observed-group views | — | ✅ | 2026-09-13 |

## Done when

- CRUD through the gateway; a deleted client's `name` cannot be recreated.
- Observed-consumer-group views return the latest sighting per `(group, topic)`.

## Notes

- Owner-label (`org.com/owner`) enforcement is an open question — do **not**
  require it at create for now.
- `deleted_client_frn` reservation vs. a `state` column is `003.10` open — pick
  the ledger approach unless 09 / 11 make a `state` column obviously cheaper.

### What landed

| Piece | Path |
|---|---|
| Domain | `core/domain/client/client.go` — `Client` (no Type/Role/Status field, matching 003.10 exactly), `New`, `SetLabels` (wholesale replacement — the only mutable field there is) |
| Ports | `core/ports/in/client.go` (`ClientService`, `ListObservedConsumerGroupsInput`/`ListConsumerGroupObservationsInput`/`ObservedGroupPage`), `core/ports/out/client.go` (`ClientRepository`) |
| Usecase | `core/usecases/clients/service.go` — CRUD over `ClientRepository`; `ListObservedConsumerGroups`/`ListConsumerGroupObservations` resolve the client first (404 if absent) then scope deliverable 15's `ObservedConsumerGroupRepository.ListCurrent`/`ListObservations` by `client.FRN.Path()` |
| Postgres | `adapters/out/postgres/client.go` — `ClientRepo`; `Create` checks `deleted_client_frn` before inserting (rejects reusing a deleted name with the same `errs.AlreadyExists` a live duplicate gets); `Delete` reserves the name/FRN and removes the row in one transaction |
| Handler | `adapters/in/grpcgateway/client.go` — full `ClientService` CRUD + the two observed-consumer-group reads (gRPC + REST); `ListClientChannelAccess` stays `Unimplemented` (17). `custom`/`last_seen_at` map straight off `consumergroup.Observation` — the current view and the history view share one mapping function since both read `ObservedAt` |
| Migration | `V1__init.sql` — `client` table (no `state` column, by design) + `deleted_client_frn` ledger (composite `(realm_id, name)` primary key, no FK back to `client` since the row it reserves is already gone) |
| Wiring | `cmd/franz/main.go` — `NewClientRepo` / `clients.NewService` / `RegisterClientService` |
| Local seed | `local/seed/05-clients.sql` — two example clients (`billing`, `payments-consumer`) so `ListClients` has real rows locally |

### Decision: the ledger approach, as the plan's own Notes call for

Task 16.1 already rules out a `state` column ("No `state` column"), and the
Notes' "pick the ledger approach" is exactly what shipped:
**`deleted_client_frn (realm_id, name, frn, deleted_at)`**, written in the same
transaction that removes the `client` row. `Create` checks it before every
insert. This is the only entity in the schema where delete is a real `DELETE`
rather than a state flip — every other Franz entity's soft-delete convention
(`state = 'DELETED'`, ADR-API-004) doesn't apply here because 003.10 is
explicit a Client has no state field at all.

### Not done (belongs to later deliverables)

- **`ListClientChannelAccess`** — needs the access-policy engine ([17](./17-access-policy-and-channel-access.md)).
- **Console UI** — no screens exist yet; scoped as [21 — Client UI](./21-client-ui.md), not built.
- **Owner-label enforcement** (`003.10` OQ1) — still unenforced, as planned.
- **Custom-group→client attribution** (`003.10` OQ2) — unchanged from 15: a
  custom-named group's `client_frn` is whatever the reporting agent resolved
  (often empty), not inferred here.
