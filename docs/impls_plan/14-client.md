# 14 — Client

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md) · [12](./12-telemetry-ingest.md) (for observed groups)
Specs: `003-franz/003.10-clients`, `003-franz/003.14-telemetry-ingest`
Proto: `ClientService` (CRUD, `ListObservedConsumerGroups`, `ListConsumerGroupObservations`)

## Goal

The fleet-wide SDK identity. No role, no state, no permission of its own — the
channel access policy is the sole authority. Consumer groups are read-only
projections of telemetry.

`ListClientChannelAccess` (the reverse access view) is
**[15](./15-access-policy-and-channel-access.md)** — it needs the access-policy
engine, which lands right after this deliverable.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 14.1 | `client` table — `labels jsonb`, `frn` unique, `(realm_id, name)` unique (**realm-wide** namespace). No `state` column | `003.12` | ⬜ | |
| 14.2 | Domain: `Client` — carries no permission; `name` realm-wide unique + immutable | `003.10` | ⬜ | |
| 14.3 | Repo + usecases: Create, Get, List (selector), Update (labels), Delete (row removed; `name` / FRN **not** freed) | `003.10` | ⬜ | |
| 14.4 | `ClientService` CRUD handlers + REST `/v1/clients` | proto | ⬜ | |
| 14.5 | `ListObservedConsumerGroups` (current view: distinct `(group, topic)` latest sighting) + `ListConsumerGroupObservations` (raw sightings, time range) — reads the deliverable 12 tables | `003.10`, `003.14` | ⬜ | |
| 14.6 | Integration tests — realm-wide uniqueness, delete keeps the FRN reserved, observed-group views | — | ⬜ | |

## Done when

- CRUD through the gateway; a deleted client's `name` cannot be recreated.
- Observed-consumer-group views return the latest sighting per `(group, topic)`.

## Notes

- Owner-label (`org.com/owner`) enforcement is an open question — do **not**
  require it at create for now.
- `deleted_client_frn` reservation vs. a `state` column is `003.10` open — pick
  the ledger approach unless 09 / 11 make a `state` column obviously cheaper.
