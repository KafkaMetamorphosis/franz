# 21 — Client UI

Status: ⬜ not started
Depends on: [06](./06-web-console-bootstrap.md) · [08](./08-resource-management-ui.md) · [16](./16-client.md)
Specs: `003-franz/003.10-clients`
Proto: none — `ClientService` was generated ahead of deliverable 16; `schema.d.ts` already carries it

## Goal

Deliverable 16 shipped the Client registry, its CRUD RPCs, and the two
observed-consumer-group read views — but **no console screens**. 21 adds them,
following the list / register / detail / edit page shape 06, 08, and 19
established.

Scope is deliberately **Client CRUD + observed-consumer-group views only**.
`ListClientChannelAccess` (the "channels this client may use" reverse view) is
**out of scope and deferred to [17](./17-access-policy-and-channel-access.md)**,
mirroring how 19 deferred `AsyncChannel`'s access-policy panel to the same
deliverable — it needs the access-policy engine, which 17 builds.

## Design

### Routes

| Path | Page |
|---|---|
| `/clients` | `ClientList` |
| `/clients/register` | `ClientRegister` |
| `/clients/:name` | `ClientDetail` |
| `/clients/:name/edit` | `ClientEdit` |

The sidebar's disabled **Clients** placeholder (left disabled by 19's Notes:
"the Clients entry stays disabled") becomes a real link.

### What is editable

`UpdateClient` masks **`labels` only** — `name` is immutable (003.10).
`ClientEdit` therefore renders name as a read-only fact and never puts it in a
mask, the same pattern `ChannelEdit` (19) uses for `type`/`channel_partitions`.

### Panels

| Panel | Where | Source |
|---|---|---|
| Observed consumer groups (current view) | `ClientDetail` | `ListObservedConsumerGroups` |
| Consumer-group observation history | `ClientDetail`, a "show history" expansion on one group row | `ListConsumerGroupObservations` |

### Panels deliberately absent from the detail page

| Demo panel | Why it is not here |
|---|---|
| Channel access ("channels this client may use") | deliverable 17 — `ListClientChannelAccess` needs the access-policy engine |
| Credentials / connection testing | `003.10` "Deferred to a later ADR" — not built anywhere yet |

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 21.1 | **Hooks** — `useClients` / `useClient` / `useCreateClient` / `useUpdateClient` / `useDeleteClient` / `useObservedConsumerGroups` / `useConsumerGroupObservations` in `src/api/hooks.ts`, mirroring the channel/agent hooks' query keys, `unwrap`, invalidation, `updateMask` | 16, 08.4 | ⬜ | |
| 21.2 | **`ClientList`** — name + FRN, label tags (highlighting `org.com/owner` when present); "Register Client" action; empty state | `001-ux` shape (no demo page exists — follow `ChannelList`) | ⬜ | |
| 21.3 | **`ClientRegister`** — name (required, immutable note), labels (an `org.com/owner` note per 003.10, not enforced — OQ1 is open) | `003.10` | ⬜ | |
| 21.4 | **`ClientDetail`** — identity (name, FRN, labels, created/updated), observed-consumer-groups table (group, channel, topic, custom badge, last seen), a per-row "history" expansion calling `ListConsumerGroupObservations`; Edit / Delete with a `confirm()` gate warning that the name/FRN can never be reused | 16.5 | ⬜ | |
| 21.5 | **`ClientEdit`** — labels only, change-detection mask, name shown read-only, 409 reload-and-re-apply | `003.10`, 08.8 | ⬜ | |
| 21.6 | **Routing + nav + Home** — four routes in `App.tsx`; sidebar `Clients` link replacing the disabled placeholder; Clients stat card on `Home` | 06, 19.7 | ⬜ | |
| 21.7 | **Tests** — vitest for `ClientList` (columns, empty state), `ClientRegister` (create body, field violation), `ClientEdit` (labels-only mask, name absent from mask, Save gating); Playwright `e2e/clients.spec.ts` (sign in → register → list → detail → edit labels → delete → recreate-same-name is rejected) | — | ⬜ | |

## Done when

- From the browser only: register a Client, see it listed, open its detail
  page, edit its labels, and delete it — every change round-trips (reload
  shows it).
- The edit form never presents `name` as editable and never sends it in an
  `update_mask`.
- Deleting a client and immediately re-registering the same name surfaces a
  clear "name is reserved" message, not a generic error (003.10 "DeleteClient
  does not free the name / FRN").
- The observed-consumer-groups panel shows real rows once
  `local/seed/05-clients.sql`'s seeded clients have any telemetry reported
  against them (empty state otherwise — reporting is via a real Telemetry
  Agent, not seeded).
- `npm run typecheck` / `lint` / `test` / `build` and the Playwright smoke are
  green.

## Notes

- **Numbering** — 21 is the next free deliverable number (01–20 exist). Same
  precedent 08/19/20 set: console work for an already-shipped backend is its
  own numbered deliverable.
- **Local dev prerequisite**: `local/seed/05-clients.sql` (added alongside
  deliverable 16) registers two example clients (`billing`,
  `payments-consumer`), so `ClientList` has real rows before this deliverable
  writes a single line of UI code.
- The observed-consumer-group panel will be empty for the seeded clients until
  a real Telemetry Agent reports against them — no seed populates
  `observed_consumer_group` rows, since a sighting is meant to be evidence a
  real agent produced, not a fixture.
- The list page does not paginate — same open item 19's Notes already tracks
  across all four existing list pages.
