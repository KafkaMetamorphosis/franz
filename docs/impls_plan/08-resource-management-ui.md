# 08 — Resource management in the console

Status: ⬜ not started
Depends on: [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md) · [06](./06-web-console-bootstrap.md)
Specs: `001-ux/README`, `001-ux/demo/agents.html`, `001-ux/demo/kafka-clusters.html`, `003-franz/003.3-kafka-cluster`, `003-franz/003.9-agents`
Proto: `UpdateKafkaClusterRequest` / `UpdateAgentRequest` (FieldMask)

## Goal

Close the console gap left by deliverable 06: an operator can **create, read,
update and delete** every resource the console shows, entirely from the browser.
06 shipped list / register / detail / lifecycle for Agents and Kafka Clusters but
**no edit path** — the backend `Update*` RPCs (03.4, 04.3) have no UI. This
deliverable adds editing and makes CRUD complete for those two resources.

Scope is the resources the console owns **today** (Agents, Kafka Clusters).
Async Channel / Client / Policy / Indicator management ships with their own
features (10, 14, 15, 13) and follows the pattern this deliverable establishes.

## What is already there (06) vs. what this adds

| Resource | Create | Read | Update | Delete | Lifecycle |
|---|---|---|---|---|---|
| Agent | ✅ 06.4 | ✅ 06.4 | **08 (new)** | ✅ 06.4 | ✅ 06.4 (pause/resume/rotate) |
| Kafka Cluster | ✅ 06.5 | ✅ 06.5 | **08 (new)** | ✅ 06.5 | ✅ 06.5 (pause/resume) |

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 08.1 | `useUpdateAgent(name)` / `useUpdateKafkaCluster(name)` TanStack Query mutations — build `update_mask` from the **changed** fields only (`fieldmask` canonical proto paths), `PATCH` via the typed client, invalidate the detail + list queries on success, surface `ApiError` (400 immutable-field, 404, 409) to the form | proto, 03.4, 04.3 | ⬜ | |
| 08.2 | **Agent edit** — from the detail page, an "Edit" affordance opens a form for the mutable fields only: `type` (select — `CLUSTER_PROVIDER` / `RESOURCE_PROVIDER` / `TELEMETRY_AGENT`) and `labels` (reuse `LabelEditor`). `name`, `frn`, `status`, timestamps are read-only. Cancel restores; Save sends `update_mask=type,labels` (or the subset touched) | `003.9`, `001-ux/demo/agents.html` | ⬜ | |
| 08.3 | **Kafka Cluster edit** — same shape, mutable fields: `connection_strings` (add / remove / edit bootstrap URL rows + type), `labels`, `cluster_configuration` (key/value editor), `cluster_provider_agent` (agent picker, may be cleared). `name`, `frn`, `state`, `provider_status`, timestamps read-only | `003.3`, `001-ux/demo/kafka-clusters.html` | ⬜ | |
| 08.4 | Re-assignment warning — when `cluster_provider_agent` is changed (or cleared) on a cluster that already has one, the form shows an inline caution ("the current provider will tear its substrate down; the new provider will re-provision") and requires an explicit confirm before Save. Wording only — the hand-off itself is 05's behaviour | `003.3`, 05 | ⬜ | |
| 08.5 | Empty / invalid input handled client-side before submit: cluster `connection_strings` must keep ≥1 non-empty bootstrap URL; agent `type` cannot be set to `UNSPECIFIED`; a no-op Save (nothing changed) is disabled | `003.3`, `003.9` | ⬜ | |
| 08.6 | Concurrency — a Save that returns 409 (`FAILED_PRECONDITION` / stale) refetches the resource and asks the operator to re-apply; no silent overwrite | ADR-API-005 (lost updates) | ⬜ | |
| 08.7 | Component tests (vitest) — mask contains only changed fields, immutable fields absent from the form, `type=UNSPECIFIED` blocked, re-assignment confirm gate, 409 refetch path | — | ⬜ | |
| 08.8 | E2E smoke (Playwright) extends 06.7 — sign in → register an agent → edit its labels + type, reload, changes persisted → register a cluster → edit its `cluster_configuration`, reload, persisted | — | ⬜ | |

## Done when

- From the browser only: register an Agent, edit its `type` and `labels`, and
  the change round-trips (reload shows it); same for a Kafka Cluster's
  `connection_strings`, `labels`, `cluster_configuration`, and provider agent.
- The edit forms never present an immutable field as editable, and never send one
  in the `update_mask`.
- Changing a cluster's provider agent is gated behind an explicit confirm.
- `make test` (vitest) and the extended Playwright smoke pass; CI green.

## Notes

- Lifecycle transitions (`pause` / `resume` / `delete` / `rotateToken`) stay as
  the dedicated actions 06 already built — they are **not** folded into the edit
  form. `state` / `status` are never in an `update_mask` (03 note, `003.9`).
- No new proto, no backend change — `UpdateAgent` and `UpdateKafkaCluster` (with
  `cluster_provider_agent` in its mask) already exist and are tested.
- Realms are not console-managed (single seeded `default`, no `RealmService`
  CRUD) — out of scope until an auth/realm feature exists.
- The `001-ux` prototype is the visual reference; `register-*.html` still shows
  removed fields (`context_selector`, `UXD-007`) — build against the current
  `.proto`.

### Decisions (asked)

_(none yet — fill in when the deliverable is run)_
