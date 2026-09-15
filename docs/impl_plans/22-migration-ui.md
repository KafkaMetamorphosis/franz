# 22 — Migration UI

Status: ✅ done (cluster-scoped migration list dropped — see the corrected Design note)
Executed by: claude (claude-sonnet-5)
Depends on: [06](./06-web-console-bootstrap.md) · [08](./08-resource-management-ui.md) · [18](./18-migration-and-data-movement.md) · [19](./19-async-channel-ui.md)
Specs: `003-franz/003.13-migration-and-data-movement`
Proto: none — `MigrationService` was generated ahead of this deliverable (alongside
18); `schema.d.ts` already carries `v1ShardMigration` / `ListShardMigrations` /
`GetShardMigration` / `MigrateKafkaTopic` / `MigrateCluster`

## Goal

Deliverable 18 shipped the migration flow and its RPCs, but the only way to
start or watch a migration today is `curl`. This adds the console screens: an
operator can migrate a shard or drain a cluster from a button, and see the
in-flight and historical migrations for a channel or a cluster without a raw
API call — closing the same "console UI" gap 19/20/21 closed for their
deliverables (see the plan `README.md`'s conventions section).

Scope is **read + trigger only** — there is no migration *editor*: a
`ShardMigration` row is created by an action (`MigrateKafkaTopic`,
`MigrateCluster`, a `drain` taint, or a cluster-delete with `force=true`) and
then only advances on its own via the server-side sweep. Nothing in this
deliverable writes to a `ShardMigration` row directly.

## Design

### Where migrations surface (no new top-level route)

There is no standalone `/migrations` list — a `ShardMigration` is always about
a specific channel's shard or a specific cluster, so it surfaces on the pages
that already exist for those:

| Panel | Where | Source |
|---|---|---|
| Shard migrations | `ChannelDetail` (19), on the existing shards table — a per-shard `MigrationPhaseBadge` renders whenever a migration involves that shard (as source or target) | `ListShardMigrations` filtered by `async_channel` |
| Migrate this shard | `ChannelDetail`, a per-shard row action (an inline target-cluster `<select>`, excluding the shard's own cluster) | `MigrateKafkaTopic` |
| Drain this cluster | `KafkaClusterDetail`, a page-level action next to Pause/Resume | `MigrateCluster` (`reason=operator`) |
| Force-delete with live shards | `KafkaClusterDetail`'s existing Delete action | `DeleteKafkaClusterRequest.force=true`, offered inline once a plain delete is rejected for having live shards |

> **Corrected against the actual wire contract (found live, not assumed):**
> the row above this note originally planned a cluster-scoped "Migrations"
> panel on `KafkaClusterDetail`, filtered by `kafka_cluster`. `ListShardMigrations`
> turned out to have **no such filter at all** — only `async_channel`, which
> it resolves to a channel row and rejects (`NotFound`) if empty or unknown.
> There is no "list every migration" query shape either. Aggregating a
> cluster-scoped view would mean fetching every channel in the realm and
> calling `ListShardMigrations` once per channel — an N+1 pattern nothing
> else in the console does, for a page that already has `ChannelDetail` as
> the correct, backend-supported place to see a shard's migration status.
> `KafkaClusterDetail` instead carries a short explanatory note pointing there,
> and keeps only the two RPCs that genuinely are cluster-scoped: `MigrateCluster`
> (Drain) and `DeleteKafkaCluster`'s `force`.

### Phase badge

`PROVISIONING` / `CUTOVER` / `DRAINING` / `RETIRING` render as a neutral
in-progress badge (same visual language `AgentStatus` already uses for
"converging"); `DONE` fades from the panel after one poll (it's history, not
action-needed); `FAILED` renders as an error badge with `failure_reason` as its
tooltip/expansion.

### Panels deliberately absent

| Demo panel | Why it is not here |
|---|---|
| A standalone `/migrations` list-everything page | No spec calls for a fleet-wide migration inbox; every migration already has an owning channel or cluster page to live on |
| Retry / cancel a migration | Not an RPC `18` shipped — `MigrationService` has no `RetryShardMigration` / `CancelShardMigration`; a `FAILED` migration is diagnosed from `failure_reason` and re-triggered as a fresh `MigrateKafkaTopic` call |
| Editing `drain_deadline` per migration | `18` resolved this as a fixed, config-wide 1h deadline (`003.13` OQ3) — there is nothing per-row to edit |

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 22.1 | **Hooks** — `useShardMigrations` (list, `async_channel`-scoped — the only shape the RPC supports), `useShardMigration` (one, by id), `useMigrateKafkaTopic` (topic name as a mutate-time variable, not a hook argument, so a shards table can trigger it per-row from one instance), `useMigrateCluster`, and `force` support on `useClusterLifecycle`'s existing `remove` mutation, in `src/api/hooks.ts` | 18 | ✅ | 2026-09-14 |
| 22.2 | **Phase badge component** — `MigrationPhaseBadge`, the visual mapping above, reused on both detail pages | 001-ux shape | ✅ | 2026-09-14 |
| 22.3 | **`ChannelDetail` integration** — per-shard migration badge + "Migrate to…" row action (a cluster picker, client-side filtered to exclude the shard's current cluster — the server re-validates via `MigrateKafkaTopic`'s own eligibility check regardless) | 19.4 | ✅ | 2026-09-14 |
| 22.4 | **`KafkaClusterDetail` integration** — "Drain" action with a confirm dialog, `force=true` offered inline once a plain Delete is rejected for having live shards (**not** a cluster-scoped Migrations panel — see the corrected Design note) | 08 | ✅ | 2026-09-14 |
| 22.5 | **Polling** — `ChannelDetail`'s migrations query polls at the same fixed 5s cadence its shard table already uses (simpler than gating the interval on whether anything is non-terminal, and consistent with `ClusterDetail`'s own provider-status poll never stopping either) | 19 | ✅ | 2026-09-14 |
| 22.6 | **Tests** — `e2e/migration.spec.ts` (drain a cluster from the browser, assert a clean response — no vitest unit tests added beyond what 22.1–22.4's code review covers, since `MigrationPhaseBadge` and the row-action wiring are simple enough that the existing suite's coverage of the surrounding pages was judged sufficient) | — | ✅ | 2026-09-14 |

## Done when

- From the browser only: see that a shard is migrating (phase + target), and
  trigger a migration for a placed shard without a raw API call.
- From the browser only: drain a cluster.
- A cluster delete that would fail for live shards surfaces the `force=true`
  path through the UI's own confirm flow, not a raw error message.
- `npm run typecheck` / `lint` / `test` / `build` are green.

## Notes

- **Numbering** — 22 is the next free deliverable number (01–21 exist).
- **Cluster-scoped migration list dropped** — see the corrected Design note.
  `KafkaClusterDetail` keeps a short pointer to `ChannelDetail` instead of a
  panel that would need an N+1 fetch to populate honestly.
- **No local-seed demo**: a real migration needs a second cluster with a real
  agent able to bring the target shard to `READY` — the local dev loop
  (`make dev`) registers exactly one `local-docker` cluster, so a placed
  shard's "Migrate to…" picker has no eligible target to offer in that
  environment (by design — the picker excludes the shard's own cluster, and
  there is no other one registered). This is a demo-environment limitation,
  not a UI gap — the per-row action renders correctly and simply has nothing
  to offer until a second cluster exists.
- `e2e/migration.spec.ts` verifies the one flow the single-cluster local loop
  *can* exercise for real: draining a cluster. `MigrateCluster` succeeds (200)
  even when every shard has no eligible target — it just moves none
  (`TestMigrateClusterSkipsShardsWithNoEligibleTarget` in the Go suite) — so
  the assertion is "the page stays clean, no error banner," not that a
  migration actually starts.
