# 22 — Migration UI

Status: ⬜ not started
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
| Shard migrations | `ChannelDetail` (19), on the existing shards table — an in-flight migration renders as a row-level badge (`phase`) next to the shard it's moving, linking to a small detail panel (target cluster, phase, drain deadline, failure reason if `FAILED`) | `ListShardMigrations` filtered by `async_channel` |
| Migrate this shard | `ChannelDetail`, a per-shard row action | `MigrateKafkaTopic` |
| Cluster migrations | `KafkaClusterDetail` (08), a "Migrations" panel listing every migration where this cluster is source or target | `ListShardMigrations` filtered by `kafka_cluster` (source or target — two calls, one list, de-duplicated) |
| Drain this cluster | `KafkaClusterDetail`, a page-level action next to Pause/Resume | `MigrateCluster` (`reason=operator`) |
| Force-delete with live shards | `KafkaClusterDetail`'s existing Delete action | `DeleteKafkaClusterRequest.force=true`, with a confirm dialog explaining this starts a drain and does not delete immediately |

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
| 22.1 | **Hooks** — `useShardMigrations` (list, filtered by `async_channel` or `kafka_cluster`), `useShardMigration` (one, by id), `useMigrateKafkaTopic`, `useMigrateCluster` in `src/api/hooks.ts`, mirroring the existing hooks' query keys / `unwrap` / invalidation | 18 | ⬜ | |
| 22.2 | **Phase badge component** — `MigrationPhaseBadge`, the visual mapping above, reused on both detail pages | 001-ux shape | ⬜ | |
| 22.3 | **`ChannelDetail` integration** — per-shard migration badge + "Migrate to…" row action (a cluster picker, client-side filtered to exclude the shard's current cluster — the server re-validates via `MigrateKafkaTopic`'s own eligibility check regardless) | 19.4 | ⬜ | |
| 22.4 | **`KafkaClusterDetail` integration** — Migrations panel (source + target, de-duplicated), "Drain this cluster" action with a confirm dialog, `force=true` wired into the existing Delete action's confirm flow when the delete is rejected for having live shards | 08 | ⬜ | |
| 22.5 | **Polling** — both panels poll every 5s while any listed migration is non-terminal (mirrors 19's shard-table poll), and stop polling once every visible row is `DONE`/`FAILED` | 19 | ⬜ | |
| 22.6 | **Tests** — vitest for `MigrationPhaseBadge` (every phase renders its expected variant), the two panel integrations (badge appears/disappears, action buttons call the right RPC with the right body); `e2e/migration.spec.ts` (sign in → open a channel with a placed shard → trigger `MigrateKafkaTopic` against a second local cluster if one exists in the fixture, else assert the button calls the RPC and surfaces its error) | — | ⬜ | |

## Done when

- From the browser only: see that a shard is migrating (phase + target), and
  trigger a migration for a placed shard without a raw API call.
- From the browser only: drain a cluster, and see the resulting migrations
  listed against it.
- A cluster delete that would fail for live shards surfaces the `force=true`
  path through the UI's own confirm flow, not a raw error message.
- `npm run typecheck` / `lint` / `test` / `build` are green.

## Notes

- **Numbering** — 22 is the next free deliverable number (01–21 exist).
- **No local-seed demo**: a real migration needs a second cluster with a real
  agent able to bring the target shard to `READY` — the local dev loop
  (`make dev`) registers exactly one `local-docker` cluster, so this
  deliverable's demo path is necessarily "trigger it, see it start and sit in
  `PROVISIONING`" rather than a full lifecycle, until local dev grows a second
  cluster recipe. This is a demo-environment limitation, not a UI gap — the
  panels above render every phase correctly regardless of how far a given
  migration gets.
- Playwright's `e2e/migration.spec.ts` should degrade gracefully in that same
  single-cluster environment rather than skip outright — assert the trigger
  button calls `MigrateKafkaTopic` and shows *some* result (success starting
  `PROVISIONING`, or the expected "no eligible target" error), not that the
  migration completes.
