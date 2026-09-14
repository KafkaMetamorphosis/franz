# 18 — Migration & data movement

Status: ✅ done (18.4's misplaced-shard auto-relocate explicitly deferred — see Notes)
Executed by: claude (claude-sonnet-5) — codex out of quota (resets 2026-09-28), never ran
Depends on: [09](./09-kafka-topic.md) · [10](./10-async-channel.md) · [13](./13-placement.md)
Specs: `003-franz/003.13-migration-and-data-movement`, `003-franz/003.3`, `003-franz/003.4`, `003-franz/003.7`, `003-franz/003.8`

## Goal

The single staged flow that moves a shard's serving position from one cluster to
another. Once it exists it unblocks: placed-shard relocation (13.5 → real move),
cluster delete with live topics (`003.3`), re-shard execution (`003.4`
`channel_partitions` change), and governance's placement / taint / re-shard
actions (`003.8` OQ1a–c).

> **The mechanism, reframed** — the design that actually shipped is simpler
> than the original task list below assumed, per the user's own correction
> mid-deliverable: an Async Channel already spreads load over N Kafka Topics by
> percentage (`traffic_share`), and a topic's consumer can already be
> ENABLED/DISABLED (`SetConsumption`, deliverable 09). **Migration is not a new
> isolated mechanism — it is orchestration of three primitives that already
> exist**: (1) create an ordinary new shard row on the target cluster (the same
> `PlaceChannelShards` path placement already uses); (2) cut over by calling the
> existing `SetConsumption(source, DISABLED)`, which already re-normalises
> `traffic_share` across the ENABLED siblings; (3) once the source has no
> consumer, no lag, and no messages left on disk (retention-cleaned), retire it
> through the **normal topic-delete path** (`state → DELETED`), which the agent
> already reacts to via the existing `CHANGE_REMOVED` assignment mechanism. The
> source row's `kafka_cluster_id` never changes — there is no "flip the
> cluster" step, which was the original (wrong) hypothesis this deliverable
> started from. `shard_migration` is bookkeeping/audit and a sweep driver, not
> a parallel state machine layered onto `kafka_topic`.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 18.1 | `shard_migration` table + phase machine (`PROVISIONING` → `CUTOVER` → `DRAINING` → `RETIRING` → `DONE` / `FAILED`), one active row per shard, idempotent/resumable sweep | `003.13` | ✅ | 2026-09-14 |
| 18.2 | Provision — create the target shard row on the destination cluster via the existing placement path; cut over via `SetConsumption(DISABLED)` on the source (no `kafka_cluster_id` flip — see reframe above) | `003.13` | ✅ | 2026-09-14 |
| 18.3 | Drain — hold `DRAINING` until two new Gregor Samsa indicators (`kafka.topic.drained`, `kafka.topic.consumer_connected`) confirm no data and no consumer, or a fixed 1h deadline elapses; then retire the source through the normal delete path | `003.13` | ✅ | 2026-09-14 |
| 18.4 | Triggers — operator RPC (`MigrateKafkaTopic`/`MigrateCluster`), cluster-delete-force, `drain` taint auto-trigger. **Misplaced-shard auto-relocate is deferred** (see Notes) | `003.7`, `003.8` | 🚧 | 2026-09-14 |
| 18.5 | Concurrency limits — `KafkaCluster.MaxConcurrentMigrations` (new field, default 1), checked on both source and target | `003.13` | ✅ | 2026-09-14 |
| 18.6 | Cluster delete with live topics — `DeleteKafkaClusterRequest.force`; without force, live shards reject with `FAILED_PRECONDITION`; with force, the cluster stays ACTIVE and `MigrateCluster` drains it, deletion completing on a later call once `CountLiveTopics` reaches zero naturally | `003.3` | ✅ | 2026-09-14 |
| 18.7 | Re-shard execution — `channel_partitions` increase adds shard topics through the normal placement path (`AsyncChannel.SetChannelPartitions`, increase-only; decrease is out of scope, since shrinking needs this same drain-then-retire flow to run per removed shard, which 18.4/18.8 do not yet drive automatically) | `003.4` | ✅ | 2026-09-14 |
| 18.8 | Enable governance placement actions (`003.8` OQ1a–c) to call the flow — `franz.affinity/*` / `franz.antiaffinity/*` / `franz.taint` labels and a `channel_partitions` increase are un-deferred from the write whitelist; a `channel_partitions` decrease stays deferred (same reason as 18.7) | `003.8` | ✅ | 2026-09-14 |
| 18.9 | Tests — phase-by-phase lifecycle (idempotent resume, no data removed before the next phase confirms), rejection cases (same cluster, ineligible target, duplicate migration, concurrency limit), drain-taint auto-trigger, re-shard, governance-action un-deferral | — | ✅ | 2026-09-14 |

## Done when

- A `drain` taint on a cluster relocates every shard off it with no consumer data
  loss (bounded, drainable lag). ✅ `TestClusterDrainTaintAutoTriggersMigration`,
  `TestMigrateClusterMovesEveryLiveShard`.
- Deleting a cluster with live topics behaves per the resolved OQ5 decision
  (`force=true` required, auto-triggers drain). ✅ `clusters.Service.Delete`.
- Governance can pause/label a resource **and** now also re-place / taint /
  re-shard. ✅ `TestEvaluateAppliesChannelPartitionsReshard`,
  `TestCreatePolicyAcceptsPlacementActions` — affinity/antiaffinity/taint
  labels and a channel_partitions increase all reach the migration flow through
  the normal `channels.Service.Update` / `clusters.Service.Update` path a
  governance action already goes through for every other field.

## Notes

- v1 is **drain-based** — no historical byte copy; a key's old messages stay on
  the old shard until consumed, cleaned up by the topic's own retention policy.
  Replication-based (MirrorMaker2 / offset translation) stays a future OQ; no
  task here needed it once the mechanism was reframed around existing
  primitives.
- **Deferred, not built**: (a) misplaced-shard auto-relocate — 13.5 already
  marks a shard `misplaced` when its cluster stops satisfying the channel's
  affinity/taint rules, but nothing calls `MigrateKafkaTopic` for it
  automatically; an operator (or a governance policy reacting to a
  `misplaced`-derived indicator, once one exists) drives it manually today. (b)
  A `channel_partitions` **decrease** (re-shard down, or the removed-shard half
  of a governance/operator re-shard) — increasing is fully automatic end to
  end; decreasing needs the same drain-then-retire sequence run per *removed*
  shard, which no trigger here drives yet. Both are bounded, well-understood
  follow-ups, not open design questions — see the tracker for the full
  decision log and the reasoning for stopping here.
- This was the last blocker in the plan; everything else (01–17, 19) is
  already shipped, and 20/21 (Governance UI / Client UI) do not depend on this.
- **No console screens for migration yet** — this deliverable ships the flow
  and the RPCs only. Per the plan's UI convention (see `README.md`),
  [22 — Migration UI](./22-migration-ui.md) is scoped (not built) to close
  that gap: trigger/observe panels on the existing Async Channel and Kafka
  Cluster detail pages, no new top-level route.

## What landed

| Piece | Path |
|---|---|
| Domain | `core/domain/migration/migration.go` — `Phase` (PROVISIONING/CUTOVER/DRAINING/RETIRING/DONE/FAILED), `Reason` constants, `ShardMigration` + read-path name projections, phase-guarded transition methods (`AdvanceToCutover`, `AdvanceToDraining`, `ReadyToRetire`, `AdvanceToRetiring`, `Complete`, `Fail`); `core/domain/channel/channel.go` — `SetChannelPartitions` (increase-only); `core/domain/cluster/cluster.go` — `MaxConcurrentMigrations` / `DefaultMaxConcurrentMigrations` |
| Ports | `core/ports/out/migration.go` (`ShardMigrationRepository`), `core/ports/in/migration.go` (`MigrationService`), `core/ports/out/topic.go` (+`GetByID`) |
| Usecases | `core/usecases/migration/service.go` — `MigrateKafkaTopic`, `MigrateCluster` (auto target selection via `placementdomain.ChannelRules.Plan`, excludes source), `Sweep` → `advance{Provisioning,Cutover,Draining,Retiring}`, concurrency + duplicate-migration checks; `core/usecases/clusters/service.go` — `migrator in.MigrationService` dependency, `Delete(ctx, name, force)`, drain-taint auto-trigger on the labels transition; `core/usecases/channels/service.go` — `ChannelPartitions` mask forwarding; `core/usecases/governance/actions.go` — `writeChannelField` (18.8), `whitelist.go`'s `deferredAction` trimmed to just the channel_partitions-decrease case |
| Telemetry | `pkg/gregorsamsa/telemetry/telemetry.go` — `IndicatorTopicDrained` / `IndicatorTopicConsumerConnected`, computed from existing `kafkaadmin.Admin.ListOffsets` / `ListConsumerGroups` / `ListConsumerGroupOffsets` (no new agent-protocol RPC) |
| Postgres | `adapters/out/postgres/migration.go` (`ShardMigrationRepo`), `cluster.go` (+`max_concurrent_migrations`), `topic.go` (+`GetByID`), `channel.go` (fixed a pre-existing bug — `persistChannel`'s UPDATE never wrote `channel_partitions`) |
| Proto/handlers | `api/franz/v1/migration.proto` (new `MigrationService`), `kafka.proto` (+`max_concurrent_migrations`, `DeleteKafkaClusterRequest.force`), `async_channel.proto` (+`channel_partitions` on Update); `adapters/in/grpcgateway/migration.go`, `kafkacluster.go`, `asyncchannel.go` |
| Migration (SQL) | `migrations/V1__init.sql` — `kafka_cluster.max_concurrent_migrations`, new `shard_migration` table with a partial-unique index enforcing one active migration per source shard |
| Wiring | `cmd/franz/main.go` — dual-provided `in.MigrationService`, `startMigrationSweep` (30s tick), `startShardMigrationPrune` (nightly, 30-day retention) |
| Tests | `core/domain/migration/migration_test.go` (10 cases); `pkg/gregorsamsa/telemetry/telemetry_test.go` (5 cases); `adapters/out/postgres/migration_integration_test.go` (full lifecycle + rejection cases + drain-taint trigger); `adapters/out/postgres/placement_integration_test.go` (re-shard increase/reject-decrease); `core/domain/governance/policy_test.go` + `core/usecases/governance/{service,evaluator}_test.go` (18.8 un-deferral) |
