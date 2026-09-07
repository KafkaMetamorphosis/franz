# 13 — Placement & selection

Status: ✅ done
Executed by: codex → claude (claude-sonnet-5) — codex out of quota (resets 2026-09-28) before task 13.1; Claude implemented
Depends on: [03](./03-kafka-cluster.md) · [10](./10-async-channel.md) · [12](./12-gregor-samsa.md) (task 13.8 — the partition-assignment publisher)
Specs: `003-franz/003.7-placement-and-selection`, `003-franz/003.1-conventions`

## Goal

Decide which cluster each shard lives on. Deterministic selection over cluster
rows, a retry sweep for unplaced shards, and detection (not yet movement) of
shards whose cluster stopped matching.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 13.1 | Selection algorithm as pure domain logic: candidates (`state = ACTIVE` ∧ labels satisfy `franz.affinity/selector`) → drop `franz.antiaffinity/selector` matches → drop `drain`-tainted and untolerated `no-creation`-tainted → order by `(franz.affinity/weight` desc`, name` asc`)` → take `min(shard-size, |candidates|)` → round-robin the shards | `003.7` | ✅ | 2026-09-07 |
| 13.2 | Reserved-label parsing/handling: `franz.affinity/selector`, `franz.antiaffinity/selector`, `franz.affinity/shard-size`, `franz.affinity/weight`, `franz.taint` (`<name>:<effect>`), `franz.taint/toleration` (comma-separated) | `003.1`, `003.7` | ✅ | 2026-09-07 |
| 13.3 | **Materialise + place shards** (ADR-API-009): on channel create/update (`franz.*` label change) and on cluster label/`state` change, run selection and — for each shard index `0..channel_partitions-1` that has no live `kafka_topic` row — **create** the row (`topic.New`, `state = PENDING`) on the selected cluster, seeding `partitions` / `replication_factor` / `materialized_configuration` from the cluster's `cluster_configuration` map (keys `partitions` / `replication-factor` for the dedicated fields; 003.6 OQ1 for the rest). A shard already placed on a still-matching cluster is left alone | `003.7`, `003.6` | ✅ | 2026-09-07 |
| 13.4 | Retry sweep (~30s, configurable) — for every `ACTIVE` channel with fewer live shards than `channel_partitions`, re-run selection and create the missing shards when a cluster becomes eligible | `003.7` | ✅ | 2026-09-07 |
| 13.5 | Misplaced detection — a placed shard whose cluster no longer satisfies the channel's affinity (or went `PAUSED`/`DELETED`) gets a `misplaced` marker on the row; **no move** (that is 17) | `003.7` | ✅ | 2026-09-07 |
| 13.6 | Absent `franz.affinity/selector` ⇒ no candidates ⇒ **no shards created** (do not reject the channel create) | `003.7` | ✅ | 2026-09-07 |
| 13.7 | Unit + integration tests — determinism (same labels + clusters ⇒ same assignment), taint filtering, a channel with no eligible cluster has 0 shards → registering a matching cluster materialises `channel_partitions` shards, `shard-size` capping | — | ✅ | 2026-09-07 |
| 13.8 | Notify the Resource Provider — when a shard `kafka_topic` row is created / re-placed / removed / marked `misplaced`, publish a `PartitionAssignment` delta to the in-scope Gregor Samsa agent via the deliverable-12 publisher (`SET` on create/re-place, `REMOVED` on removal) | [12](./12-gregor-samsa.md), `005` | ✅ | 2026-09-07 |

## Done when

- Two runs with identical channel labels + cluster set produce byte-identical
  shard→cluster assignments.
- A channel created with no matching cluster has **0** `kafka_topic` rows;
  registering a matching cluster materialises its `channel_partitions` shards
  within one sweep interval, `partitions` / `replication_factor` seeded from the
  cluster's config.
- Re-labelling a cluster so a placed shard mismatches sets `misplaced` and moves
  nothing.

## Notes

- `shard-size` vs. uneven `channel_partitions` distribution — **resolved**:
  round-robin, `chosen[index % k]`, so the earlier (higher-weight, then
  lower-named) clusters take the remainder. `shard-size` may exceed
  `channel_partitions`; it is capped at `|candidates|` and simply spreads
  thinner. Closes `003.7` OQ1.
- Retry-sweep interval — **resolved**: configurable
  `FRANZ_PLACEMENT__SWEEP_INTERVAL`, default `30s`, `0` disables. Event triggers
  (channel create / `franz.*` relabel; cluster create / relabel / pause / resume
  / delete) run *in addition*, so the sweep is a safety net. Closes `003.7` OQ5.
- `003.7` still says an unplaceable async-channel shard gets a `PENDING` /
  `kafka_cluster = NULL` **row**. This deliverable follows **ADR-API-009** + the
  task list: a `kafka_topic` row exists **only** once the shard is placed on a
  concrete cluster; an unplaced shard is a *missing* row. `003.7` lines ~22–24,
  ~66–70, the first Invariant, and the 003.6 cross-entity bullet need rewording
  (tracked in `docs/impls_tracker/13-placement.md`).
- A `PreviewPlacement` RPC is desired but not in scope.

### What landed

| Piece | Path |
|---|---|
| Selection algorithm (pure) | `pkg/franz/core/domain/placement/` — `labels.go` (reserved-label parsing: `ChannelRules` / `ClusterRules` / `Taint` / `Effect`, `CanHost` / `CanPlace`, `RulesChanged`), `select.go` (`Select` / `Plan` — the 5 steps, deterministic, round-robin remainder to earlier clusters) |
| Seed keys stripped from the merge | `topic/config.go` — `ConfigKeyPartitions` / `ConfigKeyReplicationFactor` excluded from `Materialize`; `SeedPartitions` / `SeedReplicationFactor` helpers (003.6 OQ1) |
| Misplaced marker | `topic/topic.go` domain methods; `kafka_topic.misplaced` / `misplaced_reason` in `V1__init.sql` (idempotent `ADD COLUMN IF NOT EXISTS`); additive proto `KafkaTopic.misplaced = 14` / `misplaced_reason = 15` |
| Placement use-case | `pkg/franz/core/usecases/placement/` — `Place` / `PlaceChannel` / `PlaceRealm` / `Sweep`; `out.ShardPlacer` port; `out.TopicRepository` gains a one-txn `PlaceChannelShards` (channel + shard rows `FOR UPDATE`), `AsyncChannelRepository` gains `ListActive` / `ListUnderplaced` |
| Wiring | `channels.Service` (create + `franz.*`-relabel update) and `clusters.Service` (create / update / pause / resume / delete) call the placement service, then `resourceprovider.Notifier.ShardsChanged`; label validation added to both write paths; `startPlacementSweep` fx loop in `cmd/franz/main.go` (`config.yaml` + `pkg/franz/config`) |
| Tests | `domain/placement/*_test.go` (selection + determinism + label validation), `usecases/placement/service_test.go` (fakes), `adapters/out/postgres/placement_integration_test.go` (7 Postgres tests), `adapters/in/grpcgateway/placement_integration_test.go` (bufconn: placement → connected agent `SET`) |

### Behaviour decisions worth knowing

- **Cluster create/pause/resume/delete also run a placement pass** (not just
  update) — a channel materialises on cluster registration, not up to a sweep
  interval later.
- **Cluster `Update` re-places on any label edit** (a channel's affinity matches
  free-form cluster labels, so no reserved-key shortcut); channel `Update` uses
  `RulesChanged` (reserved keys only) to skip a no-op pass.
- **Removing `franz.affinity/selector` from a channel marks every placed shard
  misplaced** (mechanically: no selector ⇒ no candidates ⇒ every cluster fails
  affinity). Any `kafka_topic` row seeded outside placement gets marked on the
  first pass over its channel.
- **`no-creation` does not misplace an already-placed shard; only `drain` does.**
- The misplaced marker does **not** bump `generation` and is only pushed to
  agents when it actually flips.
