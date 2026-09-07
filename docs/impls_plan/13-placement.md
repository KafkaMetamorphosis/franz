# 13 — Placement & selection

Status: ⬜ not started
Depends on: [03](./03-kafka-cluster.md) · [10](./10-async-channel.md) · [12](./12-gregor-samsa.md) (task 13.8 — the partition-assignment publisher)
Specs: `003-franz/003.7-placement-and-selection`, `003-franz/003.1-conventions`

## Goal

Decide which cluster each shard lives on. Deterministic selection over cluster
rows, a retry sweep for unplaced shards, and detection (not yet movement) of
shards whose cluster stopped matching.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 13.1 | Selection algorithm as pure domain logic: candidates (`state = ACTIVE` ∧ labels satisfy `franz.affinity/selector`) → drop `franz.antiaffinity/selector` matches → drop `drain`-tainted and untolerated `no-creation`-tainted → order by `(franz.affinity/weight` desc`, name` asc`)` → take `min(shard-size, |candidates|)` → round-robin the shards | `003.7` | ⬜ | |
| 13.2 | Reserved-label parsing/handling: `franz.affinity/selector`, `franz.antiaffinity/selector`, `franz.affinity/shard-size`, `franz.affinity/weight`, `franz.taint` (`<name>:<effect>`), `franz.taint/toleration` (comma-separated) | `003.1`, `003.7` | ⬜ | |
| 13.3 | **Materialise + place shards** (ADR-API-009): on channel create/update (`franz.*` label change) and on cluster label/`state` change, run selection and — for each shard index `0..channel_partitions-1` that has no live `kafka_topic` row — **create** the row (`topic.New`, `state = PENDING`) on the selected cluster, seeding `materialized_configuration` from the cluster's `franz.kafka-config/*` labels (prefix-stripped; deliverable 11) and `partitions` / `replication_factor` from `franz.kafka-config/num.partitions` / `franz.kafka-config/default.replication.factor`. A shard already placed on a still-matching cluster is left alone | `003.7`, `003.6` | ⬜ | |
| 13.4 | Retry sweep (~30s, configurable) — for every `ACTIVE` channel with fewer live shards than `channel_partitions`, re-run selection and create the missing shards when a cluster becomes eligible | `003.7` | ⬜ | |
| 13.5 | Misplaced detection — a placed shard whose cluster no longer satisfies the channel's affinity (or went `PAUSED`/`DELETED`) gets a `misplaced` marker on the row; **no move** (that is 17) | `003.7` | ⬜ | |
| 13.6 | Absent `franz.affinity/selector` ⇒ no candidates ⇒ **no shards created** (do not reject the channel create) | `003.7` | ⬜ | |
| 13.7 | Unit + integration tests — determinism (same labels + clusters ⇒ same assignment), taint filtering, a channel with no eligible cluster has 0 shards → registering a matching cluster materialises `channel_partitions` shards, `shard-size` capping | — | ⬜ | |
| 13.8 | Notify the Resource Provider — when a shard `kafka_topic` row is created / re-placed / removed / marked `misplaced`, publish a `PartitionAssignment` delta to the in-scope Gregor Samsa agent via the deliverable-12 publisher (`SET` on create/re-place, `REMOVED` on removal) | [12](./12-gregor-samsa.md), `005` | ⬜ | |

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

- `shard-size` vs. uneven `channel_partitions` distribution is an open question —
  round-robin and document the remainder behaviour.
- A `PreviewPlacement` RPC is desired but not in scope.
