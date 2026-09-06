# 11 — Placement & selection

Status: ⬜ not started
Depends on: [03](./03-kafka-cluster.md) · [10](./10-async-channel.md)
Specs: `003-franz/003.7-placement-and-selection`, `003-franz/003.1-conventions`

## Goal

Decide which cluster each shard lives on. Deterministic selection over cluster
rows, a retry sweep for unplaced shards, and detection (not yet movement) of
shards whose cluster stopped matching.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.1 | Selection algorithm as pure domain logic: candidates (`state = ACTIVE` ∧ labels satisfy `franz.affinity/selector`) → drop `franz.antiaffinity/selector` matches → drop `drain`-tainted and untolerated `no-creation`-tainted → order by `(franz.affinity/weight` desc`, name` asc`)` → take `min(shard-size, |candidates|)` → round-robin the shards | `003.7` | ⬜ | |
| 11.2 | Reserved-label parsing/handling: `franz.affinity/selector`, `franz.antiaffinity/selector`, `franz.affinity/shard-size`, `franz.affinity/weight`, `franz.taint` (`<name>:<effect>`), `franz.taint/toleration` (comma-separated) | `003.1`, `003.7` | ⬜ | |
| 11.3 | Placement on channel create/update (`franz.*` label change) and on cluster label/`state` change — write `kafka_topic.kafka_cluster_id` | `003.7` | ⬜ | |
| 11.4 | Retry sweep (~30s, configurable) — re-run selection for every `kafka_cluster_id IS NULL` shard; place when a cluster becomes eligible | `003.7` | ⬜ | |
| 11.5 | Misplaced detection — a placed shard whose cluster no longer satisfies the channel's affinity (or went `PAUSED`/`DELETED`) gets a `misplaced` marker on the row; **no move** (that is 13) | `003.7` | ⬜ | |
| 11.6 | Absent `franz.affinity/selector` ⇒ no candidates ⇒ shards stay unplaced (do not reject create) | `003.7` | ⬜ | |
| 11.7 | Unit + integration tests — determinism (same labels + clusters ⇒ same assignment), taint filtering, unplaced → placed on cluster registration, `shard-size` capping | — | ⬜ | |

## Done when

- Two runs with identical channel labels + cluster set produce byte-identical
  shard→cluster assignments.
- Registering a matching cluster causes previously-unplaced shards to be placed
  within one sweep interval.
- Re-labelling a cluster so a placed shard mismatches sets `misplaced` and moves
  nothing.

## Notes

- `shard-size` vs. uneven `channel_partitions` distribution is an open question —
  round-robin and document the remainder behaviour.
- A `PreviewPlacement` RPC is desired but not in scope.
