# 16 — Migration & data movement

Status: ⛔ blocked
Depends on: [09](./09-kafka-topic.md) · [10](./10-async-channel.md) · [11](./11-placement.md)
Blocked on: `003-franz/003.13-migration-and-data-movement` open questions 1–2
Specs: `003-franz/003.13-migration-and-data-movement`, `003-franz/003.3`, `003-franz/003.4`, `003-franz/003.7`, `003-franz/003.8`

## Goal

The single staged flow that moves a shard's serving position from one cluster to
another. Once it exists it unblocks: placed-shard relocation (11.5 → real move),
cluster delete with live topics (`003.3`), re-shard execution (`003.4`
`channel_partitions` change), and governance's placement / taint / re-shard
actions (`003.8` OQ1a–c).

## Prerequisite decisions (`003.13`)

| OQ | Question | Needed before |
|---|---|---|
| 1 | RPC surface — internal-only vs. an operator `MigrateKafkaTopic` / re-shard entry point | 16.1 |
| 2 | Data-copy mechanism — drain-based only, or add replication-based (MirrorMaker2 / copy job) with offset translation | 16.3 |
| 3 | Drain-deadline policy — fixed / per-channel / lag-derived; behaviour at the deadline per reason | 16.4 |
| 5 | Cluster-delete-with-live-topics — auto-drain vs. explicit `force` vs. require a prior `drain` taint | 16.6 |

## Tasks (draft — finalise after the OQs)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 16.1 | `shard_migration` table + state machine (`PROVISIONING` → `CUTOVER` → `DRAINING` → `RETIRING` → `DONE` / `FAILED`), one per in-flight shard; idempotent, resumable | `003.13` | ⛔ | |
| 16.2 | Provision — create the target shard topic on the destination cluster; flip `kafka_topic.kafka_cluster_id` at cut-over | `003.13` | ⛔ | |
| 16.3 | Drain — hold `RETIRING` until consumer lag = 0 or the deadline; then `SetConsumption(DISABLED)` + delete the source topic after a grace period | `003.13` | ⛔ | |
| 16.4 | Triggers — `drain` taint (all shards on a cluster), misplaced shard (11.5), governance placement action, operator RPC | `003.7`, `003.8` | ⛔ | |
| 16.5 | Concurrency limits — max simultaneous migrations per cluster / fleet | `003.13` | ⛔ | |
| 16.6 | Cluster delete with live topics — resolve `003.3` OQ1 / `003.13` OQ5 and wire it | `003.3` | ⛔ | |
| 16.7 | Re-shard execution — `channel_partitions` change adds/removes shard topics + shifts routing; removed shards drain then delete | `003.4` | ⛔ | |
| 16.8 | Enable governance placement actions (`003.8` OQ1a–c) to call the flow | `003.8` | ⛔ | |
| 16.9 | Tests — resume from each phase, no data deleted before the next phase confirms, rollback before retirement | — | ⛔ | |

## Done when

- A `drain` taint on a cluster relocates every shard off it with no consumer data
  loss (bounded, drainable lag).
- Deleting a cluster with live topics behaves per the resolved OQ5 decision.
- Governance can pause/label a resource **and** now also re-place / taint / re-shard.

## Notes

- v1 is **drain-based** — no historical byte copy; a key's old messages stay on
  the old shard until consumed. Replication-based is OQ2.
- This is the last blocker in the plan; everything else (02–15) is buildable now.
