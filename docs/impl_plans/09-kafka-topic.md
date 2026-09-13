# 09 — Kafka Topic (read model)

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex out of quota (usage limit resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md) · [03](./03-kafka-cluster.md)
Specs: `003-franz/003.6-kafka-topic`, `003-franz/003.3-kafka-cluster`, `003-franz/003.12-persistence-and-data-model`
Proto: `KafkaTopicService`

## Goal

The Kafka Topic entity, its state machine, config materialisation, and the
`SetConsumption` operation. **Creation is not here** — shards are created by the
Async Channel ([10](./10-async-channel.md)); this deliverable is the read model,
the state model, and the drain operation.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 09.1 | `kafka_topic` table — `async_channel_id` FK, `kafka_cluster_id` FK **nullable** (unplaced), `topic_configuration` + `materialized_configuration` `jsonb`, `partitions int`, `replication_factor int`, `state text` + CHECK, `consumption text` + CHECK, `traffic_share_value numeric` + `traffic_share_unit text`, `generation bigint`, `frn` unique | `003.12` | ✅ | 2026-09-06 |
| 09.2 | Domain: `KafkaTopic`, `KafkaTopicState` (`PENDING` / `READY` / `PAUSED` / `ERROR` / `DELETED`), `Consumption` orthogonal to `state`, `TrafficShare` | `003.6` | ✅ | 2026-09-06 |
| 09.3 | Config materialisation: `materialized_configuration = cluster_configuration ⊕ topic_configuration`, computed and **frozen** at create/desired-state-change; a later `cluster_configuration` edit does not touch it; `partitions` / `replication_factor` are dedicated fields, seeded from cluster defaults | `003.6`, `003.3` | ✅ | 2026-09-06 |
| 09.4 | Usecases: `GetKafkaTopic`, `ListKafkaTopics` (filter by `async_channel` / `kafka_cluster`), `SetConsumption` — `DISABLED` sets `traffic_share = 0` and re-normalises the channel's other `ENABLED` shards to an equal split; `ENABLED` restores | `003.6` | ✅ | 2026-09-06 |
| 09.5 | `KafkaTopicService` handlers + REST `/v1/kafka/topics` (+ `:setConsumption`); no Create/Update/Delete RPC | proto | ✅ | 2026-09-06 |
| 09.6 | Invariant enforcement: `partitions` may only increase; `name` / `async_channel` / `kafka_cluster` immutable; `state != DELETED` for any op | `003.6` | ✅ | 2026-09-06 |
| 09.7 | Wire 03.6's cluster-delete guard to real `kafka_topic` rows | `003.3` | ✅ | 2026-09-06 |
| 09.8 | Integration tests — materialised config frozen across a cluster-config edit, consumption drain + re-normalise, partition-decrease rejected | — | ✅ | 2026-09-06 |

## Done when

- `SetConsumption(DISABLED)` on one shard drives its `traffic_share` to 0 and the
  siblings sum back to the unit.
- Editing a cluster's `cluster_configuration` leaves existing topics' materialised
  config untouched.
- Deleting a cluster with any non-deleted topic returns `FAILED_PRECONDITION`.

## Notes

- `traffic_share` is an **intended** split, not telemetry — Franz owns it.
- `generation` is the agent-facing token; nothing echoes it yet (interaction ADR).
- Automatic-retry of `ERROR` shards has no cadence yet — model the state, defer
  the loop.

### What landed

| Piece | Path |
|---|---|
| Domain (`KafkaTopic`, state machine, `Consumption`, `TrafficShare`, `Materialize`, `IncreasePartitions`, `Rematerialize`) | `pkg/franz/core/domain/topic/` |
| Driving port | `pkg/franz/core/ports/in/topic.go` |
| Driven port | `pkg/franz/core/ports/out/topic.go` (`TopicRepository` — also `CountLiveTopics` for `ClusterTopicGuard`) |
| Application service (Get, List, SetConsumption + re-normalise) | `pkg/franz/core/usecases/topics` |
| Postgres adapter (joined reads for the API's names; `MutateChannelShards` = one txn) | `pkg/franz/adapters/out/postgres/topic.go` |
| gRPC + REST handler | `pkg/franz/adapters/in/grpcgateway/kafkatopic.go` |
| Migration | `V1__init.sql` — `async_channel` **stub** (extended by 10) + `kafka_topic` |
| Wiring | `cmd/franz/main.go` — `NewTopicRepo` as `TopicRepository` **and** `ClusterTopicGuard` (replaces `stub.NoTopicGuard`); `topics.NewService`; `RegisterKafkaTopicService` |

- **No proto change** — `KafkaTopicService` + gateway already generated.
- `materialized_configuration` is stored but **not on the proto** (003.6 is
  `ready`) — it is the frozen `cluster ⊕ topic` merge for the agent path.
- The state machine is modelled + unit-tested; nothing drives a transition in 09
  (channel propagation is 10, agent reporting is the interaction ADR).
- `async_channel` is a **minimal stub** here (id/realm/name/frn) so the FK is
  real; **deliverable 10's task 10.1 is "extend `async_channel`", not "create"**.
- Verified live: `GET /v1/kafka/topics/{name}`, `?async_channel=` filter,
  `:setConsumption` (drain → share 0, siblings re-split to 50/50).
