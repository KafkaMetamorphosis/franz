# 10 — Async Channel + access-policy wiring

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md) · [08](./08-access-policy-engine.md) · [09](./09-kafka-topic.md)
Specs: `003-franz/003.4-async-channel`, `003-franz/003.5-access-policy`
Proto: `AsyncChannelService`

## Goal

The customer-facing resource. Creating one generates its shard `kafka_topic`
rows (unplaced); it carries the embedded access policy and exposes the
client-access views. Placement of the shards is [11](./11-placement.md);
re-shard execution is [15](./15-migration-and-data-movement.md).

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 10.1 | `async_channel` table — `labels jsonb` (+ GIN), `access_policy jsonb` (whole document), `channel_partitions int`, `type text` + CHECK, `state text` + CHECK, `orn` unique, `(realm_id, name)` unique | `003.12` | ⬜ | |
| 10.2 | Domain: `AsyncChannel`, `ChannelType` (`KAFKA_TOPIC` only), `ChannelState` (`ACTIVE ↔ PAUSED → DELETED`); shard name rule `<channel-name>-<index>`, `index` `0..channel_partitions-1` | `003.4` | ⬜ | |
| 10.3 | Create usecase — one transaction: insert channel + `channel_partitions` `kafka_topic` rows (`state = PENDING`, `kafka_cluster_id = NULL`, materialised config = cluster-less defaults until placed) | `003.4` | ⬜ | |
| 10.4 | Usecases: Get, List (selector), Update (`labels` only — `channel_partitions` / `type` / `access_policy` **not** maskable), Delete (cascade: channel + all shards → `DELETED`), Pause / Resume (propagate to shards) | `003.4` | ⬜ | |
| 10.5 | `SetAccessPolicy` — replace the document wholesale; validate every statement via the 08 engine's `08.2` rules | `003.4`, `003.5` | ⬜ | |
| 10.6 | `ListChannelClients` — the forward view: evaluate the policy against every Client in the realm via the 08 engine; paginate | `003.5` | ⬜ | |
| 10.7 | Unblock `14.6` `ListClientChannelAccess` (reverse view) now that channels exist | `003.10` | ⬜ | |
| 10.8 | `AsyncChannelService` handlers + REST `/v1/async-channels` (+ `:pause` / `:resume` / `access-policy` / `clients`) | proto | ⬜ | |
| 10.9 | Integration tests — create generates N shards, delete cascades, pause propagates, `SetAccessPolicy` rejects `EFFECT_UNSPECIFIED`, forward/reverse views agree | — | ⬜ | |

## Done when

- `CreateAsyncChannel{channel_partitions: 6}` yields 6 `kafka_topic` rows named
  `<name>-0..5`, all `PENDING`/unplaced, in one transaction.
- `channel_partitions` cannot be changed via `UpdateAsyncChannel`.
- Deleting a channel soft-deletes every shard.

## Notes

- `channel_partitions` changes are a staged re-shard (13) — this deliverable only
  stores the value and generates shards at create.
- The shard **routing key / hash** is out of scope (future SDK ADR); Franz stores
  only `channel_partitions`.
