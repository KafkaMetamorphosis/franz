# 10 — Async Channel + access-policy document

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex out of quota (usage limit resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md) · [09](./09-kafka-topic.md)
Specs: `003-franz/003.4-async-channel`, `003-franz/003.5-access-policy`, `DECISIONS.md` ADR-API-009
Proto: `AsyncChannelService`

## Goal

The customer-facing resource. `CreateAsyncChannel` records **only the channel
row** and its `channel_partitions` count; it carries the embedded **access-policy
document** and validates it on write. The shard `kafka_topic` rows are
materialised by **placement** ([13](./13-placement.md)), not here (ADR-API-009);
re-shard execution is [18](./18-migration-and-data-movement.md).

The access-policy **engine** — principal matching, evaluation, and the
client-access views (`ListChannelClients` / `ListClientChannelAccess`) — is
**[17](./17-access-policy-and-channel-access.md)**, after Client exists. This
deliverable only owns the document's *shape* and *write validation*.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 10.1 | **Extend** the `async_channel` table (deliverable 09 created the id/realm/name/frn stub) — add `labels jsonb` (+ GIN), `access_policy jsonb` (whole document), `channel_partitions int`, `type text` + CHECK, `state text` + CHECK | `003.12` | ✅ | 2026-09-06 |
| 10.2 | Domain: `AsyncChannel`, `ChannelType` (`KAFKA_TOPIC` only), `ChannelState` (`ACTIVE ↔ PAUSED → DELETED`); shard name rule `<channel-name>-<index>`, `index` `0..channel_partitions-1` | `003.4` | ✅ | 2026-09-06 |
| 10.3 | Create usecase — a **single `async_channel` row** (`state = ACTIVE`, FRN assigned, `channel_partitions ≥ 1`, embedded `access_policy` validated via 10.6). **No shard rows** — placement ([13](./13-placement.md)) materialises them (ADR-API-009) | `003.4` | ✅ | 2026-09-06 |
| 10.4 | Usecases: Get, List (selector), Update (`labels` only — `channel_partitions` / `type` / `access_policy` **not** maskable), Delete (channel → `DELETED`, cascade to any shards that exist), Pause / Resume (channel state + propagate to any shards) | `003.4` | ✅ | 2026-09-06 |
| 10.5 | **Access-policy domain types** — `AccessPolicy`, `Statement`, `Effect` (`ALLOW` / `DENY`), `Principal` (`client_frn`, `labels` selector), `Permission` (`READ` / `WRITE`); a pure value object with no evaluation logic | `003.5`, proto | ✅ | 2026-09-06 |
| 10.6 | **Write validation** — `effect != UNSPECIFIED`, `permissions` non-empty + valid, `principal` has ≥1 of `client_frn` / `labels`; statement-level well-formedness only (no client resolution). **No statement cap** (003.5 OQ2 deferred). Table-driven unit tests | `003.5` | ✅ | 2026-09-06 |
| 10.7 | `SetAccessPolicy` — replace the document wholesale; validate every statement via 10.6; never through `UpdateAsyncChannel` | `003.4`, `003.5` | ✅ | 2026-09-06 |
| 10.8 | `AsyncChannelService` handlers + REST `/v1/async-channels` (+ `:pause` / `:resume` / `access-policy`). `ListChannelClients` returns `UNIMPLEMENTED` until [17](./17-access-policy-and-channel-access.md) | proto | ✅ | 2026-09-06 |
| 10.9 | Integration tests — create writes one channel row and **no shards**, delete/pause cascade to existing shards (test-inserted), `SetAccessPolicy` rejects `EFFECT_UNSPECIFIED` / empty-permissions / principal-less statements and stores a valid doc verbatim, `access_policy` / `channel_partitions` / `type` not maskable via `UpdateAsyncChannel` | — | ✅ | 2026-09-06 |

## Done when

- `CreateAsyncChannel{channel_partitions: 6}` writes **one** `async_channel` row
  and **zero** `kafka_topic` rows (placement makes the shards — ADR-API-009).
- `channel_partitions` / `type` / `access_policy` cannot be changed via
  `UpdateAsyncChannel`.
- Deleting or pausing a channel cascades to whatever shards exist at the time.
- `SetAccessPolicy` rejects a malformed statement (`003.5` rules) and stores a
  well-formed document verbatim.

## Notes

- `channel_partitions` changes are a staged re-shard ([18](./18-migration-and-data-movement.md))
  — this deliverable only stores the declared value.
- The shard **routing key / hash** is out of scope (future SDK ADR); Franz stores
  only `channel_partitions`.
- **Shard materialisation moved to placement** (ADR-API-009, user decision
  2026-09-06). Deliverable 13's task 13.3 now *creates* the shard `kafka_topic`
  rows (seeding `partitions` / `replication_factor` / `materialized_configuration`
  from the assigned cluster's `franz.kafka-config/*` labels — deliverable 11), not
  just sets `kafka_cluster_id`.
- Splitting the access-policy work: the **document** (types + write validation)
  is here because `SetAccessPolicy` needs it now; the **engine** (matching,
  evaluation, the two client-access views) has no exercisable consumer until
  Client exists, so it is [17](./17-access-policy-and-channel-access.md).

### What landed

| Piece | Path |
|---|---|
| Access-policy document + `Validate` | `pkg/franz/core/domain/accesspolicy/` |
| Channel domain (state machine, immutability, `ShardName`, `SetAccessPolicy`) | `pkg/franz/core/domain/channel/` |
| Ports | `pkg/franz/core/ports/{in,out}/channel.go` |
| Application service (Create=1 row, Get, List, Update, Delete/Pause/Resume cascades) | `pkg/franz/core/usecases/channels` |
| Postgres adapter (`access_policy jsonb` via local DTO; `MutateWithShards` = channel + shards one txn) | `pkg/franz/adapters/out/postgres/channel.go` |
| gRPC + REST handler (`ListChannelClients` → `codes.Unimplemented`) | `pkg/franz/adapters/in/grpcgateway/asyncchannel.go` |
| Migration | `V1__init.sql` — `async_channel` extended (type / channel_partitions / labels + GIN / access_policy / state) |
| Wiring | `cmd/franz/main.go` — `NewChannelRepo`, `channels.NewService`, `RegisterAsyncChannelService` |

- **No proto change.** No statement cap (003.5 OQ2). `persistTopicTx` extracted
  in `postgres/topic.go` and shared with `MutateWithShards`.
- Verified live: `POST /v1/async-channels` (0 shards), `:pause` cascade, mask
  rejection, `PUT …/access-policy` validation, `GET …/clients` → 501.
