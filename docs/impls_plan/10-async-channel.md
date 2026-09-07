# 10 — Async Channel + access-policy document

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md) · [09](./09-kafka-topic.md)
Specs: `003-franz/003.4-async-channel`, `003-franz/003.5-access-policy`
Proto: `AsyncChannelService`

## Goal

The customer-facing resource. Creating one generates its shard `kafka_topic`
rows (unplaced); it carries the embedded **access-policy document** and validates
it on write. Placement of the shards is [11](./11-placement.md); re-shard
execution is [16](./16-migration-and-data-movement.md).

The access-policy **engine** — principal matching, evaluation, and the
client-access views (`ListChannelClients` / `ListClientChannelAccess`) — is
**[15](./15-access-policy-and-channel-access.md)**, after Client exists. This
deliverable only owns the document's *shape* and *write validation*.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 10.1 | `async_channel` table — `labels jsonb` (+ GIN), `access_policy jsonb` (whole document), `channel_partitions int`, `type text` + CHECK, `state text` + CHECK, `frn` unique, `(realm_id, name)` unique | `003.12` | ⬜ | |
| 10.2 | Domain: `AsyncChannel`, `ChannelType` (`KAFKA_TOPIC` only), `ChannelState` (`ACTIVE ↔ PAUSED → DELETED`); shard name rule `<channel-name>-<index>`, `index` `0..channel_partitions-1` | `003.4` | ⬜ | |
| 10.3 | Create usecase — one transaction: insert channel + `channel_partitions` `kafka_topic` rows (`state = PENDING`, `kafka_cluster_id = NULL`, materialised config = cluster-less defaults until placed) | `003.4` | ⬜ | |
| 10.4 | Usecases: Get, List (selector), Update (`labels` only — `channel_partitions` / `type` / `access_policy` **not** maskable), Delete (cascade: channel + all shards → `DELETED`), Pause / Resume (propagate to shards) | `003.4` | ⬜ | |
| 10.5 | **Access-policy domain types** — `AccessPolicy`, `Statement`, `Effect` (`ALLOW` / `DENY`), `Principal` (`client_frn`, `labels` selector), `Permission` (`READ` / `WRITE`); a pure value object with no evaluation logic | `003.5`, proto | ⬜ | |
| 10.6 | **Write validation** — `effect != UNSPECIFIED`, `permissions` non-empty, `principal` has ≥1 of `client_frn` / `labels`; statement-level well-formedness only (no client resolution). Table-driven unit tests | `003.5` | ⬜ | |
| 10.7 | `SetAccessPolicy` — replace the document wholesale; validate every statement via 10.6; never through `UpdateAsyncChannel` | `003.4`, `003.5` | ⬜ | |
| 10.8 | `AsyncChannelService` handlers + REST `/v1/async-channels` (+ `:pause` / `:resume` / `access-policy`). `ListChannelClients` returns `UNIMPLEMENTED` until [15](./15-access-policy-and-channel-access.md) | proto | ⬜ | |
| 10.9 | Integration tests — create generates N shards, delete cascades, pause propagates, `SetAccessPolicy` rejects `EFFECT_UNSPECIFIED` / empty-permissions / principal-less statements, `access_policy` not maskable via `UpdateAsyncChannel` | — | ⬜ | |

## Done when

- `CreateAsyncChannel{channel_partitions: 6}` yields 6 `kafka_topic` rows named
  `<name>-0..5`, all `PENDING`/unplaced, in one transaction.
- `channel_partitions` cannot be changed via `UpdateAsyncChannel`.
- Deleting a channel soft-deletes every shard.
- `SetAccessPolicy` rejects a malformed statement (`003.5` rules) and stores a
  well-formed document verbatim.

## Notes

- `channel_partitions` changes are a staged re-shard (13) — this deliverable only
  stores the value and generates shards at create.
- The shard **routing key / hash** is out of scope (future SDK ADR); Franz stores
  only `channel_partitions`.
- Splitting the access-policy work: the **document** (types + write validation)
  is here because `SetAccessPolicy` needs it now; the **engine** (matching,
  evaluation, the two client-access views) has no exercisable consumer until
  Client exists, so it is [15](./15-access-policy-and-channel-access.md).
