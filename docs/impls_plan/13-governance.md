# 13 — Governance (non-placement actions)

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md) · [03](./03-kafka-cluster.md) · [09](./09-kafka-topic.md) · [10](./10-async-channel.md) · [12](./12-telemetry-ingest.md)
Specs: `003-franz/003.8-governance`
Proto: `GovernanceService` (Policy CRUD, `DryRunPolicy`, `ListPolicyActions`)

## Goal

Reactive governance: a Policy watches an Indicator, and when a Limit is crossed
it runs whitelisted Actions on matched resources. **Placement / taint / re-shard
actions wait on [15](./15-migration-and-data-movement.md)** — everything else
ships here.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 13.1 | `policy` table (`matcher jsonb`, `limit_operator` / `limit_value text`, `actions jsonb`, `weight int`, `enabled bool`, `last_fired_at`) + `policy_action` append table | `003.12` | ⬜ | |
| 13.2 | Domain: `Policy`, `Matcher` (entity + selector), `Limit` (operator + string value), `Action` (kind + args); **write-whitelist validation** at Create/Update against the `003.8` matrix (reject out-of-whitelist, unknown indicator, `applies_to` vs `matcher.entity` mismatch, arg arity) | `003.8` | ⬜ | |
| 13.3 | `GovernanceService` Policy CRUD + `DryRunPolicy` (inline defn, no mutation, no `PolicyAction`) + `ListPolicyActions` | proto | ⬜ | |
| 13.4 | Event-driven evaluation (called from 12.6): skip if indicator `STALE`; if `matcher.selector` matches the resource, compare new value to `limit`; on trigger apply `actions` in order, write one `PolicyAction` each, set `last_fired_at` | `003.8` | ⬜ | |
| 13.5 | Non-placement actions: `ADD_LABEL` / `REMOVE_LABEL` (non-`franz.*`), `SET_STATUS` (`PAUSED` / `ACTIVE` / `DELETED` on channel + cluster), `UPDATE_FIELD` / `INCREASE_FIELD_BY` / `DECREASE_FIELD_BY` on `KafkaTopic.{partitions (↑ only), replication_factor, topic_configuration.*, consumption}` and `KafkaCluster.cluster_configuration.*` — with per-action caps | `003.8` | ⬜ | |
| 13.6 | Conflict handling — when >1 triggered policy hits the same `(resource, field)`, apply in `(weight desc, name asc)` order, last write wins, log every action. **No cooldown / anti-thrash** | `003.8` | ⬜ | |
| 13.7 | Tests — whitelist rejection at write, deny-on-stale, deterministic conflict order, cap enforcement, dry-run does not mutate | — | ⬜ | |

## Done when

- A policy with an out-of-whitelist action is rejected at `CreatePolicy`.
- A sample crossing a limit triggers the action within the ingest→eval path and
  produces a `PolicyAction`.
- Two equal-weight policies on one field resolve deterministically by name.

## Notes

- Placement actions (`franz.affinity/*`, `franz.antiaffinity/*`, `franz.taint`,
  `channel_partitions`) are **whitelisted in the spec but not implemented here** —
  they need 15. Reject them at write for now, or accept-and-queue once 15 lands.
- No anti-thrash is a deliberate, documented gap (`003.8` OQ2).
- Per-action cap encoding is `003.8` OQ1 — nail it in 13.2.
