# 14 — Governance (Indicator registry + non-placement actions)

Status: ✅ done
Executed by: codex → claude (claude-opus-5) — codex out of quota (resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md) · [03](./03-kafka-cluster.md) · [09](./09-kafka-topic.md) · [10](./10-async-channel.md)
Specs: `003-franz/003.8-governance`, `003-franz/003.14-telemetry-ingest` (the Indicator registry)
Proto: `GovernanceService` (Policy CRUD, `DryRunPolicy`, `ListPolicyActions`, **Indicator CRUD**)

> **Reordered 2026-09-07** — was deliverable 15. Governance and Telemetry ingest
> (now [15](./15-telemetry-ingest.md)) had a mutual dependency; splitting the
> **Indicator registry** into this deliverable breaks the cycle. Governance owns
> the `Indicator` entity (`003.8` lists Indicator CRUD on `GovernanceService`,
> and Policy write-validation needs it — `003.8` §"`indicator` must name a
> registered Indicator"); Telemetry ingest then feeds samples against it and
> triggers evaluation. So Governance ships first, self-contained; Telemetry
> ingest depends on it.

## Goal

Reactive governance: an admin registers an **Indicator**, a Policy watches it,
and when a Limit is crossed the Policy runs whitelisted Actions on matched
resources. **Placement / taint / re-shard actions wait on
[16](./16-client.md) / [18](./18-migration-and-data-movement.md)** — everything
else ships here. The event that drives evaluation is delivered by
[15](./15-telemetry-ingest.md) (ingest → eval hook); this deliverable exposes
the evaluation entry point it calls.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 14.1 | **Indicator registry** — `indicator` table (`name`, `unit`, `applies_to` entity, `staleness_threshold`, `source_agents`, current-value + `last_sample_at` + `health`, `applies_to` immutable) + `CreateIndicator` / `GetIndicator` / `ListIndicators` / `UpdateIndicator` / `DeleteIndicator` on `GovernanceService`. `health` is derived, not written. (Sample ingest that maintains the current value is [15](./15-telemetry-ingest.md).) | `003.14`, `003.8`, proto | ✅ | 2026-09-08 |
| 14.2 | `policy` table (`indicator text`, `matcher jsonb`, `limit_operator` / `limit_value text`, `actions jsonb`, `weight int`, `enabled bool`, `last_fired_at`) + `policy_action` append table (+ nightly 30-day prune, `003.12`) | `003.12` | ✅ | 2026-09-08 |
| 14.3 | Domain: `Policy`, `Matcher` (entity + selector), `Limit` (operator + string value), `Action` (kind + args); **write-whitelist validation** at Create/Update against the `003.8` matrix (reject out-of-whitelist, **unknown indicator** — must name a registered `Indicator` (14.1), `applies_to` vs `matcher.entity` mismatch, arg arity) | `003.8` | ✅ | 2026-09-08 |
| 14.4 | `GovernanceService` Policy CRUD + `DryRunPolicy` (inline defn, no mutation, no `PolicyAction`) + `ListPolicyActions` | proto | ✅ | 2026-09-08 |
| 14.5 | Event-driven evaluation entry point (invoked by [15](./15-telemetry-ingest.md) task 15.6): skip if indicator `STALE`; if `matcher.selector` matches the resource, compare the new value to `limit`; on trigger apply `actions` in order, write one `PolicyAction` each, set `last_fired_at`. Exposed as an in-process interface so 15 can wire the call | `003.8`, `003.14` | ✅ | 2026-09-08 |
| 14.6 | Non-placement actions: `ADD_LABEL` / `REMOVE_LABEL` (non-`franz.*`), `SET_STATUS` (`PAUSED` / `ACTIVE` / `DELETED` on channel + cluster), `UPDATE_FIELD` / `INCREASE_FIELD_BY` / `DECREASE_FIELD_BY` on `KafkaTopic.{partitions (↑ only), replication_factor, topic_configuration.*, consumption}` and `KafkaCluster.cluster_configuration.*` — with per-action caps | `003.8` | ✅ | 2026-09-08 |
| 14.7 | Conflict handling — when >1 triggered policy hits the same `(resource, field)`, apply in `(weight desc, name asc)` order, last write wins, log every action. **No cooldown / anti-thrash** | `003.8` | ✅ | 2026-09-08 |
| 14.8 | Tests — Indicator CRUD (`applies_to` immutable, `health` derived), whitelist rejection at write, unknown-indicator rejection, deny-on-stale, deterministic conflict order, cap enforcement, dry-run does not mutate | — | ✅ | 2026-09-08 |

## Done when

- `CreateIndicator` registers an indicator; `CreatePolicy` referencing an
  unregistered indicator is rejected.
- A policy with an out-of-whitelist action is rejected at `CreatePolicy`.
- Calling the evaluation entry point with a limit-crossing value applies the
  action and produces a `PolicyAction`; a `STALE` indicator is skipped.
- Two equal-weight policies on one field resolve deterministically by name.

## Notes

- The **current value / `health` maintenance** on an `Indicator` is done by the
  ingest path in [15](./15-telemetry-ingest.md) (15.3). This deliverable stores
  the columns and derives `health` on read; 15 writes them.
- Placement actions (`franz.affinity/*`, `franz.antiaffinity/*`, `franz.taint`,
  `channel_partitions`) are **whitelisted in the spec but not implemented here** —
  they need [16](./16-client.md) / [18](./18-migration-and-data-movement.md).
  Reject them at write for now.
- No anti-thrash is a deliberate, documented gap (`003.8` OQ2).
- Per-action cap encoding is `003.8` OQ1 — nail it in 14.3. **Resolved**: an
  optional third positional `Action.args` entry, `"max=<ceiling>"` on
  `INCREASE_FIELD_BY` and `"min=<floor>"` on `DECREASE_FIELD_BY`, bounding the
  *resulting field value* (not the per-fire delta) and **clamping** rather than
  failing. Required on `INCREASE_FIELD_BY partitions`, optional elsewhere. Needs
  no proto change. See `core/domain/governance/cap.go`; `003.8` OQ1 still needs
  the human edit that records this.

### What landed

| Piece | Path |
|---|---|
| Policy domain | `core/domain/governance/` — `policy.go` (`Policy`/`Definition`/`Matcher`/`Limit`/`Operator`/`Action`/`ActionKind` + validation), `whitelist.go` (the `003.8` `(entity,field,ops)` matrix + `ValidateAgainstWhitelist`), `cap.go` (OQ1 cap encoding), `action_record.go` |
| Indicator domain | `core/domain/indicator/registry.go` (`Indicator`, `Entity`, staleness parse, `health` derivation — STALE when never sampled), `value.go` (`Unit` + per-unit comparator: numeric / boolean / duration / byte-size) |
| Real Kafka config keys | `core/domain/topic/kafkaconfig.go` — the topic-config key set the `topic_configuration.<key>` whitelist checks against |
| Ports | `core/ports/in/governance.go` (`GovernanceService` + `GovernanceEvaluator`), `core/ports/out/governance.go` (`IndicatorRepository` / `PolicyRepository` / `PolicyActionRepository`); `ports/out/resource.go` — `IndicatorSampleRepository` +`List` / `LatestPerResource` |
| Usecases | `core/usecases/governance/` — `service.go` (CRUD + `DryRunPolicy`, placement actions rejected at write), `evaluator.go` (event-driven pass + `NoopEvaluator`), `actions.go` (non-placement action applier), `resolver.go` (FRN → resource labels over the channel/cluster/topic repos) |
| Postgres | `adapters/out/postgres/` — `indicator.go` (`IndicatorRepo` + sample `List`/`LatestPerResource`), `policy.go` (`PolicyRepo` + `ListEnabledByIndicator` / `CountByIndicator` / `MarkFired`), `policyaction.go` (append + cursor list + prune) |
| Handler | `adapters/in/grpcgateway/governance.go` — full `GovernanceService` (gRPC + REST); the `Entity` proto↔domain mapping pair moved here (telemetry ingest shares the read half) |
| Migration | `V1__init.sql` — `indicator`, `policy`, `policy_action` tables + indexes |
| Wiring | `cmd/franz/main.go` — repos + `governance.NewService` / `NewEvaluator` (evaluator provided but nothing consumes it until 15); `startPolicyActionPrune` nightly 30-day job |

### Not done (belongs to deliverable 15)

- Nothing calls the `GovernanceEvaluator` yet — deliverable 15's ingest path
  wires the call after `PublishIndicatorSamples` updates a current value.
- `Indicator.current_value` / `last_sample_at` columns exist and `health`
  derives from them, but only 15's `RecordSample` writes them.
