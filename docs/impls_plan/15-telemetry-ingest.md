# 15 — Telemetry ingest

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md) · [14](./14-governance.md) (Indicator registry + the evaluation entry point)
Specs: `003-franz/003.14-telemetry-ingest`, `003-franz/003.12-persistence-and-data-model`
Proto: `TelemetryService`; `GovernanceService.ListIndicatorSamples`

> **Reordered 2026-09-07** — was deliverable 14. The **Indicator registry**
> (`CreateIndicator` etc.) moved to [14](./14-governance.md) (Governance owns the
> `Indicator` entity), breaking the old mutual dependency. This deliverable now
> depends on 14: it ingests samples against 14's registered indicators and calls
> 14's evaluation entry point.

## Goal

The two inbound agent streams — indicator samples and consumer-group
observations — stored as append-only 30-day time series, the write-side that
maintains each `Indicator`'s current value / `health`, and the hook that fires
governance evaluation.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 15.1 | `indicator_sample` **append** table — `(indicator, resource_frn, resource_entity, value, sample_at, received_at)`, index `(indicator, resource_frn, sample_at desc)`; nightly prune (30d). Deliverable 12 shipped a minimal version of this table — extend/replace it and make `indicator` a foreign key to 14.1's `indicator` table | `003.12`, `003.14` | ⬜ | |
| 15.2 | `observed_consumer_group` **append** table + nightly prune (30d); `ReportConsumerGroups` upserts sightings; `ListIndicatorSamples` (on `GovernanceService`) + `ListConsumerGroupObservations` history endpoints; `ListObservedConsumerGroups` current view | `003.14` | ⬜ | |
| 15.3 | `PublishIndicatorSamples` — reject unknown indicator (`FAILED_PRECONDITION` — must be registered via 14.1), reject `resource_entity != Indicator.applies_to`, validate `value` parses per `unit`; **maintain the `Indicator`'s current value** (latest per `(indicator, resource_frn)`), `last_sample_at`, and `health` (`STALE` past `staleness_threshold`); an out-of-order sample is stored but is not "current" and does not trigger eval | `003.14` | ⬜ | |
| 15.4 | Widen `agentauth.go` was done in 12 — confirm the `TelemetryService` stream still accepts a `RESOURCE_PROVIDER` / `TELEMETRY_AGENT` bearer token; the finer per-agent-type policy is the agent-auth ADR (leave the seam) | `003.14` | ⬜ | |
| 15.5 | Adopt the deliverable-12 `StreamIndicatorSamples` client stream into this ingest path (12 stubbed a minimal append; route it through 15.3's validation + current-value maintenance) | `005` §2.2, `003.14` | ⬜ | |
| 15.6 | **Ingest → eval hook** — a `PublishIndicatorSamples` / `StreamIndicatorSamples` batch that changes an `Indicator`'s current value calls [14](./14-governance.md)'s evaluation entry point (14.5), synchronously to start (`003.14` OQ) | `003.8`, `003.14` | ⬜ | |
| 15.7 | Tests — unknown-indicator rejection, `resource_entity` mismatch, `value` parse failure, staleness flip, out-of-order handling, prune job, history pagination, eval hook fires only on a current-value change | — | ⬜ | |

## Done when

- A sample for an unregistered indicator is rejected; registering it (14.1) then
  lets samples through.
- After `staleness_threshold` with no sample, `Indicator.health` reads `STALE`
  and [14](./14-governance.md) stops acting on it.
- A current-value-changing sample calls the governance evaluation entry point;
  an out-of-order sample does not.
- The prune job keeps only the last 30 days.

## Notes

- `indicator_sample` volume (indicators × resources × rate × 30d) may need daily
  partitioning or a tighter retention — `003.12` open question.
- Synchronous vs. queued ingest→eval delivery is `003.14` open — start synchronous.
- Deliverable 12 already added a minimal `indicator_sample` table + nightly prune
  and the `StreamIndicatorSamples` RPC; this deliverable is where they become
  real (FK to `indicator`, validation, current-value maintenance, eval trigger).
