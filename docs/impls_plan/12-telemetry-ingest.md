# 12 — Telemetry ingest

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md) · [13](./13-governance.md) (eval hook — mutual)
Specs: `003-franz/003.14-telemetry-ingest`, `003-franz/003.12-persistence-and-data-model`
Proto: `TelemetryService`, `GovernanceService` (`Indicator` CRUD + `ListIndicatorSamples`)

## Goal

The two inbound agent streams — indicator samples and consumer-group
observations — stored as append-only 30-day time series, plus pre-registration
of indicators and the hook that fires governance evaluation.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 12.1 | `indicator` table + `CreateIndicator` / `GetIndicator` / `ListIndicators` / `UpdateIndicator` / `DeleteIndicator` on `GovernanceService` (`applies_to` immutable) | `003.14`, proto | ⬜ | |
| 12.2 | `indicator_sample` **append** table — `(indicator, resource_frn, resource_entity, value, sample_at, received_at)`, index `(indicator, resource_frn, sample_at desc)`; nightly prune (30d) | `003.12`, `003.14` | ⬜ | |
| 12.3 | `PublishIndicatorSamples` — reject unknown indicator (`FAILED_PRECONDITION`), reject `resource_entity != applies_to`, validate `value` parses per `unit`; maintain "current" (latest per `(indicator, resource_frn)`), `last_sample_at`, `health` (`STALE` past `staleness_threshold`); an out-of-order sample is stored but is not "current" and does not trigger eval | `003.14` | ⬜ | |
| 12.4 | `observed_consumer_group` **append** table + nightly prune (30d); `ReportConsumerGroups` upserts sightings; `ListIndicatorSamples` + `ListConsumerGroupObservations` history endpoints; `ListObservedConsumerGroups` current view | `003.14` | ⬜ | |
| 12.5 | Agent-auth stub — accept any caller on the `TelemetryService` stream; leave a clearly-marked seam for the agent-auth ADR | `003.14` | ⬜ | |
| 12.6 | Ingest → eval hook — a `PublishIndicatorSamples` that changes a "current" value calls into governance evaluation (13.4) synchronously or via an in-process queue | `003.8`, `003.14` | ⬜ | |
| 12.7 | Tests — unknown-indicator rejection, staleness flip, out-of-order handling, prune job, history pagination | — | ⬜ | |

## Done when

- A sample for an unregistered indicator is rejected; registering it then lets
  samples through.
- After `staleness_threshold` with no sample, `Indicator.health` reads `STALE`
  and 13 (governance) stops acting on it.
- The prune job keeps only the last 30 days.

## Notes

- `indicator_sample` volume (indicators × resources × rate × 30d) may need daily
  partitioning or a tighter retention — `003.12` open question.
- Synchronous vs. queued ingest→eval delivery is `003.14` open — start synchronous.
