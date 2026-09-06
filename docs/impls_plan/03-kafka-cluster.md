# 03 — Kafka Cluster

Status: ⬜ not started
Depends on: [02](./02-domain-foundations.md)
Specs: `003-franz/003.3-kafka-cluster`, `003-franz/003.12-persistence-and-data-model`
Proto: `KafkaClusterService`

## Goal

The first full `domain → ports/out → postgres → adapters/in/grpcgateway` vertical
slice. It establishes the pattern every other entity follows.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 03.1 | `kafka_cluster` table — `connection_strings` / `labels` / `cluster_configuration` `jsonb` (+ GIN on `labels`), `state text` + CHECK, `cluster_provider_agent text` (no FK), `frn` unique, `(realm_id, name)` unique **unconditionally** | `003.12` | ⬜ | |
| 03.2 | Domain: `KafkaCluster`, `ConnectionString`, `KafkaClusterState` machine (`ACTIVE ↔ PAUSED → DELETED`), invariants (non-empty `connection_strings`, `name`/`frn` immutable) | `003.3` | ⬜ | |
| 03.3 | `ports/out.KafkaClusterRepository` + postgres adapter — CRUD, `List` with selector + typed filters + pagination, soft-delete filter (`state != 'DELETED'` by default; `Get` still returns deleted) | `003.3`, `003.12` | ⬜ | |
| 03.4 | Usecases: Create (assign FRN), Get, List, Update (FieldMask; `SELECT … FOR UPDATE`), Delete (soft), Pause, Resume | `003.3` | ⬜ | |
| 03.5 | `adapters/in/grpcgateway` — `KafkaClusterService` handlers + REST `/v1/kafka/clusters` (+ `:pause` / `:resume`) | `003.3`, proto | ⬜ | |
| 03.6 | Delete guard: reject with `FAILED_PRECONDITION` when the cluster still hosts non-deleted `kafka_topic` rows (no-op until 09 creates that table) | `003.3` | ⬜ | |
| 03.7 | `cluster_configuration` edits do not touch existing topics (materialisation is 08's concern; nothing to do here beyond storing the map) | `003.3` | ⬜ | |
| 03.8 | Integration tests — lifecycle, `FOR UPDATE` serialises concurrent updates, selector `List`, soft-deleted rows hidden from `List` but returned by `Get`, name not reusable after delete | — | ⬜ | |

## Done when

- Full CRUD + pause/resume works end to end through the REST gateway with correct
  status codes and `BadRequest` details.
- A deleted cluster's `name` cannot be recreated (`ALREADY_EXISTS`).
- `cluster_provider_agent` accepts any string (no validation).

## Notes

- This slice's repo/usecase/handler shape is copied by 04, 09, 10, 14.
- `state` is only ever changed via pause/resume/delete — never in an Update mask.
