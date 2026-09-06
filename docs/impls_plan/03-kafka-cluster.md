# 03 — Kafka Cluster

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex unavailable (usage limit, resets 2026-09-28)
Depends on: [02](./02-domain-foundations.md)
Specs: `003-franz/003.3-kafka-cluster`, `003-franz/003.12-persistence-and-data-model`
Proto: `KafkaClusterService`

## Goal

The first full `domain → ports/out → postgres → adapters/in/grpcgateway` vertical
slice. It establishes the pattern every other entity follows.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 03.1 | `kafka_cluster` table — `connection_strings` / `labels` / `cluster_configuration` `jsonb` (+ GIN on `labels`), `state text` + CHECK, `cluster_provider_agent text` (no FK), `frn` unique, `(realm_id, name)` unique **unconditionally** | `003.12` | ✅ | 2026-09-06 |
| 03.2 | Domain: `KafkaCluster`, `ConnectionString`, `KafkaClusterState` machine (`ACTIVE ↔ PAUSED → DELETED`), invariants (non-empty `connection_strings`, `name`/`frn` immutable) | `003.3` | ✅ | 2026-09-06 |
| 03.3 | `ports/out.KafkaClusterRepository` + postgres adapter — CRUD, `List` with selector + typed filters + pagination, soft-delete filter (`state != 'DELETED'` by default; `Get` still returns deleted) | `003.3`, `003.12` | ✅ | 2026-09-06 |
| 03.4 | Usecases: Create (assign FRN), Get, List, Update (FieldMask; `SELECT … FOR UPDATE`), Delete (soft), Pause, Resume | `003.3` | ✅ | 2026-09-06 |
| 03.5 | `adapters/in/grpcgateway` — `KafkaClusterService` handlers + REST `/v1/kafka/clusters` (+ `:pause` / `:resume`) | `003.3`, proto | ✅ | 2026-09-06 |
| 03.6 | Delete guard: reject with `FAILED_PRECONDITION` when the cluster still hosts non-deleted `kafka_topic` rows (no-op until 10 creates that table) | `003.3` | ✅ | 2026-09-06 |
| 03.7 | `cluster_configuration` edits do not touch existing topics (materialisation is 09's concern; nothing to do here beyond storing the map) | `003.3` | ✅ | 2026-09-06 |
| 03.8 | Integration tests — lifecycle, `FOR UPDATE` serialises concurrent updates, selector `List`, soft-deleted rows hidden from `List` but returned by `Get`, name not reusable after delete | — | ✅ | 2026-09-06 |

## Done when

- Full CRUD + pause/resume works end to end through the REST gateway with correct
  status codes and `BadRequest` details.
- A deleted cluster's `name` cannot be recreated (`ALREADY_EXISTS`).
- `cluster_provider_agent` accepts any string (no validation).

## Notes

- This slice's repo/usecase/handler shape is copied by 04, 10, 11, 15.
- `state` is only ever changed via pause/resume/delete — never in an Update mask.

### What landed

| Layer | Package |
|---|---|
| Domain entity + state machine | `pkg/franz/core/domain/cluster` |
| Driving port | `pkg/franz/core/ports/in/cluster.go` (`KafkaClusterService`, input types) |
| Driven port | `pkg/franz/core/ports/out/cluster.go` (`ClusterRepository`, `ClusterTopicGuard`) |
| Application service | `pkg/franz/core/usecases/clusters` |
| Postgres adapter | `pkg/franz/adapters/out/postgres/cluster.go` (hand-written pgx; `Mutate` = `SELECT … FOR UPDATE` in one txn) |
| gRPC + REST handler | `pkg/franz/adapters/in/grpcgateway/kafkacluster.go` (`RegisterKafkaClusterService`) |
| Delete-guard stub | `pkg/franz/adapters/out/stub` (`NoTopicGuard`, replaced in 10) |
| Migration | `migrations/V1__init.sql` — `kafka_cluster` |
| Shared | `pkg/shared/fieldmask` gained `CanonicalPaths` + `update_mask` in the immutable set |

Verified end-to-end through the REST gateway against a real Postgres: create →
FRN rendered with the configured prefix, get, selector list, pause (state +
`updated_at`), duplicate create → 409, empty `connection_strings` → 400 +
`google.rpc.BadRequest`, delete → 200.

### Assumptions (revisit if wrong)

1. **Pause / Resume are idempotent** — `Pause` on an already-PAUSED cluster (or
   `Resume` on ACTIVE) returns 200, not an error. Spec did not say.
2. **`GetKafkaCluster` returns a soft-deleted cluster** (with `state = DELETED`)
   rather than `NOT_FOUND`. Only *mutating* ops fail on a deleted cluster; a read
   shows the tombstone. Matches `003.12` ("a direct Get still returns one").
3. **`ListKafkaClustersResponse.page.total_size` is `0`** (best-effort, allowed by
   `003.1`). Not computed because the selector filter runs Go-side.
4. **Selector filtering is Go-side with a `LIMIT 5000` scan cap per page**
   (`003.12` OQ2: start Go-side). A realm with >5000 non-deleted clusters could
   miss rows on later pages — acceptable pre-scale; revisit with push-down.
5. **`ListClusterProviderEvents`** returns `Unimplemented` — it belongs to
   deliverable 05.
6. Domain `cluster.New` normalises an unset `ConnectionString.Type` to
   `PLAINTEXT`.
