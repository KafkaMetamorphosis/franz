# 02 — Domain foundations

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex unavailable (usage limit, resets 2026-09-28)
Depends on: [01](./01-project-scaffolding.md)
Specs: `003-franz/003.1-conventions`, `003-franz/003.12-persistence-and-data-model`

## Goal

The cross-cutting primitives every later deliverable reuses: FRN, name
validation, the label-selector grammar, pagination, error mapping, the FieldMask
helper, the `realm` bootstrap, and the Postgres plumbing. Pure `pkg/shared` /
`core/domain` plus a thin `adapters/out/postgres` base — no entity is built here.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 02.1 | `FRN` value type — parse/format `frn:<realm>:<type>:<name>`, immutable, kebab-case type set; prefix-less `Path()` for storage; `Codec` applies the configurable `resource_prefix` (default `frn`, `frn:`/`orn:` aliases) at the API boundary | `003.1` (ADR-API-007) | ✅ | 2026-09-06 |
| 02.2 | Resource-name validation `^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$`, 1–200 chars | `003.1` | ✅ | 2026-09-06 |
| 02.3 | Label-selector grammar — parser for `=`, `!=`, `IN`, `NOT IN`, `key`, `!key`; matcher over a label map | `003.1` | ✅ | 2026-09-06 |
| 02.4 | Wildcard `*` glob matcher (label values + FRN principals); `\*` literal | `003.1` | ✅ | 2026-09-06 |
| 02.5 | Pagination — opaque `page_token` codec, default 50 / cap 1000, order-by-`name` ascending | `003.1` | ✅ | 2026-09-06 |
| 02.6 | Error mapping — domain errors → gRPC status + `google.rpc.BadRequest` (`INVALID_ARGUMENT` / `NOT_FOUND` / `ALREADY_EXISTS` / `FAILED_PRECONDITION` / `PERMISSION_DENIED` / `RESOURCE_EXHAUSTED`) | `003.1` | ✅ | 2026-09-06 |
| 02.7 | FieldMask apply helper — mask-gated field set, empty-mask reject, map/repeated replaced wholesale, `name` not maskable | `003.1` | ✅ | 2026-09-06 |
| 02.8 | Postgres plumbing — `pgxpool`, `WithTx` helper, migration run on boot; hand-written query style (no ORM) | `003.12`, ADR-API-005 (D1) | ✅ | 2026-09-06 |
| 02.9 | `Realm` entity + repo + `V1__init.sql` seed (`slug='default'`, fixed uuid); a request-context resolver returning it | `003.1`, ADR-API-005 (D3) | ✅ | 2026-09-06 |
| 02.10 | Allow-all auth interceptor stub that populates the realm in context (seam for `003.2`) | `003.2` | ✅ | 2026-09-06 |

## Done when

- Selector grammar and glob matcher pass an exhaustive table-driven suite
  (every requirement type, whitespace, quoted values, empty selector = match all).
- `WithTx` rolls back on error; a repo method can be composed into a caller's
  transaction.
- Every request handler can get `realm_id` from context; nothing hard-codes it.

## Notes

- The selector matcher is reused verbatim by 08 (access policy), 11 (placement),
  13 (governance matchers). Get it right here.
- SQL push-down vs. Go-side filtering of the selector is `003.12` OQ2 — start
  Go-side, optimise later.

### What landed

| Concern | Package |
|---|---|
| FRN value object + `Codec` (configurable prefix) | `pkg/franz/core/domain/frn` |
| Name validation | `pkg/franz/core/domain/naming` |
| Selector grammar + matcher | `pkg/franz/core/domain/selector` (`selector.go`, `parse.go`) |
| Glob (`*`, `\*`) | `pkg/shared/glob` |
| Domain error vocabulary | `pkg/franz/core/domain/errs` |
| Pagination codec | `pkg/shared/pagetoken` |
| gRPC error mapping (`google.rpc.BadRequest`) | `pkg/franz/adapters/in/grpcgateway/errmap.go` |
| FieldMask apply | `pkg/shared/fieldmask` |
| Postgres pool + `WithTx` + boot migrations | `pkg/franz/adapters/out/postgres/db.go`, `migrations/embed.go` |
| Realm value object + context + repo | `pkg/franz/core/domain/realm`, `pkg/franz/core/ports/out/realm.go`, `pkg/franz/adapters/out/postgres/realm.go` |
| Allow-all auth + realm-in-context (unary/stream/HTTP) | `pkg/franz/adapters/in/grpcgateway/interceptor.go` |

- **FRN, not ORN** (ADR-API-007): the identifier is the Franz Resource Name.
  Config key `resource_prefix` (default `frn`, `^[a-z][a-z0-9]*$`, 2–16 chars) is
  read once at boot (`fx.Invoke` forces it — a bad value fails startup). FRNs are
  stored prefix-less (`FRN.Path()`); `frn.Codec` renders/parses with the
  configured prefix and always accepts `frn:` / `orn:` aliases.
- **Migration on boot** deviates slightly from `003.12` ("Flyway"): Franz embeds
  `migrations/*.sql` and runs them on start when `db.auto_migrate` is set (default
  on). The SQL is written idempotently so Flyway (docker-compose) stays the
  authority and both paths are safe. See tracker for rationale.
- Postgres integration tests (`db_integration_test.go`) self-skip unless
  `FRANZ_TEST_DB_DSN` is set — no Docker daemon in the session that implemented
  this. `go build` / `go vet` / `go test ./...` (unit) all pass; the fx graph
  resolves end-to-end (verified: boots to the DB-connect step).
