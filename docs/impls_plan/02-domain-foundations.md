# 02 — Domain foundations

Status: ⬜ not started
Depends on: [01](./01-project-scaffolding.md)
Specs: `003-franz/003.1-conventions`, `003-franz/003.12-persistence-and-data-model`

## Goal

The cross-cutting primitives every later deliverable reuses: ORN, name
validation, the label-selector grammar, pagination, error mapping, the FieldMask
helper, the `realm` bootstrap, and the Postgres plumbing. Pure `pkg/shared` /
`core/domain` plus a thin `adapters/out/postgres` base — no entity is built here.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 02.1 | `ORN` value type — parse/format `orn:<realm>:<type>:<name>`, immutable, kebab-case type set | `003.1` | ⬜ | |
| 02.2 | Resource-name validation `^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$`, 1–200 chars | `003.1` | ⬜ | |
| 02.3 | Label-selector grammar — parser for `=`, `!=`, `IN`, `NOT IN`, `key`, `!key`; matcher over a label map | `003.1` | ⬜ | |
| 02.4 | Wildcard `*` glob matcher (label values + ORN principals); `\*` literal | `003.1` | ⬜ | |
| 02.5 | Pagination — opaque `page_token` codec, default 50 / cap 1000, order-by-`name` ascending | `003.1` | ⬜ | |
| 02.6 | Error mapping — domain errors → gRPC status + `google.rpc.BadRequest` (`INVALID_ARGUMENT` / `NOT_FOUND` / `ALREADY_EXISTS` / `FAILED_PRECONDITION` / `PERMISSION_DENIED` / `RESOURCE_EXHAUSTED`) | `003.1` | ⬜ | |
| 02.7 | FieldMask apply helper — mask-gated field set, empty-mask reject, map/repeated replaced wholesale, `name` not maskable | `003.1` | ⬜ | |
| 02.8 | Postgres plumbing — `pgxpool`, `WithTx` helper, migration run on boot; hand-written query style (no ORM) | `003.12`, ADR-API-005 (D1) | ⬜ | |
| 02.9 | `Realm` entity + repo + `V1__init.sql` seed (`slug='default'`, fixed uuid); a request-context resolver returning it | `003.1`, ADR-API-005 (D3) | ⬜ | |
| 02.10 | Allow-all auth interceptor stub that populates the realm in context (seam for `003.2`) | `003.2` | ⬜ | |

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
