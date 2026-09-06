# 01 — Project scaffolding

Status: ✅ done
Executed by: claude + user (repo/branch) → codex (blocked: sandbox network + usage limit) → claude (claude-sonnet-5)
Landed: 2026-09-06 · branch `go-monorepo`
Depends on: —
Specs: `002-monorepo-structure/README`, `DECISIONS.md` (ADR-API-001, ADR-API-005)
Tracker: `../../../docs/impls_tracker/01-project-scaffolding.md`

## Goal

An empty but running Franz: the Go module, the hexagonal directory skeleton,
`buf` codegen, a Postgres + Flyway dev environment, an `fx` app that boots the
gRPC server and REST gateway with a health check, and CI that guards all of it.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 01.1 | Repo on `go-monorepo` (off `main`); Clojure tree `git rm`-ed; `.gitignore` (Go/buf/IDE, `pkg/gen/go` **not** ignored); module rooted at `franz/` (ADR-002 updated) | — | ✅ | go-monorepo · 2026-09-06 |
| 01.2 | Go module `github.com/KafkaMetamorphosis/franz`; hexagonal skeleton (`cmd/franz`, `pkg/franz/core/{domain,usecases,ports/{in,out}}`, `pkg/franz/adapters/{in/grpcgateway,out/postgres}`, `pkg/franz/config`, `pkg/shared`, `pkg/gen/go`, `migrations`) — `doc.go` in the empty core pkgs | `002` | ✅ | go-monorepo · 2026-09-06 |
| 01.3 | `api/buf.yaml` + `api/buf.lock`; `buf.gen.yaml` at `franz/` root; **`use_opaque_api=true`** on the gateway plugin (edition 2024 → Opaque API); `buf generate api` → committed `pkg/gen/go` | `002`, ADR-API-001 | ✅ | go-monorepo · 2026-09-06 |
| 01.4 | `docker-compose.yml` — Postgres 16 + a one-shot Flyway `migrate`; empty `migrations/V1__init.sql` (header only) | `002`, `003.12` | ✅ | go-monorepo · 2026-09-06 |
| 01.5 | `cmd/franz/main.go` — `fx.App` assembling config + gRPC server + grpc-gateway mux + `GET /healthz`; graceful shutdown via lifecycle hooks; verified boot + `/healthz` → 200 | `002` | ✅ | go-monorepo · 2026-09-06 |
| 01.6 | `pkg/franz/config` — `config.yaml` dev defaults + `FRANZ_`-prefixed env overrides via `koanf` (`FRANZ_DB__PASSWORD` etc.), provided through `fx`; unit-tested | ADR-API-005 (D4) | ✅ | go-monorepo · 2026-09-06 |
| 01.7 | `.github/workflows/ci.yml` — `buf lint` / `buf breaking` vs `main`; `go vet` / `build` / `test`; generated-code-is-current check; Postgres service | `002` | ✅ | go-monorepo · 2026-09-06 |
| 01.8 | `CHANGELOG.md` (Keep a Changelog, `[Unreleased]`) | conv. | ✅ | go-monorepo · 2026-09-06 |

## Done when

- ✅ `go run ./cmd/franz` boots via `fx` and answers `GET /healthz` → `200 {"status":"ok"}`.
- ✅ `buf lint` clean; `buf generate api` reproduces `pkg/gen/go` with no diff.
- ✅ Dependency rule holds — `go list -deps ./pkg/franz/core/...` pulls in no
  adapter / transport / `fx` / `pgx` / gRPC package.
- ✅ `go vet ./...`, `go test ./...` pass (config + gateway packages tested).

## Notes

- No entities yet — `V1__init.sql` is a header comment only.
- **Go 1.25** — `go mod tidy` set `go 1.25.0` (transitive gRPC requirement); CI
  pins `1.25`.
- **Edition-2024 + grpc-gateway** needs `use_opaque_api=true` on the gateway
  plugin — otherwise the generated `.pb.gw.go` uses Open-Struct field access that
  won't compile against the Opaque messages protoc-gen-go emits for editions.
  See the tracker and `DECISIONS.md` ADR-API-001.
