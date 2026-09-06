# 06 — Web console bootstrap

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex unavailable (usage limit, resets 2026-09-28)
Depends on: [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md) · [05](./05-agent-interaction-cluster-provider.md)
Specs: `001-ux/README` + `001-ux/demo/` (clickable prototype), `002-monorepo-structure/README`, `004-local-kafka-docker-agent`
Proto: consumes the REST gateway

## Goal

The minimum React console to drive Feature 1 end to end from the browser: a shell,
Login, and the **Agents** and **Kafka Clusters** screens. Nothing beyond that.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 06.1 | Vite + React + TypeScript app in `franz/webconsole/`; build wired into CI; **packaging: served separately** (decision — see below) | `002` | ✅ | 2026-09-06 |
| 06.2 | App shell (router, nav, layout) ported from `franz-console.css`; typed REST client **generated from the protos** (`buf` → OpenAPI → `openapi-typescript` + `openapi-fetch`); TanStack Query; token in memory + `Authorization` header | `001-ux/demo` | ✅ | 2026-09-06 |
| 06.3 | Login screen against the auth stub (02.10); any values sign you in | `001-ux/demo/login.html` | ✅ | 2026-09-06 |
| 06.4 | **Agents** — list, register (→ `CreateAgent`, token shown once with a copy button), detail (type, status, pause/resume/delete, rotate token) | `001-ux/demo/agents.html` | ✅ | 2026-09-06 |
| 06.5 | **Kafka Clusters** — list, register (name, provider-agent picker, bootstrap URLs, `franz.provisioning/*` fields, `cluster_configuration`), detail with intent `state` **and** provider status + event timeline (05.8) | `001-ux/demo/kafka-clusters.html` | ✅ | 2026-09-06 |
| 06.6 | Cluster detail polls `GetKafkaCluster` + the event history every 4s (TanStack Query `refetchInterval`) | ADR §4 | ✅ | 2026-09-06 |
| 06.7 | E2E smoke (Playwright) — **scoped down** (decision): sign in → register agent → copy token → register cluster → detail renders the provider-status panel + timeline ("no report yet"). The full "reaches `READY`" flow lands in deliverable 07 with the real agent. | — | ✅ | 2026-09-06 |

## Done when

- From the browser only, an operator registers a `CLUSTER_PROVIDER` agent, gets
  its token, registers a Kafka Cluster pointing at it, and watches the cluster go
  to `READY`.

## Notes

- The prototype (`001-ux/demo/`) is the visual reference; the RFC
  (`001-ux/README`) the behavioural one. Async Channel / Governance / Client
  screens come with their features.
- `register-agent.html` in the prototype still shows removed fields
  (`context_selector` etc., `UXD-007`) — build against the current `agent.proto`.

### Decisions (asked)

1. **Packaging — separate static build.** `webconsole/` builds and deploys
   independently; Franz stays API-only; Vite proxies `/v1` + `/healthz` to the
   gateway in dev; `VITE_API_BASE` points at the gateway in prod. (Not embedded
   in the Franz binary.)
2. **API client — generated from the protos.** `buf generate api` now also emits
   `api/openapi/franz.swagger.json` (grpc-gateway OpenAPI v2, committed, CI
   verifies). `webconsole` converts it 2.0 → 3.0 and runs `openapi-typescript`
   to produce `src/api/schema.d.ts` (committed, CI verifies); `openapi-fetch` is
   the ~2 kB runtime, wrapped by TanStack Query hooks.
3. **Stack — Vite + React + TS + React Router + TanStack Query**, no component
   library; `franz-console.css` ported verbatim + a small `console.css`.
4. **06.7 scoped down** — no real agent exists yet (deliverable 07), so the
   Playwright smoke stops at "cluster registered, provider-status panel renders".

### What landed

| Area | Path |
|---|---|
| App shell + routing + auth stub | `webconsole/src/{App.tsx,main.tsx,components/Shell.tsx,auth/AuthContext.tsx}` |
| Generated REST client | `webconsole/src/api/{schema.d.ts,client.ts,hooks.ts,enums.ts}`, `webconsole/scripts/gen-api.mjs` |
| Login | `webconsole/src/pages/Login.tsx` |
| Agents (list / register+token / detail+rotate) | `webconsole/src/pages/agents/` |
| Kafka Clusters (list / register / detail + 4s poll + timeline) | `webconsole/src/pages/clusters/` |
| Shared UI (Panel, StatusPill, ErrorBanner, LabelEditor, CopyButton) | `webconsole/src/components/` |
| Tests | Vitest component tests + `webconsole/e2e/console.spec.ts` (Playwright) |
| CI | `.github/workflows/ci.yml` — `webconsole` job (gen check, lint, typecheck, test, build) + `console-e2e` job (Postgres + Franz + Playwright) |
| Contract | `franz/api/openapi/franz.swagger.json` (generated, committed) |
| Local dev | `franz/Makefile` — `make dev` runs Postgres + control plane + console together (Ctrl-C stops all); `make run` / `make console` / `make gen` / `make test` / `make e2e` / `make lint` |
