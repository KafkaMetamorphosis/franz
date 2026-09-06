# Franz Web Console

The operator console for Feature 1 — register a Cluster Provider agent and stand
a Kafka cluster up from the browser. Deliverable
[`06`](../docs/impls_plan/06-web-console-bootstrap.md).

- **Vite + React + TypeScript**, React Router, TanStack Query.
- **Separate static build** — the console is not embedded in the Franz binary
  (deliverable 06.1). In dev, Vite proxies `/v1` and `/healthz` to the gateway on
  `:8080`; in prod, set `VITE_API_BASE` to the gateway URL.
- **Typed REST client generated from the protos** — `buf generate api` emits
  `../api/openapi/franz.swagger.json`; `npm run gen:api` turns it into
  `src/api/schema.d.ts` (committed, CI-verified). `openapi-fetch` is the runtime.

## Develop

```sh
# 1. backend
cd .. && docker compose up -d postgres && go run ./cmd/franz

# 2. console
cd webconsole && npm install && npm run dev      # http://localhost:5173
```

## Scripts

| Script | What |
|---|---|
| `npm run dev` | Vite dev server with the gateway proxy |
| `npm run gen:api` | regenerate `src/api/schema.d.ts` from the committed OpenAPI spec |
| `npm run typecheck` | `tsc --noEmit` |
| `npm run lint` | ESLint |
| `npm run test` | Vitest component tests (jsdom) |
| `npm run build` | typecheck + `vite build` → `dist/` |
| `npm run e2e` | Playwright smoke (needs Franz + Postgres running) |

## Scope

Login (stub), Home, **Agents** (list / register / detail + rotate token),
**Kafka Clusters** (list / register / detail with live provider status + event
timeline). Async Channel / Governance / Client screens arrive with their
features. The Playwright smoke stops at "cluster registered, provider-status
panel renders"; the full "reaches READY" flow lands in deliverable 07 with the
real agent.
