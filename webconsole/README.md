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
cd ..        # franz/ (module root)
make dev     # Postgres + control plane (:8080) + console (:5173); Ctrl-C stops all
```

Or run the pieces separately: `make run` (control plane only), `make console`
(console only, expects the gateway on :8080). `make help` lists every target.

To actually see a cluster reach `READY`: run `make agent` (needs Docker). With no
`TOKEN=` it self-registers with Franz as `local-kafka-agent` (creating the agent
if absent, rotating its token if it already exists) — no console step needed.
Then register a Kafka Cluster with
`franz.provisioning/deployment-type=local-docker` and
`cluster_provider_agent=local-kafka-agent` — a broker comes up in Docker and the
cluster detail page turns green.

Pass `make agent TOKEN=<token> AGENT_NAME=<agent>` to use a token you minted in
the console instead of self-registering. Self-register is local-dev only: Franz's
`AgentService` is unauthenticated there.

On registration the agent advertises its provisioning-label schema
(`deployment-type` / `kafka-version` / `kafka-image`), so the cluster form
pre-fills and constrains those fields once you pick the agent. Set
`franz.provisioning/kafka-image` on a cluster to pin a full image ref
(e.g. `apache/kafka:3.9.0` or a registry mirror) instead of only a version.

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
