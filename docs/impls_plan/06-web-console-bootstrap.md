# 06 — Web console bootstrap

Status: ⬜ not started
Depends on: [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md) · [05](./05-agent-interaction-cluster-provider.md)
Specs: `001-ux/README` + `001-ux/demo/` (clickable prototype), `002-monorepo-structure/README`, `004-local-kafka-docker-agent`
Proto: consumes the REST gateway

## Goal

The minimum React console to drive Feature 1 end to end from the browser: a shell,
Login, and the **Agents** and **Kafka Clusters** screens. Nothing beyond that.

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 06.1 | Vite + React + TypeScript app in `franz/webconsole/`; build wired into CI; **decide packaging** — embedded in the Franz binary (`embed.FS` behind the gateway) vs. served separately | `002` | ⬜ | |
| 06.2 | App shell — router, nav, layout; port the prototype's look from `001-ux/demo/franz-console.css`; a typed API client for the REST gateway; token held in memory + `Authorization` header | `001-ux/demo` | ⬜ | |
| 06.3 | Login screen against the auth stub (02.10); no real IdP | `001-ux/demo/login.html` | ⬜ | |
| 06.4 | **Agents** — list, register (form → `CreateAgent`, **show the returned token once** with a copy button), detail (type, status, rotate token) | `001-ux/demo/agents.html`, `register-agent.html` | ⬜ | |
| 06.5 | **Kafka Clusters** — list, register (name, `cluster_provider_agent` picker, `connection_strings`, `cluster_configuration`, `franz.provisioning/*` fields), detail showing intent `state` **and** current provider status + event timeline (05.8) | `001-ux/demo/kafka-clusters.html`, `register-kafka-cluster.html` | ⬜ | |
| 06.6 | Cluster detail auto-refreshes provider status (poll `GetKafkaCluster` every few seconds) | ADR §4 | ⬜ | |
| 06.7 | E2E smoke (Playwright): register agent → copy token → register cluster → provider status reaches `READY` | — | ⬜ | |

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
