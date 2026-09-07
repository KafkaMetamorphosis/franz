# Franz

**Franz** is the control plane of **KafkaMetamorphosis** — a system for running
Kafka *as a fleet*. You declare intent (clusters, topics, channels, access
policy, provisioning hints) against Franz's API; agents outside Franz watch that
intent and make it real. Franz never connects to a Kafka broker itself.

> The name is a nod to Kafka. The sibling services are **Gregor Samsa** (topic
> reconciliation) and **Odradek** (SLO/telemetry).

## What's here today

Franz is built one deliverable at a time (`docs/impls_plan/`). Shipped so far:

| Area | What works |
|---|---|
| **Kafka Cluster** registry | full CRUD + pause/resume, hand-written `pgx`, `SELECT … FOR UPDATE` |
| **Agent** registry | CRUD + pause/resume, one-time bearer tokens (stored hashed), rotate; advisory `franz.default-kafka-config/*` label defaults the console pre-fills |
| **Cluster Provider protocol** | `WatchClusterAssignments` server-stream + `ReportClusterStatus`; an append-only `cluster_provider_event` log surfaced as `KafkaCluster.provider_status` |
| **local-kafka-docker-agent** | a Cluster Provider agent that turns a Kafka Cluster registration into a running `apache/kafka` KRaft container in local Docker, keeps it converged, and reports health |
| **Web console** | Vite + React + TS operator UI: register + **edit** Agents and Kafka Clusters, watch a cluster go `READY` |

**Feature 1** — "register an agent, register a cluster pointing at it, watch a
Kafka broker come up in Docker on your machine" — is end-to-end complete.

Async Channel, Access Policy, Placement, Telemetry, Governance, Client and
Migration are planned (`docs/impls_plan/README.md`).

## Stack

- **Go** (`go 1.25`), hexagonal layout under `pkg/franz/` — `core/{domain,usecases,ports}` + `adapters/{in/grpcgateway,out/postgres,…}`
- **protobuf** (edition 2024, opaque API) is the authoritative contract; `buf generate` emits Go stubs, an OpenAPI spec, and the console's typed client
- **gRPC** (`:9090`) + **grpc-gateway REST** (`:8080`) from the same service definitions
- **PostgreSQL**, no ORM — hand-written queries, embedded idempotent migrations applied on boot
- **`go.uber.org/fx`** for wiring
- **webconsole/** — a separate static build (React + TanStack Query + `openapi-fetch`)

## Run it locally

**Prerequisites:** Go 1.25, Docker (Compose v2), Node 20. `make gen` also needs
[`buf`](https://buf.build).

```sh
make dev
```

That brings up, wired together (Ctrl-C stops all):

| | |
|---|---|
| Postgres | `localhost:5432` — started, migrated, and **seeded** (see `local/`) |
| pgAdmin | `http://localhost:5050` — browse the schema (Franz DB pre-registered) |
| Control plane | REST `http://localhost:8080` · gRPC `:9090` |
| Web console | `http://localhost:5173` |

Sign in to the console with any values (auth is a stub until the auth model
lands).

### See a Kafka broker come up

`make deps` seeds a whole local loop — no console step:

- `local-kafka-agent` — a Cluster Provider agent (`local/seed/01-local-agent.sql`)
- `local-1` — a Kafka Cluster wired to it, bootstrap `localhost:9092`,
  `franz.placement/env=local` (`local/seed/02-local-cluster.sql`)
- `gregor-samsa` — a Resource Provider agent scoped to that cluster
  (`local/seed/03-gregor-samsa.sql`)

In separate terminals:

```sh
make agent        # local-kafka-docker-agent — brings local-1's broker up in Docker
make gregorsamsa  # Gregor Samsa — reconciles async-channel partitions on local-1
```

`make agent` brings an `apache/kafka` container up for `local-1` and reports
`PROVISIONING → READY`; the cluster detail page turns green. Connect any Kafka
client at `localhost:9092`.

`make gregorsamsa` connects and holds `local-1` in scope, but has **no work**
until [placement (deliverable 13)](docs/impls_plan/13-placement.md) materialises
shard rows — creating an Async Channel does not yet create them. Use
`make gregorsamsa-e2e` to exercise it end-to-end today.

To register another cluster from the console: **Kafka Clusters → Register** —
name, bootstrap URL, pick `local-kafka-agent` as the provider (the **Cluster
configuration** section pre-fills from its `franz.default-kafka-config/*`
labels). Re-run `make seed` afterward, or add `franz.placement/env=local` in
the console, to bring it into Gregor Samsa's scope too.

### Other targets

```sh
make run          # just the control plane
make console      # just the web console (expects the gateway on :8080)
make gen          # regenerate protobuf stubs + OpenAPI + the console client
make test         # Go + console unit tests
make e2e          # Playwright console smoke against a live stack
make agent-e2e    # real-Docker Cluster Provider smoke (opt-in, local only)
make gregorsamsa-e2e  # real-Docker Resource Provider smoke (opt-in, local only)
make lint         # gofmt + go vet + console lint/typecheck
make seed         # re-run the local/seed/*.sql scripts (idempotent)
make deps-reset   # drop the Postgres volume (next `make deps` re-seeds)
make help         # every target
```

Config is `config.yaml`, overlaid by `FRANZ_`-prefixed env vars
(`FRANZ_HTTP_PORT`, `FRANZ_DB__PASSWORD`, …).

## Layout

```
api/franz/v1/       protobuf service + message definitions (the contract)
cmd/franz/          control-plane entrypoint (fx)
cmd/localkafkaagent/   the Cluster Provider agent
pkg/franz/          control-plane code (hexagonal)
pkg/localkafkaagent/  the agent (deliberately simple — plain packages, no fx)
pkg/shared/         cross-cutting helpers (frn, token, pagetoken, fieldmask, …)
pkg/gen/go/         generated protobuf Go (do not edit)
migrations/         SQL schema (idempotent; applied on boot)
webconsole/         React operator console
local/              local-dev docker-compose (Postgres + seed + pgAdmin)
docs/impls_plan/    the deliverable-by-deliverable build plan
docs/impls_tracker/ per-deliverable decision + verification records
```

Design docs and ADRs live in the sibling
[`KafkaMetamorphosis/docs`](https://github.com/KafkaMetamorphosis/docs) repo
(`003-franz/`, `004-local-kafka-docker-agent/`, `DECISIONS.md`).

## Author

**José Roniérison** — [linkedin.com/in/joseronierison](https://www.linkedin.com/in/joseronierison/)

## License

[Apache License 2.0](./LICENSE)
