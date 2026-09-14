# `local/` — local-dev infrastructure

Everything needed to run Franz on your machine. **Not** used by CI or any
deployment.

| File | What |
|---|---|
| `docker-compose.yml` | Postgres + a one-shot `seed` service + pgAdmin |
| `seed/*.sql` | local-dev fixtures, applied after the schema, in filename order |
| `pgadmin/servers.json` | pre-registers the Franz DB connection in pgAdmin |

## Flow

`make deps` (invoked by `make run` / `make dev` / `make e2e` / `make agent-e2e`):

1. starts Postgres (`franz-postgres-1`, port 5432, volume `franz-pgdata`);
2. runs the `seed` one-shot — applies `../migrations/*.sql` then `seed/*.sql`
   with `psql`. Both are idempotent, so re-running is safe (`make seed`);
3. starts **pgAdmin** at <http://localhost:5050>.

Then Franz starts and re-applies `migrations/` on boot (also idempotent).

## pgAdmin

<http://localhost:5050> — no pgAdmin login (desktop mode). The **Franz (local)**
server is already in the tree; expand it and enter the DB password `franz` once
(tick "Save password"). Browse `Databases → franz → Schemas → public → Tables`.

## Seeded fixtures

### `seed/01-local-agent.sql`

Registers the **`local-kafka-agent`** Cluster Provider so `make agent` connects
with no console step. It installs:

- the agent row (`ACTIVE`, `CLUSTER_PROVIDER`) with its
  `franz.provisioning/*` schema (`deployment-type` / `kafka-version` /
  `kafka-image`);
- a **fixed, public** bearer token —
  `frnat_local-dev-do-not-use-in-production` — whose `sha256` is stored as
  `token_hash`. `make agent` passes it as `FRANZ_TOKEN` (override with
  `make agent TOKEN=…`).

Re-running the seed refreshes the schema / token / status (`ON CONFLICT DO
UPDATE`).

### `seed/02-local-cluster.sql`

Registers the **`local-1`** Kafka Cluster, wired to the whole local loop:
provider = `local-kafka-agent` (seed 01), `franz.placement/env=local` so it's
in scope for Gregor Samsa (seed 03), bootstrap `localhost:9092` (what the
`local-docker` recipe advertises).

### `seed/03-gregor-samsa.sql`

Registers **`gregor-samsa`**, the Resource Provider agent, with
`franz.placement-selector/env=local` — matching seed 02's coordinate so
`make gregorsamsa` has `local-1` in scope immediately. Also back-fills
`franz.placement/env=local` onto any other local cluster so one created
through the console lands in scope too. Fixed public token —
`frnat_local-dev-gregor-samsa`.

### `seed/04-indicators.sql`

Registers the 13 structural indicators Gregor Samsa's telemetry sweep
publishes (005 ADR §2.1 — `kafka.topic.*` / `kafka.cluster.*`). Deliverable 15
made pre-registration real (`PublishIndicatorSamples` now rejects an unknown
indicator), so without this seed Gregor Samsa's sweep fails every publish
against a fresh local database.

### `seed/05-clients.sql`

Registers two example Clients (003.10) — `billing` and `payments-consumer` —
so `ListClients` and the observed-consumer-group views have real rows to show
without a hand-authored `CreateClient` call first.

### `seed/06-access-policy-demo.sql`

Registers one Async Channel (`billing-events`) with a real access policy
(003.5) so the two access-policy views (`ListChannelClients` /
`ListClientChannelAccess`, deliverable 17) have something to show: a
label-selector `ALLOW` grants both seeded clients READ, and a `client_frn`
`ALLOW` additionally grants `billing` WRITE — deliberately asymmetric so the
views are worth looking at. Inserted directly as a row (bypasses placement;
no shard is materialised).

## Reset

```
make deps-reset   # docker compose down -v — drops the Postgres + pgAdmin volumes
make deps         # recreate + re-seed
```
