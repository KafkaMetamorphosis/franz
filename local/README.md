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

## Reset

```
make deps-reset   # docker compose down -v — drops the Postgres + pgAdmin volumes
make deps         # recreate + re-seed
```
