-- V1__init.sql — Franz schema.
--
-- Single migration, edited in place while Franz is pre-production
-- (ADR-API-005 / 003.12). Entities are added from deliverable 02 onward:
--   deliverable 02 — realm (table + seed row)
--   deliverable 03 — kafka_cluster
--   deliverable 04 — agent
--   ...
--
-- Every statement is written idempotently (IF NOT EXISTS / ON CONFLICT). Flyway
-- (docker-compose) is the migration authority; Franz also runs this file on boot
-- (db.auto_migrate, default on) so a plain `go run ./cmd/franz` against a fresh
-- database just works. Both paths are safe because the file is idempotent.

-- Realm — the tenant / FRN scope (003.1). Provisioning is out of scope; a single
-- seeded 'default' realm backs every request until API auth (003.2) carries one.
CREATE TABLE IF NOT EXISTS realm (
    id         uuid        PRIMARY KEY,
    slug       text        NOT NULL UNIQUE,
    name       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO realm (id, slug, name)
VALUES ('00000000-0000-0000-0000-000000000001', 'default', 'Default realm')
ON CONFLICT (id) DO NOTHING;

-- Kafka Cluster — a registration, not a managed resource (003.3). `frn` holds the
-- prefix-less path (003.12 / ADR-API-007). `state` is server-managed via
-- pause/resume/delete only. `(realm_id, name)` is unconditionally unique — a
-- soft-deleted row keeps its name.
CREATE TABLE IF NOT EXISTS kafka_cluster (
    id                     uuid        PRIMARY KEY,
    realm_id               uuid        NOT NULL REFERENCES realm (id),
    name                   text        NOT NULL,
    frn                    text        NOT NULL,
    connection_strings     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    labels                 jsonb       NOT NULL DEFAULT '{}'::jsonb,
    cluster_configuration  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    cluster_provider_agent text        NOT NULL DEFAULT '',
    state                  text        NOT NULL DEFAULT 'ACTIVE'
                               CHECK (state IN ('ACTIVE', 'PAUSED', 'DELETED')),
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (realm_id, name),
    UNIQUE (frn)
);

CREATE INDEX IF NOT EXISTS kafka_cluster_labels_gin
    ON kafka_cluster USING gin (labels);

-- Agent — Franz's record of an external program that connects to Franz (003.9).
-- Registration is inert. `type` is an organisational filter only. `token_hash`
-- is the sha256 of the one-time bearer token (the plaintext is never stored).
CREATE TABLE IF NOT EXISTS agent (
    id         uuid        PRIMARY KEY,
    realm_id   uuid        NOT NULL REFERENCES realm (id),
    name       text        NOT NULL,
    frn        text        NOT NULL,
    type       text        NOT NULL
                   CHECK (type IN ('CLUSTER_PROVIDER', 'RESOURCE_PROVIDER',
                                   'TELEMETRY_AGENT', 'CUSTOM')),
    labels     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- Advisory schema of the franz.provisioning/* labels this agent's recipes
    -- read (003.9, ADR-API-008). Array of {key, description, allowed_values,
    -- default_value, required}. Franz never enforces it against a resource.
    provisioning_labels jsonb NOT NULL DEFAULT '[]'::jsonb,
    status     text        NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED')),
    token_hash text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (realm_id, name),
    UNIQUE (frn)
);

CREATE INDEX IF NOT EXISTS agent_labels_gin ON agent USING gin (labels);

-- Cluster Provider status reports — append-only (004 ADR §4). "Current provider
-- status" for a cluster is the newest row. Pruned nightly at 30 days.
CREATE TABLE IF NOT EXISTS cluster_provider_event (
    id              uuid        PRIMARY KEY,
    realm_id        uuid        NOT NULL REFERENCES realm (id),
    kafka_cluster_id uuid       NOT NULL REFERENCES kafka_cluster (id),
    cluster_frn     text        NOT NULL,
    phase           text        NOT NULL
                        CHECK (phase IN ('PROVISIONING', 'READY', 'DEGRADED',
                                         'ERROR', 'STOPPED', 'REMOVED')),
    reachable       boolean     NOT NULL,
    message         text        NOT NULL DEFAULT '',
    reporting_agent text        NOT NULL,
    recipe_ref      text        NOT NULL DEFAULT '',
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    received_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cluster_provider_event_cluster_time
    ON cluster_provider_event (kafka_cluster_id, occurred_at DESC, id DESC);
