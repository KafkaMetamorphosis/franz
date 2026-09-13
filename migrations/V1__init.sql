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
    -- The single home for this cluster's Kafka config (ADR-API-010): topic-config
    -- defaults + `partitions` / `replication-factor` + `kafka-version`.
    cluster_configuration  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    cluster_provider_agent text        NOT NULL DEFAULT '',
    -- Cluster shape, forwarded to the Cluster Provider agent (ADR-API-010).
    brokers                integer     CHECK (brokers IS NULL OR brokers >= 1),
    disk_size              text        NOT NULL DEFAULT '',
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
    -- Free-form metadata + reserved prefixes (003.9): franz.default-kafka-config/*
    -- (advisory config defaults the console pre-fills) and franz.placement-selector/*
    -- (agent-watch scoping). ADR-API-010 removed the structured provisioning_labels.
    labels     jsonb       NOT NULL DEFAULT '{}'::jsonb,
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

-- Async Channel — the customer-facing resource (003.4). It is abstract: no Kafka
-- config of its own. `channel_partitions` is the declared shard count; the shard
-- kafka_topic rows are created by placement (deliverable 11, ADR-API-009), not at
-- CreateAsyncChannel. `access_policy` is the embedded document (003.5), changed
-- only via SetAccessPolicy. `state` has no PENDING/ERROR — that lives on shards.
CREATE TABLE IF NOT EXISTS async_channel (
    id                 uuid        PRIMARY KEY,
    realm_id           uuid        NOT NULL REFERENCES realm (id),
    name               text        NOT NULL,
    frn                text        NOT NULL,
    type               text        NOT NULL DEFAULT 'KAFKA_TOPIC'
                           CHECK (type IN ('KAFKA_TOPIC')),
    channel_partitions integer     NOT NULL DEFAULT 1
                           CHECK (channel_partitions >= 1),
    labels             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    access_policy      jsonb       NOT NULL DEFAULT '{"statements":[]}'::jsonb,
    state              text        NOT NULL DEFAULT 'ACTIVE'
                           CHECK (state IN ('ACTIVE', 'PAUSED', 'DELETED')),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (realm_id, name),
    UNIQUE (frn)
);

CREATE INDEX IF NOT EXISTS async_channel_labels_gin
    ON async_channel USING gin (labels);

-- Kafka Topic — one shard of an Async Channel, placed on one Kafka Cluster,
-- tracking reconciliation with the real topic (003.6). Franz owns every field;
-- the only client mutation is SetConsumption. Rows are created by placement
-- (003.7, ADR-API-009), never through an API here — a row exists only for an
-- async-channel shard that already has a cluster, so `kafka_cluster_id` is set
-- on every row placement writes. `materialized_configuration` is the frozen
-- cluster⊕topic config merge — internal, not on the proto.
CREATE TABLE IF NOT EXISTS kafka_topic (
    id                        uuid        PRIMARY KEY,
    realm_id                  uuid        NOT NULL REFERENCES realm (id),
    async_channel_id          uuid        NOT NULL REFERENCES async_channel (id),
    kafka_cluster_id          uuid        REFERENCES kafka_cluster (id),
    name                      text        NOT NULL,
    frn                       text        NOT NULL,
    topic_configuration       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    materialized_configuration jsonb      NOT NULL DEFAULT '{}'::jsonb,
    partitions                integer     NOT NULL,
    replication_factor        integer     NOT NULL,
    state                     text        NOT NULL DEFAULT 'PENDING'
                                  CHECK (state IN ('PENDING', 'READY', 'PAUSED',
                                                   'ERROR', 'DELETED')),
    consumption               text        NOT NULL DEFAULT 'ENABLED'
                                  CHECK (consumption IN ('ENABLED', 'DISABLED')),
    traffic_share_value       double precision NOT NULL DEFAULT 0,
    traffic_share_unit        text        NOT NULL DEFAULT 'percent',
    generation                bigint      NOT NULL DEFAULT 1,
    -- Resource Provider reporting (005 ADR §1.5). `reconciled_generation` is the
    -- last generation an agent confirmed the real topic satisfies — NULL until
    -- the first successful report, and lagging `generation` while the shard is
    -- not converged. `last_reconcile_message` is the newest report's detail.
    reconciled_generation     bigint,
    last_reconcile_message    text        NOT NULL DEFAULT '',
    -- Placement (003.7 "Re-placement"). `misplaced` marks an async-channel shard
    -- whose cluster stopped satisfying the owning channel's affinity, went
    -- PAUSED/DELETED, or gained a `drain` taint. Franz sets the marker and moves
    -- nothing — relocation is the migration flow (003.13). `misplaced_reason` is
    -- the operator-facing why, '' while `misplaced` is false.
    misplaced                 boolean     NOT NULL DEFAULT false,
    misplaced_reason          text        NOT NULL DEFAULT '',
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now(),
    UNIQUE (realm_id, name),
    UNIQUE (frn)
);

-- The four columns above were added after kafka_topic first shipped; CREATE
-- TABLE IF NOT EXISTS is a no-op on a database that already has the table, so
-- bring an already-migrated development database forward explicitly. Idempotent,
-- like every other statement in this file.
ALTER TABLE kafka_topic
    ADD COLUMN IF NOT EXISTS reconciled_generation  bigint,
    ADD COLUMN IF NOT EXISTS last_reconcile_message text    NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS misplaced              boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS misplaced_reason       text    NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS kafka_topic_channel
    ON kafka_topic (async_channel_id);
CREATE INDEX IF NOT EXISTS kafka_topic_cluster
    ON kafka_topic (kafka_cluster_id) WHERE kafka_cluster_id IS NOT NULL;

-- Indicator samples — the append-only 30-day time series every telemetry
-- producer feeds (003.14). This is the minimal ingest table deliverable 12 needs
-- for Gregor Samsa's structural telemetry (005 ADR Part 2); deliverable 14
-- (telemetry ingest) adopts it and adds the `indicator` registry that makes
-- `indicator` a foreign key and pre-registration enforceable. Until then any
-- indicator name is accepted. Pruned nightly at 30 days, like
-- cluster_provider_event.
CREATE TABLE IF NOT EXISTS indicator_sample (
    id              uuid        PRIMARY KEY,
    realm_id        uuid        NOT NULL REFERENCES realm (id),
    indicator       text        NOT NULL,
    -- The resource the sample describes. A plain string, not an FRN foreign key:
    -- 005 ADR §2.1 also samples cluster sub-resources ("<cluster-frn>/broker/3").
    resource_frn    text        NOT NULL,
    resource_entity text        NOT NULL
                        CHECK (resource_entity IN ('ASYNC_CHANNEL', 'KAFKA_TOPIC',
                                                   'KAFKA_CLUSTER')),
    -- Encoded per the indicator's unit ("3", "true", "168Gi", ...).
    value            text        NOT NULL,
    reporting_agent  text        NOT NULL,
    sample_at        timestamptz NOT NULL,
    received_at      timestamptz NOT NULL DEFAULT now()
);

-- Serves both the history API and the "latest sample per (indicator, resource)"
-- current-value lookup governance reads (003.14).
CREATE INDEX IF NOT EXISTS indicator_sample_series
    ON indicator_sample (indicator, resource_frn, sample_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS indicator_sample_sample_at
    ON indicator_sample (sample_at);

-- Indicator registry — the pre-registered signals a Telemetry Agent publishes
-- samples for and a governance Policy watches (003.14 "Indicators are
-- pre-registered", 003.8). Registration is an admin action; there is no
-- auto-creation, so `indicator_sample.indicator` names a row here.
--
-- `applies_to` is immutable once registered (003.14). That is enforced in the
-- repository (IndicatorRepo.Mutate never writes the column) and in the domain
-- (UpdateIndicatorInput has no applies_to field) rather than by a trigger:
-- 003.12 keeps behaviour in Go, and a trigger would hide the rule from the code
-- that has to explain it to the operator.
--
-- `health` is NOT a column. It is derived on every read from `last_sample_at`
-- and `staleness_threshold` (003.14), so it can never go stale itself.
CREATE TABLE IF NOT EXISTS indicator (
    id                   uuid        PRIMARY KEY,
    realm_id             uuid        NOT NULL REFERENCES realm (id),
    name                 text        NOT NULL,
    frn                  text        NOT NULL,
    -- Free-form ("bytes", "count", "duration", "boolean", ...). Franz classifies
    -- rather than enumerates: an unrecognised unit compares numerically.
    unit                 text        NOT NULL,
    applies_to           text        NOT NULL
                             CHECK (applies_to IN ('ASYNC_CHANNEL', 'KAFKA_TOPIC',
                                                   'KAFKA_CLUSTER')),
    -- Operator-authored text ("90d", "12h", "5m"), kept verbatim so a read
    -- returns what was written; Go parses it.
    staleness_threshold  text        NOT NULL,
    source_agents        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    -- The ingest-maintained projection of the newest sample (003.14). Written by
    -- the telemetry ingest path; empty / NULL until the first sample lands.
    current_value        text        NOT NULL DEFAULT '',
    current_resource_frn text        NOT NULL DEFAULT '',
    last_sample_at       timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (realm_id, name),
    UNIQUE (frn)
);

-- Governance policies (003.8). One policy watches one indicator; when the latest
-- sample crosses `limit`, its `actions` run on every resource `matcher` selects.
-- `limit` is a reserved word, so the two halves are stored as their own columns.
CREATE TABLE IF NOT EXISTS policy (
    id              uuid        PRIMARY KEY,
    realm_id        uuid        NOT NULL REFERENCES realm (id),
    name            text        NOT NULL,
    frn             text        NOT NULL,
    -- The Indicator name this policy watches. Not a foreign key: the guard that
    -- keeps it referential is DeleteIndicator refusing while a policy names it
    -- (003.8), which gives a FAILED_PRECONDITION with an explanation rather than
    -- a constraint violation.
    indicator       text        NOT NULL,
    -- {"entity": "...", "selector": "..."} — 003.8 Matcher.
    matcher         jsonb       NOT NULL,
    limit_operator  text        NOT NULL,
    -- Encoded in the indicator's unit ("150Gi", "3", "90d").
    limit_value     text        NOT NULL,
    -- Ordered [{"kind": "...", "args": [...]}, ...] — applied front to back.
    actions         jsonb       NOT NULL,
    weight          integer     NOT NULL DEFAULT 0,
    enabled         boolean     NOT NULL DEFAULT true,
    last_fired_at   timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (realm_id, name),
    UNIQUE (frn)
);

-- The evaluation pass's work list: every enabled policy in a realm bound to the
-- indicator a sample just advanced.
CREATE INDEX IF NOT EXISTS policy_indicator
    ON policy (realm_id, indicator);

-- PolicyAction — the append-only audit of every automated change (003.8
-- "Auditability": "every automated change is a PolicyAction; there is no silent
-- mutation"). Pruned nightly at 30 days like every other Franz time series
-- (003.12 / 003.14).
--
-- `policy_id` carries NO foreign key and `policy_name` is denormalised beside
-- it: the record of what a policy did outlives the policy, so a deleted policy's
-- history stays queryable by the name the operator remembers.
CREATE TABLE IF NOT EXISTS policy_action (
    id              uuid        PRIMARY KEY,
    realm_id        uuid        NOT NULL REFERENCES realm (id),
    policy_id       uuid        NOT NULL,
    policy_name     text        NOT NULL,
    occurred_at     timestamptz NOT NULL,
    -- The resource the action changed, stored prefix-less like every other FRN.
    resource_frn    text        NOT NULL,
    -- The sample that made the policy trigger, verbatim.
    indicator_value text        NOT NULL,
    -- {"kind": "...", "args": [...]} — the single action this row records.
    action          jsonb       NOT NULL,
    -- Operator-facing outcome: the new field value, a "capped at ..." note, "no
    -- change", or the reason the action failed. A failure is a logged row, not a
    -- lost one.
    result          text        NOT NULL DEFAULT '',
    received_at     timestamptz NOT NULL DEFAULT now()
);

-- ListPolicyActions reads one policy's series newest-first with a
-- (occurred_at, id) cursor.
CREATE INDEX IF NOT EXISTS policy_action_series
    ON policy_action (realm_id, policy_name, occurred_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS policy_action_occurred_at
    ON policy_action (occurred_at);
