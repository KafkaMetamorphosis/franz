CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE clusters (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name           TEXT NOT NULL UNIQUE,
    bootstrap_url  TEXT NOT NULL,
    labels         JSONB NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_clusters_name ON clusters (name);

CREATE TABLE topic_definitions (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL UNIQUE,
    partitions         INTEGER NOT NULL CHECK (partitions > 0),
    replication_factor INTEGER NOT NULL CHECK (replication_factor > 0),
    retention_ms       BIGINT,
    configs            JSONB NOT NULL DEFAULT '{}',
    labels             JSONB NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_topic_definitions_name ON topic_definitions (name);

CREATE TABLE topic_claims (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_id           UUID NOT NULL REFERENCES topic_definitions(id) ON DELETE CASCADE,
    cluster_id         UUID NOT NULL REFERENCES clusters(id) ON DELETE RESTRICT,
    status             TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'synced', 'error', 'deleting')),
    error_message      TEXT,
    last_reconciled_at TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_id, cluster_id)
);

CREATE INDEX idx_topic_claims_topic_id   ON topic_claims (topic_id);
CREATE INDEX idx_topic_claims_status     ON topic_claims (status);
CREATE INDEX idx_topic_claims_cluster_id ON topic_claims (cluster_id);
