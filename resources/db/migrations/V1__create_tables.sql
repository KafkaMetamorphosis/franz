CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE topic_configurations (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL UNIQUE,
    partitions         INTEGER NOT NULL CHECK (partitions > 0),
    replication_factor INTEGER NOT NULL CHECK (replication_factor > 0),
    retention_ms       BIGINT NOT NULL,
    configs            JSONB NOT NULL DEFAULT '{}',
    labels             JSONB NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_topic_configurations_name ON topic_configurations (name);

CREATE TABLE clusters (
    id                             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                           TEXT NOT NULL UNIQUE,
    bootstrap_url                  TEXT NOT NULL,
    default_topic_configuration_id UUID NOT NULL REFERENCES topic_configurations(id),
    labels                         JSONB NOT NULL DEFAULT '{}',
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_clusters_name ON clusters (name);

CREATE TABLE topic_definitions (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_name               TEXT NOT NULL UNIQUE,
    topic_configuration_id   UUID NOT NULL REFERENCES topic_configurations(id),
    labels                   JSONB NOT NULL DEFAULT '{}',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_topic_definitions_topic_name ON topic_definitions (topic_name);

CREATE TABLE topic_claims (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_definition_id      UUID NOT NULL REFERENCES topic_definitions(id) ON DELETE CASCADE,
    cluster_id               UUID NOT NULL REFERENCES clusters(id) ON DELETE RESTRICT,
    topic_configuration_id   UUID NOT NULL REFERENCES topic_configurations(id),
    status                   TEXT NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'applying', 'retrying', 'synced', 'error')),
    labels                   JSONB NOT NULL DEFAULT '{}',
    error_message            TEXT,
    last_reconciled_at       TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_definition_id, cluster_id)
);

CREATE INDEX idx_topic_claims_topic_definition_id  ON topic_claims (topic_definition_id);
CREATE INDEX idx_topic_claims_status               ON topic_claims (status);
CREATE INDEX idx_topic_claims_cluster_id           ON topic_claims (cluster_id);
