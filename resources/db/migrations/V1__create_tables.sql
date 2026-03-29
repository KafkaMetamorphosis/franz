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
    topic_name               TEXT NOT NULL,
    topic_configuration_id   UUID NOT NULL REFERENCES topic_configurations(id),
    status                   TEXT NOT NULL DEFAULT 'Active'
                             CHECK (status IN ('Active', 'Paused', 'Error', 'Deleted')),
    expansion_status         TEXT NOT NULL DEFAULT 'PendingExpansion'
                             CHECK (expansion_status IN ('Expanded', 'PendingExpansion')),
    labels                   JSONB NOT NULL DEFAULT '{}',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Allow name reuse after soft deletion
CREATE UNIQUE INDEX idx_topic_definitions_topic_name_active
    ON topic_definitions (topic_name)
    WHERE status != 'Deleted';

CREATE INDEX idx_topic_definitions_status ON topic_definitions (status);

CREATE TABLE topic_claims (
    id                                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_definition_id               UUID NOT NULL REFERENCES topic_definitions(id) ON DELETE RESTRICT,
    cluster_id                        UUID NOT NULL REFERENCES clusters(id) ON DELETE RESTRICT,
    topic_configuration_override_id   UUID REFERENCES topic_configurations(id),
    status                            TEXT NOT NULL DEFAULT 'Pending'
                                      CHECK (status IN ('Pending', 'Ready', 'Paused', 'Deleted', 'Error')),
    labels                            JSONB NOT NULL DEFAULT '{}',
    error                             TEXT,
    created_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_definition_id, cluster_id)
);

CREATE INDEX idx_topic_claims_topic_definition_id ON topic_claims (topic_definition_id);
CREATE INDEX idx_topic_claims_cluster_id          ON topic_claims (cluster_id);
CREATE INDEX idx_topic_claims_status              ON topic_claims (status);

CREATE TABLE topic_revisions (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_claim_id            UUID NOT NULL REFERENCES topic_claims(id) ON DELETE RESTRICT,
    kafka_cluster_id          UUID NOT NULL REFERENCES clusters(id) ON DELETE RESTRICT,
    topic_configuration       JSONB NOT NULL,
    status                    TEXT NOT NULL DEFAULT 'PendingReconciliation'
                              CHECK (status IN ('PendingReconciliation', 'PendingDelete', 'Created', 'Updated', 'Deleted', 'Error')),
    last_topic_configuration  JSONB,
    error                     TEXT,
    attempts                  INTEGER NOT NULL DEFAULT 0,
    retry_of_revision_id      UUID REFERENCES topic_revisions(id),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_topic_revisions_topic_claim_id      ON topic_revisions (topic_claim_id);
CREATE INDEX idx_topic_revisions_kafka_cluster_id    ON topic_revisions (kafka_cluster_id);
CREATE INDEX idx_topic_revisions_status              ON topic_revisions (status);
CREATE INDEX idx_topic_revisions_retry_of_revision_id ON topic_revisions (retry_of_revision_id)
    WHERE retry_of_revision_id IS NOT NULL;
