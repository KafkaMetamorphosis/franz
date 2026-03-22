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
