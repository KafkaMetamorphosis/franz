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
