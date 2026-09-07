-- Local-dev seed: register the local-kafka-docker-agent so `make agent` can
-- connect without a console round-trip. NOT for any shared or production
-- database — the token is public and fixed.
--
-- The plaintext token is:  frnat_local-dev-do-not-use-in-production
-- (Makefile's `make agent` passes it as FRANZ_TOKEN.)
-- token_hash below = sha256(plaintext), matching pkg/shared/token.Hash.
--
-- Idempotent: re-running refreshes the labels / token / status.

INSERT INTO agent (id, realm_id, name, frn, type, labels, status, token_hash)
SELECT
    '00000000-0000-0000-0000-0000000a9e01',
    r.id,
    'local-kafka-agent',
    'default:agent:local-kafka-agent',
    'CLUSTER_PROVIDER',
    -- franz.default-kafka-config/* = advisory defaults the console pre-fills into
    -- a Kafka Cluster form when this agent is the linked provider (ADR-API-010).
    '{
       "franz.role": "local-kafka-agent",
       "franz.default-kafka-config/partitions": "3",
       "franz.default-kafka-config/replication-factor": "1",
       "franz.default-kafka-config/retention.ms": "604800000",
       "franz.default-kafka-config/kafka-version": "3.9.0",
       "franz.default-kafka-config/available-versions": "3.7.0,3.9.0,4.0.0"
     }'::jsonb,
    'ACTIVE',
    encode(sha256(convert_to('frnat_local-dev-do-not-use-in-production', 'UTF8')), 'hex')
FROM realm r
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    type       = EXCLUDED.type,
    labels     = EXCLUDED.labels,
    status     = 'ACTIVE',
    token_hash = EXCLUDED.token_hash,
    updated_at = now();
