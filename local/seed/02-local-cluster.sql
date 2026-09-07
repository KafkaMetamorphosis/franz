-- Local-dev seed: register one Kafka Cluster wired to the whole local loop —
-- the Cluster Provider agent stands its broker up (`make agent`), and it carries
-- the franz.placement/* coordinate the Resource Provider agent scopes on
-- (`make gregorsamsa`). NOT for any shared or production database.
--
--   provider     : local-kafka-agent          (seed 01)
--   placement    : franz.placement/env=local  → matches gregor-samsa's
--                  franz.placement-selector/env=local (seed 03)
--   bootstrap    : localhost:9092             (what the local-docker recipe advertises)
--   config       : the same Franz-friendly keys local-kafka-agent advertises
--                  as its franz.default-kafka-config/* defaults (seed 01)
--
-- Idempotent: re-running refreshes the labels / config / provider / state
-- (and revives it if a previous run left it DELETED).

INSERT INTO kafka_cluster (
    id, realm_id, name, frn,
    connection_strings, labels, cluster_configuration,
    cluster_provider_agent, brokers, state
)
SELECT
    '00000000-0000-0000-0000-00000000c101',
    r.id,
    'local-1',
    'default:kafka-cluster:local-1',
    '[{"bootstrap_urls": ["localhost:9092"], "type": "PLAINTEXT"}]'::jsonb,
    '{
       "franz.role": "local-loop",
       "franz.placement/env": "local"
     }'::jsonb,
    '{
       "partitions": "3",
       "replication-factor": "1",
       "retention.ms": "604800000",
       "kafka-version": "3.9.0"
     }'::jsonb,
    'local-kafka-agent',
    1,
    'ACTIVE'
FROM realm r
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    connection_strings     = EXCLUDED.connection_strings,
    labels                 = EXCLUDED.labels,
    cluster_configuration  = EXCLUDED.cluster_configuration,
    cluster_provider_agent = EXCLUDED.cluster_provider_agent,
    brokers                = EXCLUDED.brokers,
    state                  = 'ACTIVE',
    updated_at             = now();
