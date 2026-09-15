-- Local-dev seed: register the 15 structural indicators Gregor Samsa publishes
-- (005 ADR §2.1, pkg/gregorsamsa/telemetry/telemetry.go). NOT for any shared or
-- production database — this is one fixed set for the 'default' realm only.
--
-- Two of the fifteen (kafka.topic.drained / kafka.topic.consumer_connected)
-- were added with deliverable 18 — the migration flow's early-completion
-- signals (003.13): whether a shard still holds data on disk, and whether any
-- consumer group has committed an offset to it. Computed for every managed
-- topic every sweep, the same as the other thirteen — Gregor Samsa has no way
-- to know which topics are migration-involved, only Franz's bookkeeping does.
--
-- Deliverable 15 makes pre-registration real: PublishIndicatorSamples now
-- rejects a sample for an unknown indicator with FAILED_PRECONDITION. Before
-- 15 this was silently accepted ("until the registry exists Franz accepts any
-- name" — see the comment history in telemetry.go); Gregor Samsa's sweep would
-- otherwise fail every publish from the moment 15 ships. This seed is the local
-- stand-in for whatever a real deployment's provisioning step is meant to be —
-- see the open question this leaves in docs/impls_tracker/15-telemetry-ingest.md.
--
-- staleness_threshold = 5m for all thirteen: Gregor Samsa's full sweep defaults to
-- a 60s cadence (005 ADR §2.2), so 5m tolerates a few missed sweeps before
-- Governance treats the signal as STALE. No indicator-specific tuning yet —
-- revisit per-indicator if that turns out too coarse.
--
-- Idempotent: re-running refreshes unit / applies_to / staleness / source_agents.

INSERT INTO indicator (id, realm_id, name, frn, unit, applies_to, staleness_threshold, source_agents)
SELECT v.id, r.id, v.name, 'default:indicator:' || v.name, v.unit, v.applies_to,
       '5m', '["gregor-samsa"]'::jsonb
FROM realm r
CROSS JOIN (VALUES
    -- Topic-scoped (005 ADR §2.1)
    ('00000000-0000-0000-0000-0000000ea001'::uuid, 'kafka.topic.state', 'enum', 'KAFKA_TOPIC'),
    ('00000000-0000-0000-0000-0000000ea002'::uuid, 'kafka.topic.partitions', 'count', 'KAFKA_TOPIC'),
    ('00000000-0000-0000-0000-0000000ea003'::uuid, 'kafka.topic.replication_factor', 'count', 'KAFKA_TOPIC'),
    ('00000000-0000-0000-0000-0000000ea004'::uuid, 'kafka.topic.under_replicated_partitions', 'count', 'KAFKA_TOPIC'),
    ('00000000-0000-0000-0000-0000000ea005'::uuid, 'kafka.topic.config_drift', 'boolean', 'KAFKA_TOPIC'),
    ('00000000-0000-0000-0000-0000000ea00e'::uuid, 'kafka.topic.drained', 'boolean', 'KAFKA_TOPIC'),
    ('00000000-0000-0000-0000-0000000ea00f'::uuid, 'kafka.topic.consumer_connected', 'boolean', 'KAFKA_TOPIC'),
    -- Cluster-scoped (005 ADR §2.1)
    ('00000000-0000-0000-0000-0000000ea006'::uuid, 'kafka.cluster.broker_count', 'count', 'KAFKA_CLUSTER'),
    ('00000000-0000-0000-0000-0000000ea007'::uuid, 'kafka.cluster.online_broker_count', 'count', 'KAFKA_CLUSTER'),
    ('00000000-0000-0000-0000-0000000ea008'::uuid, 'kafka.cluster.controller_id', 'string', 'KAFKA_CLUSTER'),
    ('00000000-0000-0000-0000-0000000ea009'::uuid, 'kafka.cluster.total_partition_replicas', 'count', 'KAFKA_CLUSTER'),
    ('00000000-0000-0000-0000-0000000ea00a'::uuid, 'kafka.cluster.replicas_per_broker', 'count', 'KAFKA_CLUSTER'),
    ('00000000-0000-0000-0000-0000000ea00b'::uuid, 'kafka.cluster.leaders_per_broker', 'count', 'KAFKA_CLUSTER'),
    ('00000000-0000-0000-0000-0000000ea00c'::uuid, 'kafka.cluster.under_replicated_partitions', 'count', 'KAFKA_CLUSTER'),
    ('00000000-0000-0000-0000-0000000ea00d'::uuid, 'kafka.cluster.offline_partitions', 'count', 'KAFKA_CLUSTER')
) AS v(id, name, unit, applies_to)
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    unit                = EXCLUDED.unit,
    applies_to          = EXCLUDED.applies_to,
    staleness_threshold = EXCLUDED.staleness_threshold,
    source_agents       = EXCLUDED.source_agents,
    updated_at          = now();
