-- Local-dev seed: one Async Channel that actually gets **placed**, so the local
-- loop produces real Kafka topics and topic-scoped indicators without anyone
-- having to hand-author a channel in the console first. NOT for any shared or
-- production database.
--
-- Placement is opt-in (003.7 step 1): a channel with no
-- `franz.affinity/selector` has *no* candidate clusters and materialises
-- nothing. Every seeded channel before this one was in exactly that state, so a
-- fresh local database produced no `kafka_topic` rows at all — and with no
-- placed shard, Gregor Samsa had no topic to describe, leaving every
-- `kafka.topic.*` indicator (005 §2.1) permanently unsampled.
--
--   affinity : env=local  → matches local-1's free-form `env` label (seed 02).
--
-- The selector deliberately targets the **free-form** `env` label rather than
-- `franz.placement/env`. Both would match — `franz.affinity/selector` is
-- evaluated against the cluster's whole label map — but channel→cluster affinity
-- is specified against free-form labels, while `franz.placement/*` is the
-- agent→cluster scoping prefix (005 OQ2 keeps them separate on purpose).
-- Selecting on the agent's prefix would work by accident and teach the wrong
-- thing.
--
-- `env=local` resolves to exactly one cluster in the local loop (local-1, the
-- only cluster seed 02 registers), so both shards land there and the mapping
-- stays unambiguous. No `franz.shard-size` / `franz.weight` is set; the 003.7
-- defaults apply.
--
-- channel_partitions = 2 so the console's shards table and the per-shard
-- indicators have more than one row to show. These become the Kafka topics
-- `shipments-0` and `shipments-1`.
--
-- The name avoids `orders` / `alpha` / `beta`, which the Postgres integration
-- tests create and delete in whatever database `FRANZ_TEST_DB_DSN` points at —
-- a seeded row under one of those names would be clobbered by a test run.
--
-- Inserted directly as a row, which bypasses the create-time placement trigger
-- (ADR-API-009) — but *not* placement itself: Franz's placement retry sweep
-- (003.7 OQ5, `startPlacementSweep`, every `placement.sweep_interval`, default
-- 30s) picks up any ACTIVE channel with fewer live shard rows than its declared
-- `channel_partitions`. So the shards appear within ~30s of Franz starting, then
-- Gregor Samsa creates the real topics on its next reconcile.
--
-- Seed 06's `billing-events` is deliberately left with no affinity: it stays the
-- worked example of a channel that is registered but intentionally unplaced.
--
-- Idempotent: re-running refreshes the labels / access_policy. It does not
-- delete shard rows — an already-placed channel stays placed.

INSERT INTO async_channel (id, realm_id, name, frn, type, channel_partitions, labels, access_policy)
SELECT
    '00000000-0000-0000-0000-00000000ac02',
    r.id,
    'shipments',
    'default:async-channel:shipments',
    'KAFKA_TOPIC',
    2,
    '{
       "team": "logistics",
       "franz.affinity/selector": "env=local"
     }'::jsonb,
    '{
       "statements": [
         {
           "effect": "ALLOW",
           "principal": {"labels": "org.com/owner=payments-team"},
           "permissions": ["READ", "WRITE"]
         }
       ]
     }'::jsonb
FROM realm r
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    labels        = EXCLUDED.labels,
    access_policy = EXCLUDED.access_policy,
    updated_at    = now();
