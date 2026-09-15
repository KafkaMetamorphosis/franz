-- Local-dev seed: register one Async Channel with a real access policy (003.5)
-- so the deliverable-17 access-policy views (ListChannelClients /
-- ListClientChannelAccess) have something to show against the clients seed 05
-- already registers. NOT for any shared or production database.
--
-- The policy deliberately uses BOTH principal kinds so the two seeded clients
-- (billing, payments-consumer — both carry org.com/owner=payments-team, seed
-- 05) end up with *different* grants, which is the more interesting case to
-- explore against ListChannelClients than "everyone gets the same thing":
--
--   - a label-selector ALLOW grants READ to anyone with org.com/owner=payments-team
--     (both seeded clients)
--   - a client_frn ALLOW additionally grants WRITE to billing only
--
-- Inserted directly as a row, not through CreateAsyncChannel — this bypasses
-- placement (no shard rows are materialised, ADR-API-009), which is fine here
-- since the access-policy views only need the channel row and the clients to
-- exist, not a placed shard.
--
-- Idempotent: re-running refreshes the labels / access_policy.

INSERT INTO async_channel (id, realm_id, name, frn, type, channel_partitions, labels, access_policy)
SELECT
    '00000000-0000-0000-0000-00000000ac01',
    r.id,
    'billing-events',
    'default:async-channel:billing-events',
    'KAFKA_TOPIC',
    1,
    '{"team": "payments"}'::jsonb,
    '{
       "statements": [
         {
           "effect": "ALLOW",
           "principal": {"labels": "org.com/owner=payments-team"},
           "permissions": ["READ"]
         },
         {
           "effect": "ALLOW",
           "principal": {"client_frn": "default:client:billing"},
           "permissions": ["WRITE"]
         }
       ]
     }'::jsonb
FROM realm r
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    labels         = EXCLUDED.labels,
    access_policy  = EXCLUDED.access_policy,
    updated_at     = now();
