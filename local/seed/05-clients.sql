-- Local-dev seed: register two example Clients (003.10) so the fleet has real
-- SDK identities to list/inspect without a hand-authored CreateClient call
-- first. NOT for any shared or production database.
--
-- Names match the ones used throughout the telemetry/consumer-group tests and
-- docs (billing / payments) purely for narrative consistency — nothing ties
-- them to those tests at runtime.
--
-- Idempotent: re-running refreshes labels only. A Client's name is immutable
-- and its FRN is never freed once deleted (003.10), so this seed never
-- attempts to resurrect a row `make deps-reset` didn't create.

INSERT INTO client (id, realm_id, name, frn, labels)
SELECT v.id, r.id, v.name, 'default:client:' || v.name, v.labels::jsonb
FROM realm r
CROSS JOIN (VALUES
    ('00000000-0000-0000-0000-00000000cc01'::uuid, 'billing',
     '{"org.com/owner": "payments-team"}'),
    ('00000000-0000-0000-0000-00000000cc02'::uuid, 'payments-consumer',
     '{"org.com/owner": "payments-team"}')
) AS v(id, name, labels)
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    labels     = EXCLUDED.labels,
    updated_at = now();
