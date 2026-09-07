-- Local-dev seed: register Gregor Samsa (the Resource Provider agent) and give
-- the local Kafka Cluster the franz.placement/* labels its scope selects on, so
-- `make gregorsamsa` connects and immediately has work in scope. NOT for any
-- shared or production database — the token is public and fixed.
--
-- The plaintext token is:  frnat_local-dev-gregor-samsa
-- (Makefile's `make gregorsamsa` passes it as FRANZ_TOKEN.)
-- token_hash below = sha256(plaintext), matching pkg/shared/token.Hash.
--
-- Scope rule (005 ADR §1.2): a cluster is in scope iff, for every
-- franz.placement-selector/<key>=<value> on this agent, the cluster carries
-- franz.placement/<key>=<value>. An agent with no selector labels is inert, so
-- the pair below has to line up for the local loop to do anything.
--
-- Idempotent: re-running refreshes the labels / token / status.

INSERT INTO agent (id, realm_id, name, frn, type, labels, status, token_hash)
SELECT
    '00000000-0000-0000-0000-0000000a9e02',
    r.id,
    'gregor-samsa',
    'default:agent:gregor-samsa',
    'RESOURCE_PROVIDER',
    '{
       "franz.role": "gregor-samsa",
       "franz.placement-selector/env": "local"
     }'::jsonb,
    'ACTIVE',
    encode(sha256(convert_to('frnat_local-dev-gregor-samsa', 'UTF8')), 'hex')
FROM realm r
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    type       = EXCLUDED.type,
    labels     = EXCLUDED.labels,
    status     = 'ACTIVE',
    token_hash = EXCLUDED.token_hash,
    updated_at = now();

-- Give every local Kafka Cluster the matching coordinate, so a cluster created
-- through the console before this seed ran also lands in scope. Merging (||)
-- rather than replacing keeps whatever other labels the operator set.
UPDATE kafka_cluster kc
SET labels     = kc.labels || '{"franz.placement/env": "local"}'::jsonb,
    updated_at = now()
FROM realm r
WHERE r.id = kc.realm_id
  AND r.slug = 'default'
  AND kc.state <> 'DELETED'
  AND COALESCE(kc.labels ->> 'franz.placement/env', '') <> 'local';
