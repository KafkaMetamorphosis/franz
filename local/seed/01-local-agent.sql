-- Local-dev seed: register the local-kafka-docker-agent so `make agent` can
-- connect without a console round-trip. NOT for any shared or production
-- database — the token is public and fixed.
--
-- The plaintext token is:  frnat_local-dev-do-not-use-in-production
-- (Makefile's `make agent` passes it as FRANZ_TOKEN.)
-- token_hash below = sha256(plaintext), matching pkg/shared/token.Hash.
--
-- Idempotent: re-running refreshes the schema / token / status.

INSERT INTO agent (id, realm_id, name, frn, type, labels, provisioning_labels, status, token_hash)
SELECT
    '00000000-0000-0000-0000-0000000a9e01',
    r.id,
    'local-kafka-agent',
    'default:agent:local-kafka-agent',
    'CLUSTER_PROVIDER',
    '{"franz.role": "local-kafka-agent"}'::jsonb,
    -- Mirrors localkafka recipe's franz.provisioning/* keys.
    '[
       {"key": "franz.provisioning/deployment-type",
        "description": "Selects the recipe family.",
        "allowed_values": ["local-docker"],
        "default_value": "local-docker",
        "required": true},
       {"key": "franz.provisioning/kafka-version",
        "description": "apache/kafka image tag when kafka-image is unset.",
        "default_value": "3.7.0"},
       {"key": "franz.provisioning/kafka-image",
        "description": "Full apache/kafka-compatible image ref (tag, digest, or mirror). Overrides kafka-version."}
     ]'::jsonb,
    'ACTIVE',
    encode(sha256(convert_to('frnat_local-dev-do-not-use-in-production', 'UTF8')), 'hex')
FROM realm r
WHERE r.slug = 'default'
ON CONFLICT (realm_id, name) DO UPDATE SET
    type                = EXCLUDED.type,
    labels              = EXCLUDED.labels,
    provisioning_labels = EXCLUDED.provisioning_labels,
    status              = 'ACTIVE',
    token_hash          = EXCLUDED.token_hash,
    updated_at          = now();
