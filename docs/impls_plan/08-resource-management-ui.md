# 08 — Resource management & agent provisioning schema

Status: 🚧 in progress
Executed by: codex (pending)
Depends on: [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md) · [06](./06-web-console-bootstrap.md) · [07](./07-local-kafka-docker-agent.md)
Specs: `003-franz/003.9-agents` (amended — provisioning-label schema), `004-local-kafka-docker-agent/README` (amended — `kafka-image`, self-declared schema), `003-franz/003.3-kafka-cluster`, `001-ux/README`
Proto: `agent.proto` — new `ProvisioningLabelSpec`; `Agent` / `CreateAgentRequest` / `UpdateAgentRequest` gain `provisioning_labels`

## Goal

Make the resources the console owns (Agents, Kafka Clusters) **fully manageable
from the browser**, and make provisioning-label entry **schema-driven** instead of
free-text guesswork.

Two threads:

1. **Edit / full CRUD** — 06 shipped list / register / detail / lifecycle but no
   edit path, though `UpdateAgent` (04.3) and `UpdateKafkaCluster` (03.4, incl.
   `cluster_provider_agent`) exist and are tested. 08 adds the edit pages.
2. **Agent provisioning-label schema** — an agent advertises which
   `franz.provisioning/*` labels its recipes understand, with allowed values and
   defaults. The console renders cluster forms from that schema: pick a provider
   agent → its provisioning fields appear, pre-filled and constrained. The
   local-kafka-docker-agent self-declares its schema on registration.

The schema is **advisory** — a console UX aid. Franz stores and serves it but
does **not** validate `KafkaCluster.labels` against it at write time; the
`cluster_provider_agent` link stays an unvalidated string (`003.9`), and the
agent remains the sole authority on what it does with labels.

## Design

### Proto (`agent.proto`)

```proto
message ProvisioningLabelSpec {
  string key = 1;                     // e.g. "franz.provisioning/kafka-image"
  string description = 2;
  repeated string allowed_values = 3; // empty ⇒ free text
  string default_value = 4;
  bool   required = 5;                // console-enforced only
}

message Agent {
  // ...existing fields 1–7...
  repeated ProvisioningLabelSpec provisioning_labels = 8;
}

message CreateAgentRequest {
  // ...name, type, labels...
  repeated ProvisioningLabelSpec provisioning_labels = 4;
}

message UpdateAgentRequest {
  // ...name, type, labels...
  repeated ProvisioningLabelSpec provisioning_labels = 5;
  google.protobuf.FieldMask update_mask = 4;   // add "provisioning_labels"
}
```

### `local-docker` recipe — `franz.provisioning/kafka-image`

| Label | Meaning | Default |
|---|---|---|
| `franz.provisioning/kafka-image` | full image ref for an **apache/kafka-compatible** image (tag, digest, or registry mirror) | — |
| `franz.provisioning/kafka-version` | tag sugar when `kafka-image` is unset | `3.7.0` ⇒ `apache/kafka:3.7.0` |

Precedence: `kafka-image` wins; else `apache/kafka:<kafka-version>`. The resolved
ref already feeds `recipe.Spec.Image` and the hash, so a change recreates the
container (volume kept).

### Local agent self-declared schema

`EnsureRegistered` (07) sends `provisioning_labels` on `CreateAgent` and refreshes
them via `UpdateAgent` on the reuse path:

| key | allowed_values | default | required |
|---|---|---|---|
| `franz.provisioning/deployment-type` | `["local-docker"]` | `local-docker` | yes |
| `franz.provisioning/kafka-version` | — | `3.7.0` | no |
| `franz.provisioning/kafka-image` | — | — | no |

## Tasks

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 08.1 | **Proto** — `ProvisioningLabelSpec`; `Agent` / `CreateAgentRequest` / `UpdateAgentRequest` `provisioning_labels`; `buf generate` (Go + OpenAPI + console `schema.d.ts`); `buf lint` / `buf breaking` (additive-only) | proto, `003.9` | ⬜ | |
| 08.2 | **Domain + storage (deliverable 04)** — `agent.ProvisioningLabelSpec` value type + validation (`key` non-empty and `franz.` -prefixed, no dup keys, `default_value` ∈ `allowed_values` when both set); `agent` table `provisioning_labels jsonb` (edit `V1__init.sql`); repo read/write | `003.9`, `003.12` | ⬜ | |
| 08.3 | **`agents.Service` + handler** — Create accepts `provisioning_labels`; Update honours the `provisioning_labels` mask path (replace wholesale); `GetAgent` / `ListAgents` return it; unit + REST tests | `003.9` | ⬜ | |
| 08.4 | **`useUpdateAgent` / `useUpdateKafkaCluster` hooks** — build `update_mask` from changed fields only (camelCase JSON paths, comma-joined), invalidate detail + list, surface `ApiError` (400 immutable, 404, 409) | proto, 03.4, 04.3 | ⬜ | |
| 08.5 | **Agent edit page** (`/agents/:name/edit`, "Edit" button on detail) — form for `type` (select) and `labels` (`LabelEditor`), plus a **provisioning-label schema editor**: repeatable rows (key, description, allowed values (comma), default, required). `name` / `frn` / `status` / timestamps read-only. Save sends only the touched mask paths | `003.9`, `001-ux/demo/agents.html` | ⬜ | |
| 08.6 | **Schema-driven provisioning fields** — shared component: given an agent's `provisioning_labels`, render one control per spec (`allowed_values` ⇒ `<select>`, else text), pre-filled with `default_value`, required ones marked; emits a `Record<string,string>` merged into `KafkaCluster.labels`. Falls back to today's fixed `deployment-type` + `kafka-version` inputs when the selected agent has no schema | `003.3` | ⬜ | |
| 08.7 | **Cluster register form** — use 08.6: choosing `cluster_provider_agent` swaps in that agent's provisioning fields; other labels stay in the generic `LabelEditor` | `003.3`, `001-ux/demo/register-kafka-cluster.html` | ⬜ | |
| 08.8 | **Cluster edit page** (`/kafka/clusters/:name/edit`, "Edit" button on detail) — `connection_strings` (bootstrap URLs of the one entry; type stays `PLAINTEXT`), `labels` (generic editor + the 08.6 provisioning fields), `cluster_configuration` (key=value textarea), `cluster_provider_agent` (agent picker). `name` / `frn` / `state` / `provider_status` / timestamps read-only | `003.3`, `001-ux/demo/kafka-clusters.html` | ⬜ | |
| 08.9 | **Re-assignment confirm** — changing (or clearing) `cluster_provider_agent` on a cluster that already has one shows an inline caution and requires an explicit confirm before Save. Wording only — the hand-off is 05's behaviour | `003.3`, 05 | ⬜ | |
| 08.10 | **Client-side guards** — cluster `connection_strings` keeps ≥1 non-empty bootstrap URL; agent `type` not settable to `UNSPECIFIED`; required provisioning labels must be filled; a no-op Save is disabled; a 409 refetches and asks the operator to re-apply (no silent overwrite) | `003.3`, `003.9`, ADR-API-005 | ⬜ | |
| 08.11 | **`local-docker` recipe** — `franz.provisioning/kafka-image` (precedence over `kafka-version`); recipe tests for both paths and the hash change; ADR-004 label table updated in code comments/docs pointers | `004` | ⬜ | |
| 08.12 | **Local agent self-declares its schema** — `EnsureRegistered` sends `provisioning_labels` on create and refreshes on the reuse path; unit test against the bufconn stub | `004`, 07 | ⬜ | |
| 08.13 | **Tests** — vitest: mask holds only changed fields, immutable fields absent, `type=UNSPECIFIED` blocked, schema field rendering (select vs text, defaults, required), re-assignment gate, 409 refetch. Playwright (extends 06.7): register agent with a provisioning schema → register cluster, provisioning fields pre-filled from the agent → edit cluster config + labels → reload, persisted → edit agent type → reload, persisted | — | ⬜ | |

## Done when

- From the browser only: register an Agent (with a provisioning-label schema),
  register a Kafka Cluster pointing at it and see the provisioning fields
  pre-filled from the agent, then edit the cluster's `connection_strings` /
  `labels` / `cluster_configuration` / provider and the agent's `type` /
  `labels` / schema — every change round-trips (reload shows it).
- Edit forms never present an immutable field as editable or send one in a mask.
- Changing a cluster's provider agent is gated behind an explicit confirm.
- `make agent` registers the local agent with a `deployment-type` /
  `kafka-version` / `kafka-image` schema; a cluster can pin
  `franz.provisioning/kafka-image` and the broker comes up on that image.
- `go build/test/vet`, `buf lint`/`breaking`, `make test` (vitest), the extended
  Playwright smoke — all green; CI green.

## Notes

- Lifecycle transitions (`pause` / `resume` / `delete` / `rotateToken`) stay as
  the dedicated actions 06 built — not folded into the edit form. `state` /
  `status` are never in an `update_mask`.
- `provisioning_labels` is additive proto — `buf breaking` must stay clean.
- Realms are not console-managed (single seeded `default`, no `RealmService`
  CRUD) — out of scope.
- Async Channel / Client / Policy / Indicator management ships with their own
  features (11 / 15 / 14 / 13), following the edit-page + schema patterns here.
- `kafka-image` must be an apache/kafka-compatible image (same KRaft env
  contract the recipe renders) — not an arbitrary Kafka distribution.

### Spec amendments made for this deliverable (docs repo)

- `003-franz/003.9-agents.md` — new "Provisioning-label schema" section; `Key
  fields` row; `CreateAgent`/`UpdateAgent` now also carry `provisioning_labels`;
  invariant that it is advisory and server-unvalidated against clusters.
- `004-local-kafka-docker-agent/README.md` — `franz.provisioning/kafka-image`
  row in the label table; note that the agent self-declares its provisioning
  schema at registration.

### Decisions (asked)

- **Scope** — user chose to expand 08 to the full schema feature (not split).
- **Schema is advisory** — Franz stores/serves, console renders, no write-time
  enforcement on `KafkaCluster`.
- **Edit lives on separate `/…/edit` pages** with an "Edit" button on detail.
- **`connection_strings` edit** — bootstrap URLs of the single entry only.
- **17 assumptions from trackers 03/04/05** — all ratified as-is.
