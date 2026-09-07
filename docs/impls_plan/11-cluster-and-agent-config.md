# 11 — Cluster & agent configuration model

Status: ✅ done
Executed by: claude (claude-sonnet-5) — codex out of quota (usage limit resets 2026-09-28)
Depends on: [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md) · [08](./08-resource-management-ui.md)
Specs: `003-franz/003.3-kafka-cluster`, `003-franz/003.1-conventions`, `003-franz/003.9-agents`, `004-local-kafka-docker-agent`, `DECISIONS.md` (new **ADR-API-010**, supersedes ADR-API-008)
Proto: `agent.proto` (remove `ProvisioningLabelSpec` / `provisioning_labels`), `kafka.proto` (`KafkaCluster` gains `brokers` / `disk_size`), `agent_cluster_provider.proto` (`ClusterAssignment` gains `brokers` / `disk_size`, drops `provisioning`)

## Goal

Settle where a Kafka Cluster's config lives and how an agent hands the console
sensible defaults — without the structured provisioning-label schema.

- **`KafkaCluster.cluster_configuration` stays a `map<string,string>`** — the one
  home for Kafka config: topic-config defaults (`retention.ms`, `cleanup.policy`,
  …), the shard defaults `partitions` / `replication-factor`, and `kafka-version`.
- **`KafkaCluster` gains two typed fields**: `brokers` (int32) and `disk_size`
  (string size hint). These are cluster *shape*, not Kafka config.
- **`kafka-image` is dropped** — the agent picks the image; Franz does not store
  or forward it.
- **`deployment-type` is dropped** — the agent *is* the deployment type (one
  recipe family per agent; you choose it by choosing the agent).
- **`franz.provisioning/*` labels on the cluster are retired.**
- **Agents advertise defaults as `franz.default-kafka-config/*` labels** on their
  own `Agent.labels` — e.g. `franz.default-kafka-config/partitions=4`,
  `franz.default-kafka-config/retention.ms=604800000`,
  `franz.default-kafka-config/available-versions=3.7.0,3.9.0,4.0.0`,
  `franz.default-kafka-config/kafka-version=3.9.0`. The console reads them to
  pre-fill the cluster form. Advisory — **Franz enforces nothing**;
  `cluster_provider_agent` stays an unvalidated string (`003.3`).
- **Remove the structured schema** (ADR-API-008): `ProvisioningLabelSpec`,
  `Agent.provisioning_labels` (proto + `jsonb` column), `ValidateProvisioningLabels`,
  `ProvisioningFields` / `ProvisioningLabelEditor`.

Not in scope: agent→cluster watch scoping (`franz.placement/*` ↔
`franz.placement-selector/*`) — that lands with Gregor Samsa (12). Channel→cluster
placement (`franz.affinity/*`, `003.7`) is unchanged.

## Config-map key convention

Keys in `cluster_configuration` are **Franz-friendly** (`partitions`,
`replication-factor`, `kafka-version`, `retention.ms`, …) — they mirror the agent
label suffixes. Translation to real Kafka broker/topic keys is the consumer's job
(the recipe's allow-list, the `003.6` config merge). `partitions` /
`replication-factor` are the two placement (13) reads to seed shard rows.

## Tasks

### Specs & ADR (docs repo → main)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.1 | **ADR-API-010** in `DECISIONS.md` — `cluster_configuration` stays a map; `brokers` / `disk_size` typed; `kafka-image` + `deployment-type` dropped; `franz.provisioning/*` retired; agents advertise `franz.default-kafka-config/*` label defaults (console-only, unenforced). Mark **ADR-API-008 superseded** | — | ✅ | 2026-09-07 |
| 11.2 | `003.1` reserved-label table — remove `franz.provisioning/*`; add `franz.default-kafka-config/*` (on Agent, advisory console defaults). Note the `available-versions` / `kafka-version` meta-keys | `003.1` | ✅ | 2026-09-07 |
| 11.3 | `003.3` — add `brokers` / `disk_size` key-field rows; `cluster_configuration` stays a map (now also holds `kafka-version` + `partitions` + `replication-factor`); drop the `franz.provisioning/*` mention from the `labels` row; update the provider-link bullet | `003.3` | ✅ | 2026-09-07 |
| 11.4 | `003.9` — delete the "Provisioning-label schema" section + `provisioning_labels` key-field row + invariant; add a short "Advertised defaults" paragraph (`franz.default-kafka-config/*`, console-only, never enforced); fix ADR-API-008 → 010 refs | `003.9` | ✅ | 2026-09-07 |
| 11.5 | `004-local-kafka-docker-agent` §3 — replace the `franz.provisioning/*` table + `Agent.provisioning_labels` paragraph: recipe inputs now come from `cluster_configuration` (`kafka-version`) + the typed `brokers` / `disk_size` on the assignment; `kafka-image` / `deployment-type` gone; defaults advertised via `franz.default-kafka-config/*` | `004` | ✅ | 2026-09-07 |
| 11.6 | `003.8` governance whitelist — keep "edit `cluster_configuration.*` keys"; add `brokers` / `disk_size` to the editable-field set if `003.8` enumerates cluster fields | `003.8` | ✅ | 2026-09-07 |

### Proto (`franz/api/franz/v1`) — then `make gen`

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.7 | `agent.proto` — remove `message ProvisioningLabelSpec`; remove `provisioning_labels` from `Agent` (8), `CreateAgentRequest` (4), `UpdateAgentRequest` (5); `reserved` those numbers | ADR-API-010 | ✅ | 2026-09-07 |
| 11.8 | `kafka.proto` — `KafkaCluster` gains `int32 brokers` + `string disk_size`; same two on `CreateKafkaClusterRequest` + `UpdateKafkaClusterRequest` (mask paths `brokers` / `disk_size`). `cluster_configuration` **unchanged** on all three | ADR-API-010 | ✅ | 2026-09-07 |
| 11.9 | `agent_cluster_provider.proto` — `ClusterAssignment`: `reserved 6` (`provisioning` map gone); add `int32 brokers = 7`, `string disk_size = 8`. `cluster_configuration` (5) unchanged; Franz fills it from `KafkaCluster.cluster_configuration` verbatim | `004` | ✅ | 2026-09-07 |
| 11.10 | `buf lint` clean; `buf breaking` documents the removals (pre-production, no gate on `main`) | — | ✅ | 2026-09-07 |

### Migration (`V1__init.sql`, edited in place → `make deps-reset`)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.11 | Drop `agent.provisioning_labels jsonb`; add `kafka_cluster.brokers int` (nullable) + `kafka_cluster.disk_size text` (default `''`). `kafka_cluster.cluster_configuration jsonb` unchanged | `003.12` | ✅ | 2026-09-07 |
| 11.12 | `local/seed/01-local-agent.sql` — drop the `provisioning_labels` JSON; put defaults in `labels`: `franz.default-kafka-config/{partitions=3,replication-factor=1,retention.ms=604800000,kafka-version=3.9.0,available-versions=3.7.0,3.9.0,4.0.0}` | `004` | ✅ | 2026-09-07 |

### Backend (`franz/pkg/franz`)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.13 | `core/domain/agent` — delete `provisioning.go`, `ProvisioningLabelSpec`, `Agent.ProvisioningLabels`, `SetProvisioningLabels`; drop the `New(...)` param; fix every caller + test | `003.9` | ✅ | 2026-09-07 |
| 11.14 | `core/domain/cluster` — add `Brokers int32` + `DiskSize string` fields + `New(...)` params + a `SetBrokers` / `SetDiskSize` on the mutable path; `Configuration` map unchanged. `ToAssignment()` fills the typed `Brokers` / `DiskSize` and `Configuration` | `003.3` | ✅ | 2026-09-07 |
| 11.15 | `core/domain/provider` — delete `ProvisioningLabels(labels)` (the `provisioning` map is gone); the assignment carries typed `Brokers` / `DiskSize` + the `Configuration` map | `004` | ✅ | 2026-09-07 |
| 11.16 | `adapters/out/postgres/{cluster,agent}.go` — drop `provisioning_labels` col + DTO/marshal; add `brokers` / `disk_size` cols to cluster insert/select/scan + `clusterColumns` | — | ✅ | 2026-09-07 |
| 11.17 | `adapters/in/grpcgateway/kafkacluster.go` — Create/Update read `brokers` / `disk_size`; add them to the update-mask switch; `toProto` renders them. `cluster_configuration` handling unchanged | proto | ✅ | 2026-09-07 |
| 11.18 | `adapters/in/grpcgateway/agent.go` — remove `provisioning_labels` from Create/Update/Get/List mapping; `cmd/franz/main.go` — drop references to deleted types | proto | ✅ | 2026-09-07 |
| 11.19 | `pkg/localkafkaagent/recipe/recipe.go` — read `kafka-version` from `Spec`-fed `cluster_configuration` (not a label); read `brokers` from the typed assignment field (`> 1` → warn, single node); drop `DeploymentTypeLabel` / `KafkaImageLabel` / the `franz.provisioning/*` constants; keep the broker-env allow-list, exclude `kafka-version` from it | `004` | ✅ | 2026-09-07 |

### Console (`franz/webconsole`) — after `make gen`

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.20 | Delete `ProvisioningLabelEditor.tsx`, `ProvisioningFields.tsx` (+ test), `provisioning.test.ts`; rewrite `provisioning.ts` → `clusterConfig.ts` — `DEFAULT_KAFKA_CONFIG_PREFIX = "franz.default-kafka-config/"`; `defaultsFromAgent(agent)` → `{config: Record<string,string>, versions: string[], defaultVersion: string}` (splits scalar keys from `available-versions` / `kafka-version`); a `FALLBACK_CONFIG` (partitions, replication-factor) | — | ✅ | 2026-09-07 |
| 11.21 | `ClusterRegister.tsx` / `ClusterEdit.tsx` — a **Cluster configuration** section: a pre-filled `key = value` textarea (from `defaultsFromAgent().config`) + a **kafka-version `<select>`** (options = `versions`, default = `defaultVersion` → written into the config map as `kafka-version`) + number input **brokers** + text input **disk size**. Remove the *Provisioning intent* section entirely. `cluster_configuration` = parsed textarea ∪ `{kafka-version}`; `brokers` / `disk_size` are typed request fields | — | ✅ | 2026-09-07 |
| 11.22 | Keep a **Labels** section — the generic `LabelEditor` for free-form + `franz.placement/*` cluster metadata (no schema, no provisioning rows) | — | ✅ | 2026-09-07 |
| 11.23 | `ClusterDetail.tsx` — show `cluster_configuration` + `brokers` + `disk_size`; `AgentRegister.tsx` / `AgentEdit.tsx` — drop `ProvisioningLabelEditor`; `franz.default-kafka-config/*` are ordinary `LabelEditor` rows | — | ✅ | 2026-09-07 |
| 11.24 | `api/hooks.ts` — remove `ProvisioningLabelSpec` type + `provisioningLabels`; add `brokers` / `diskSize` to the cluster create/update inputs. Regenerate the typed client. Update `ClusterEdit.test.tsx` + the Playwright edit round-trip; delete `ProvisioningFields.test.tsx` | — | ✅ | 2026-09-07 |

### Downstream plan / doc updates

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.25 | `13-placement` task 13.3 — revert to "seed `partitions` / `replication_factor` / `materialized_configuration` from the cluster's `cluster_configuration` map" | — | ✅ | 2026-09-07 |
| 11.26 | `10-async-channel` note — revert the shard-seeding wording to `cluster_configuration` | — | ✅ | 2026-09-07 |
| 11.27 | `franz/README.md` cluster walkthrough — "provisioning fields pre-fill from the agent's schema" → "the config section pre-fills from the agent's `franz.default-kafka-config/*` labels"; `CHANGELOG.md` entry | — | ✅ | 2026-09-07 |

## Done when

- `KafkaCluster` has typed `brokers` / `disk_size` and a `cluster_configuration`
  map; `Create`/`Update` accept all three; `GetKafkaCluster` returns them.
- No `provisioning_labels` / `ProvisioningLabelSpec` anywhere (proto, DB, domain,
  console); no `franz.provisioning/*` in the reserved-label table.
- The cluster form has a **Cluster configuration** section pre-filled from the
  linked agent's `franz.default-kafka-config/*` labels — a `key = value` textarea,
  a version `<select>`, brokers, disk size — plus a plain **Labels** section.
- `WatchClusterAssignments` delivers `cluster_configuration` + typed `brokers` /
  `disk_size`; the `local-docker` recipe builds from those (no `franz.provisioning/*`,
  no `kafka-image`).
- `make gen`, `go build/vet/test ./...`, webconsole `typecheck`/`lint`/`test`,
  Playwright edit round-trip green. `make deps-reset && make dev` boots on the new
  schema; the seeded agent carries `franz.default-kafka-config/*` labels.

## Notes / risks

- **Nothing is enforced.** A `kafka-version` outside the agent's
  `available-versions`, or a config key the recipe doesn't know, is accepted;
  it fails (or warns) downstream at the agent. Matches `003.3`'s "unvalidated
  string" provider link.
- **`deployment-type` removal assumes one recipe family per agent.** If an agent
  ever needs to offer several, re-introduce it as a `franz.default-kafka-config/`
  key or a typed field. The `local-docker` recipe just always builds local-docker.
- **Config-map key naming.** Keys are Franz-friendly (`partitions`, not
  `num.partitions`); the recipe / config-merge translate. Document the map's key
  vocabulary in `003.3` (task 11.3) so it isn't guesswork.
- **`ClusterAssignment` loses the `provisioning` map** — the shipped
  deliverable-05/07 recipe reads it today; task 11.19 moves those reads to the
  typed `brokers` field + the `cluster_configuration` map. Small recipe diff.
- Pre-production: `V1__init.sql` edited in place; dev DBs need `make deps-reset`.
- `buf breaking` flags the removed proto fields — expected, no gate on `main`.

### Deviations from the task list (as built)

- **`cluster.New` signature unchanged.** Instead of adding `brokers` / `disk_size`
  params (10+ call sites), the domain gained `Brokers` / `DiskSize` fields plus a
  single `SetShape(brokers, diskSize) error` (validates `brokers >= 1` when set);
  `clusters.Service` calls it after `New` on Create and inside the Update closure.
- **Recipe config allow-list** became `configKeyToBroker` — a map from a
  `cluster_configuration` key (Franz-friendly *or* raw Kafka) to the broker key it
  sets, or `""` to consume silently (`kafka-version`).
- **`ClusterAssignment.provisioning` (map, field 6)** is `reserved`; `brokers` /
  `disk_size` are fields 7 / 8. `agent_cluster_provider.proto` otherwise unchanged.
- Console: `provisioning.ts` → `clusterConfig.ts` (`defaultsFromAgent`); one
  `ClusterConfiguration` section rendered inline in `ClusterRegister` /
  `ClusterEdit` (no separate component). `ProvisioningFields` /
  `ProvisioningLabelEditor` deleted.
- REST smokes exercised create/GET/PATCH-mask for `brokers` / `disk_size` /
  `cluster_configuration` and agent create/GET without `provisioning_labels`.
- **Follow-up (2026-09-07):** task 11.24's Playwright half was marked done but the
  spec still drove the removed provisioning UI, so the `console e2e` CI job failed.
  `webconsole/e2e/console.spec.ts` now uses the generic `LabelEditor` to advertise
  `franz.default-kafka-config/partitions` on the Cluster Provider agent and asserts
  the pre-filled `cluster_configuration` textarea; the `deployment-type` and
  `kafka-image` steps are gone. Suite run green against a live stack (2 passed).
