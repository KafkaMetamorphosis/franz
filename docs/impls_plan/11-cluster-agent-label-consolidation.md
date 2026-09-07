# 11 — Cluster & agent label consolidation

Status: ⬜ not started
Depends on: [03](./03-kafka-cluster.md) · [04](./04-agent-registry.md) · [08](./08-resource-management-ui.md)
Specs: `003-franz/003.3-kafka-cluster`, `003-franz/003.1-conventions`, `003-franz/003.6-kafka-topic`, `003-franz/003.9-agents`, `004-local-kafka-docker-agent`, `DECISIONS.md` (new **ADR-API-010**, supersedes ADR-API-008)
Proto: `agent.proto` (remove `ProvisioningLabelSpec` / `provisioning_labels`), `kafka.proto` (`cluster_configuration` output-only)

## Goal

Make **labels the single surface** for a Kafka Cluster's provisioning intent and
Kafka configuration, and make agent-advertised defaults **plain labels** too.

- One `Labels` section on the cluster form — no separate *Provisioning intent* /
  *Context labels* / *Cluster configuration* sections.
- `KafkaCluster.cluster_configuration` stops being independently writable; it is a
  **read-only projection** of the cluster's `franz.kafka-config/*` labels
  (prefix-stripped). The config-merge (`003.6`), placement, the `local-docker`
  recipe, and Gregor Samsa keep consuming a clean `map<string,string>` — Franz
  computes it.
- Agents advertise defaults as `franz.default-provisioning/*` and
  `franz.default-kafka-config/*` on their own `Agent.labels`. The console
  pre-fills a cluster's `franz.provisioning/*` / `franz.kafka-config/*` rows from
  the selected agent's `franz.default-*` labels (copy-at-create, advisory —
  Franz never validates).
- **Remove** the structured `Agent.provisioning_labels` schema (ADR-API-008):
  `ProvisioningLabelSpec`, the proto fields, the `jsonb` column,
  `ValidateProvisioningLabels`, `ProvisioningFields` / `ProvisioningLabelEditor`.
  Accepted loss: no `allowed_values` dropdowns, no `required` enforcement, no
  per-key descriptions.

`num.partitions` and `default.replication.factor` live under `franz.kafka-config/*`
with their real Kafka key names; placement (13) seeds the shard `partitions` /
`replication_factor` from them.

## Tasks

### Specs & ADR (docs repo → main)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.1 | **ADR-API-010** in `DECISIONS.md` — labels are the config/provisioning surface; `cluster_configuration` is a derived read projection; agent `franz.default-*` convention; `num.partitions`/`default.replication.factor` under `franz.kafka-config/*`. Mark **ADR-API-008 superseded** | — | ⬜ | |
| 11.2 | `003.1` reserved-label table — add `franz.kafka-config/*` (Kafka Cluster), `franz.default-provisioning/*` + `franz.default-kafka-config/*` (Agent). State that label selectors (`List.selector`, `franz.affinity/*`, `Principal.labels`) evaluate against **non-`franz.*`** labels only. Note the ≤63-char `<name>` limit applies to the stripped Kafka key | `003.1` | ⬜ | |
| 11.3 | `003.3` — `cluster_configuration` key-field row → "derived, read-only: the `franz.kafka-config/*` subset of `labels`". `labels` row mentions `franz.kafka-config/*`. Config-merge + governance cross-entity bullets: `cluster_configuration` → `franz.kafka-config/*` | `003.3` | ⬜ | |
| 11.4 | `003.6` config-merge — base layer is the cluster's `franz.kafka-config/*` (prefix-stripped), not `cluster_configuration`; `partitions`/`replication_factor` seeded from `franz.kafka-config/num.partitions` / `default.replication.factor` | `003.6` | ⬜ | |
| 11.5 | `003.9` — delete the "Provisioning-label schema" section, the `provisioning_labels` key-field row + invariant; add a short "Advertised defaults" paragraph (`franz.default-*` labels, console-only, never enforced); fix ADR-API-008 → 010 refs | `003.9` | ⬜ | |
| 11.6 | `004-local-kafka-docker-agent` §3 — keep the `franz.provisioning/*` table; replace the `Agent.provisioning_labels` paragraph with the `franz.default-*` convention; note `franz.kafka-config/*` feeds the recipe allow-list (was `cluster_configuration`) | `004` | ⬜ | |
| 11.7 | `003.8` governance whitelist — "edit `cluster_configuration.*` keys" → "edit `franz.kafka-config/*` label keys" | `003.8` | ⬜ | |

### Proto (`franz/api/franz/v1`) — then `make gen`

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.8 | `agent.proto` — remove `message ProvisioningLabelSpec`; remove `provisioning_labels` from `Agent` (8), `CreateAgentRequest` (4), `UpdateAgentRequest` (5); `reserved` those numbers | ADR-API-010 | ⬜ | |
| 11.9 | `kafka.proto` — remove `cluster_configuration` from `CreateKafkaClusterRequest` (4) + `UpdateKafkaClusterRequest` (4), `reserved`; **keep** `KafkaCluster.cluster_configuration` (5), documented output-only (derived). `agent_cluster_provider.proto` **unchanged** — Franz fills `ClusterAssignment.cluster_configuration` from the projection | ADR-API-010 | ⬜ | |
| 11.10 | `buf lint` clean; `buf breaking` documents the removals (pre-production, no gate on `main`) | — | ⬜ | |

### Migration (`V1__init.sql`, edited in place → `make deps-reset`)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.11 | Drop `agent.provisioning_labels jsonb` and `kafka_cluster.cluster_configuration jsonb` columns | `003.12` | ⬜ | |
| 11.12 | `local/seed/01-local-agent.sql` — remove the `provisioning_labels` JSON; put defaults in `labels`: `franz.default-provisioning/{deployment-type=local-docker,kafka-version=3.7.0}`, `franz.default-kafka-config/{num.partitions=3,default.replication.factor=1,log.retention.ms=604800000}` | `004` | ⬜ | |

### Backend (`franz/pkg/franz`)

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.13 | `core/domain/agent` — delete `provisioning.go`, `ProvisioningLabelSpec`, `Agent.ProvisioningLabels`, `SetProvisioningLabels`; drop the `New(...)` param; fix every caller + test | `003.9` | ⬜ | |
| 11.14 | `core/domain/cluster` — drop `Configuration` field + `New(...)` param; add `func (c *Cluster) KafkaConfig() map[string]string` (extract `franz.kafka-config/*`, strip prefix). `ToAssignment()` fills `Configuration: c.KafkaConfig()` | `003.3`, `003.6` | ⬜ | |
| 11.15 | `core/domain/provider` — add `KafkaConfigLabels(labels)` filter (mirror of `ProvisioningLabels`, `franz.kafka-config/` prefix) | `004` | ⬜ | |
| 11.16 | `core/domain/topic/config.go` + `core/usecases/topics` — `Materialize` signature unchanged; callers pass `cluster.KafkaConfig()` where they passed `cluster.Configuration` | `003.6` | ⬜ | |
| 11.17 | `adapters/out/postgres/{cluster,agent}.go` — drop the two columns from insert/select/scan + the `*Columns` lists + the `provisioningLabelRow` DTO / marshal helpers | — | ⬜ | |
| 11.18 | `adapters/in/grpcgateway/kafkacluster.go` — Create/Update ignore request `cluster_configuration`; `toProto` fills it from `KafkaConfig()`; remove it from the update-mask switch (`labels` now covers it) | proto | ⬜ | |
| 11.19 | `adapters/in/grpcgateway/agent.go` — remove `provisioning_labels` from Create/Update/Get/List mapping; `cmd/franz/main.go` — drop references to deleted types | proto | ⬜ | |

### Console (`franz/webconsole`) — after `make gen`

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.20 | Delete `ProvisioningLabelEditor.tsx`, `ProvisioningFields.tsx` (+ test), `provisioning.test.ts` | — | ⬜ | |
| 11.21 | Rewrite `provisioning.ts` → `clusterLabels.ts` — prefix constants (`franz.provisioning/`, `franz.kafka-config/`, `franz.default-provisioning/`, `franz.default-kafka-config/`); `defaultsFromAgent(agent)` rewrites `franz.default-X/<k>` → `franz.X/<k>`; a built-in `FALLBACK_DEFAULTS` (deployment-type, kafka-version, num.partitions, default.replication.factor) | — | ⬜ | |
| 11.22 | New `ClusterLabelsSection.tsx` — one section: a labelled, pre-filled, editable row per known `franz.*` key (agent defaults ∪ fallback), visually grouped (Provisioning / Kafka config / Other) over a single `labels` map; generic add-label row for arbitrary keys | UXD-* | ⬜ | |
| 11.23 | `ClusterRegister.tsx` / `ClusterEdit.tsx` — replace the three sections with `<ClusterLabelsSection>`; submit sends `labels` only (drop `clusterConfiguration`); edit mask = `labels`. Remove `parseKeyValues` / `missingRequired` / `prefilled` / `ProvisioningFields` usage | — | ⬜ | |
| 11.24 | `ClusterDetail.tsx` — render labels grouped; show the derived `cluster_configuration` read-only. `AgentRegister.tsx` / `AgentEdit.tsx` — drop `ProvisioningLabelEditor`; `franz.default-*` are ordinary `LabelEditor` rows | — | ⬜ | |
| 11.25 | `api/hooks.ts` — remove `ProvisioningLabelSpec` type + `provisioningLabels`. Regenerate the typed client. Update `ClusterEdit.test.tsx` + the Playwright edit-round-trip e2e; delete `ProvisioningFields.test.tsx` | — | ⬜ | |

### Downstream plan updates

| # | Task | Ref | Status | Landed |
|---|---|---|---|---|
| 11.26 | `12-gregor-samsa` — shard `desired_config` "from `cluster.KafkaConfig()`"; drop the now-stale `003.1`/`003.9` spec-edit bullets it inherited (this deliverable does them) | — | ⬜ | |
| 11.27 | `13-placement` — task 13.3 already reworded; confirm 13.8 unaffected | — | ⬜ | |
| 11.28 | `franz/README.md` cluster walkthrough — "provisioning fields pre-fill from the agent's schema" → "…from the agent's `franz.default-*` labels"; `CHANGELOG.md` entry | — | ⬜ | |

## Done when

- The cluster form has **one** `Labels` section; picking a provider agent
  pre-fills `franz.provisioning/*` and `franz.kafka-config/*` rows from its
  `franz.default-*` labels, all editable, plus a free-form add-label.
- `GetKafkaCluster` returns `cluster_configuration` computed from
  `franz.kafka-config/*`; `Create`/`Update` ignore any `cluster_configuration` in
  the request.
- `WatchClusterAssignments` still delivers a populated `cluster_configuration`
  map — the `local-docker` recipe and deliverable 05/07 need **no** change.
- `Agent` has no `provisioning_labels` anywhere (proto, DB, domain, console).
- `make gen`, `go build/vet/test ./...`, webconsole `typecheck`/`lint`/`test`,
  and the Playwright edit round-trip are green. `make deps-reset && make dev`
  boots on the new schema and the seeded agent carries `franz.default-*` labels.

## Notes / risks

- **`deployment-type` is now free text.** A typo is not caught at cluster-create;
  the `local-docker` recipe rejects an unknown value at reconcile
  (`unsupported franz.provisioning/deployment-type=…`). Regression from the
  ADR-API-008 dropdown — accepted (user decision 2026-09-06).
- **Two lenses on `franz.kafka-config/*`.** The recipe reads them as *broker*
  settings (its allow-list uses broker key names — `log.retention.ms`,
  `num.partitions`, …); the `003.6` config-merge treats them as *topic-config*
  defaults for new topics. A value like `franz.kafka-config/local.retention.ms`
  reaches the topic but not the broker env. This ambiguity already exists with
  `cluster_configuration`; document it in `003.3` (task 11.3).
- **Copy-at-create, not live inheritance.** Editing an agent's `franz.default-*`
  later does not change existing clusters (matches ADR-API-008's advisory
  posture). The `cluster_provider_agent` link can dangle, so a live fallback
  would be fragile.
- Pre-production: `V1__init.sql` edited in place; dev DBs need `make deps-reset`.
- `buf breaking` will flag the removed proto fields — expected, no gate on `main`.
