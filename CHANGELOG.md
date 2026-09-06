# Changelog

All notable changes to Franz are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Agent registry** (impls_plan deliverable 04): `core/domain/agent` (entity,
  `AgentType` organisational filter, `ACTIVE ↔ PAUSED → DELETED` status machine),
  `core/ports/{in,out}` + `core/usecases/agents`, a hand-written pgx adapter
  (`adapters/out/postgres/agent.go`, type filter pushed to SQL), and the
  `AgentService` gRPC + REST handler (`/v1/kafka/agents` with `:pause` /
  `:resume` / `:rotateToken`). `CreateAgent` mints a one-time bearer token
  (`pkg/shared/token`: `frnat_` + 32 random bytes; only the sha256 is stored);
  `RotateAgentToken` replaces it. New `agent` table with a `token_hash` column.
  Registration is inert — no connection, no work protocol (that is a later ADR).
- **Kafka Cluster** (impls_plan deliverable 03): the first full
  `domain → ports → postgres → grpc-gateway` vertical slice —
  `core/domain/cluster` (entity + `ACTIVE ↔ PAUSED → DELETED` state machine),
  `core/ports/in.KafkaClusterService` / `core/ports/out.ClusterRepository`,
  `core/usecases/clusters`, a hand-written pgx adapter
  (`adapters/out/postgres/cluster.go`, `Mutate` = `SELECT … FOR UPDATE` in one
  transaction), and the `KafkaClusterService` gRPC + REST handler
  (`adapters/in/grpcgateway/kafkacluster.go`, `/v1/kafka/clusters` with
  `:pause` / `:resume`). New `kafka_cluster` table (`migrations/V1__init.sql`).
  Soft delete; `(realm_id, name)` unconditionally unique; label-selector `List`
  with opaque pagination. `ListClusterProviderEvents` is left `Unimplemented`
  until deliverable 05.
- `pkg/shared/fieldmask`: `CanonicalPaths` helper; `update_mask` added to the
  immutable set.
- **Project scaffolding** (impls_plan deliverable 01): Go module
  `github.com/KafkaMetamorphosis/franz` rooted at `franz/`; hexagonal package
  skeleton (`cmd/franz`, `pkg/franz/core/{domain,usecases,ports}`,
  `pkg/franz/adapters/{in/grpcgateway,out/postgres}`, `pkg/franz/config`,
  `pkg/shared`); `buf` codegen to committed `pkg/gen/go`; `docker-compose.yml`
  (Postgres 16 + Flyway) with an empty `migrations/V1__init.sql`;
  `cmd/franz` `fx` application booting a gRPC server + grpc-gateway mux + a
  `GET /healthz` probe; `koanf` config (`config.yaml` + `FRANZ_` env overrides);
  GitHub Actions CI (`buf lint`/`buf breaking`, `go vet`/`build`/`test`,
  generated-code freshness check).
- **Domain foundations** (impls_plan deliverable 02): the `003.1` cross-cutting
  primitives every later deliverable reuses —
  the `FRN` (Franz Resource Name) value object (`core/domain/frn`) with a
  `Codec` for the configurable `resource_prefix` (default `frn`; `frn:` / `orn:`
  accepted as aliases; FRNs stored prefix-less) — ADR-API-007,
  resource-name validation (`core/domain/naming`),
  the label-selector grammar + matcher (`core/domain/selector`) and `*` / `\*`
  glob (`pkg/shared/glob`),
  a transport-free domain error vocabulary (`core/domain/errs`) with a
  gRPC-status + `google.rpc.BadRequest` mapper (`adapters/in/grpcgateway/errmap.go`),
  an opaque pagination-cursor codec (`pkg/shared/pagetoken`, default 50 / cap 1000),
  a `google.protobuf.FieldMask` apply helper (`pkg/shared/fieldmask`; empty-mask
  reject, `name` immutable, map/repeated wholesale),
  hand-written `pgx/v5` Postgres plumbing (`adapters/out/postgres/db.go`:
  pool, `WithTx`, embedded Flyway-compatible migrations run on boot when
  `db.auto_migrate` is set),
  the `Realm` value object + request-context plumbing (`core/domain/realm`) with
  a repository (`core/ports/out`, `adapters/out/postgres/realm.go`) and the
  seeded `default` realm in `migrations/V1__init.sql`,
  and an allow-all authenticator (`adapters/in/grpcgateway/interceptor.go`) that
  resolves the realm into context on every inbound path (gRPC unary + stream,
  gateway HTTP) — the `003.2` seam.

### Changed

- `config`: added `db.auto_migrate` (default `true`) and `resource_prefix`
  (default `frn`; an invalid value fails the boot).
- Proto: the resource-identifier field is `frn` (was `orn`); likewise
  `client_frn` / `resource_frn` / `cluster_frn`. `pkg/gen/go` regenerated.
- `grpcgateway.New` takes functional options; `WithAuthenticator` installs the
  realm interceptors.
- Repository reset from the Clojure implementation to the Go monorepo on the
  `go-monorepo` branch.
