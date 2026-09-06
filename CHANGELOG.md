# Changelog

All notable changes to Franz are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

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

### Changed

- Repository reset from the Clojure implementation to the Go monorepo on the
  `go-monorepo` branch.
