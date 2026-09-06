# Franz — local development.
#
#   make dev        control plane + web console + Postgres, all wired, Ctrl-C stops it
#   make run        just the control plane (starts Postgres first)
#   make console    just the web console (expects the control plane on :8080)
#   make help       every target
#
# Requires: Go 1.25, Docker (compose v2), Node 20. `make gen` also needs buf.

SHELL := bash
.DEFAULT_GOAL := help

FRANZ_BIN   := bin/franz
COMPOSE     := docker compose
NPM         := npm --prefix webconsole
GATEWAY_URL := http://localhost:8080
CONSOLE_URL := http://localhost:5173

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- dependencies -----------------------------------------------------------

.PHONY: deps
deps: ## Start Postgres and wait for it to be ready
	@$(COMPOSE) up -d --wait postgres 2>/dev/null || { \
		$(COMPOSE) up -d postgres; \
		echo "waiting for Postgres…"; \
		for i in $$(seq 1 30); do \
			$(COMPOSE) exec -T postgres pg_isready -U franz -d franz >/dev/null 2>&1 && break; \
			sleep 1; \
		done; }
	@echo "Postgres ready on localhost:5432"

.PHONY: deps-down
deps-down: ## Stop Postgres (keep the data volume)
	@$(COMPOSE) down

.PHONY: deps-reset
deps-reset: ## Stop Postgres and drop the data volume
	@$(COMPOSE) down -v

webconsole/node_modules: webconsole/package-lock.json ## Install console dependencies
	@$(NPM) install
	@touch webconsole/node_modules

.PHONY: install
install: webconsole/node_modules ## Install all dependencies (Go modules + console)
	@go mod download

# --- codegen --------------------------------------------------------------

.PHONY: gen
gen: ## Regenerate protobuf Go stubs + OpenAPI + the console's typed client
	@command -v buf >/dev/null || { echo "buf not found — go install github.com/bufbuild/buf/cmd/buf@latest"; exit 1; }
	buf generate api
	@$(NPM) run gen:api

# --- run -----------------------------------------------------------------

.PHONY: build-franz
build-franz: ## Build the control-plane binary to bin/franz
	@go build -o $(FRANZ_BIN) ./cmd/franz

.PHONY: run
run: deps build-franz ## Run the control plane (Postgres + gateway on :8080, gRPC on :9090)
	@echo "→ control plane: $(GATEWAY_URL)  (gRPC :9090)"
	@FRANZ_DB__HOST=localhost ./$(FRANZ_BIN)

.PHONY: console
console: webconsole/node_modules ## Run the web console dev server (proxies /v1 to :8080)
	@echo "→ web console: $(CONSOLE_URL)"
	@$(NPM) run dev

AGENT_NAME ?= local-kafka-agent

.PHONY: agent
agent: ## Run the local-kafka-docker-agent (self-registers with Franz; override with TOKEN=/NAME=)
	@FRANZ_ENDPOINT=localhost:9090 FRANZ_AGENT_NAME=$(AGENT_NAME) \
		$(if $(TOKEN),FRANZ_TOKEN=$(TOKEN),FRANZ_REGISTER=1) \
		go run ./cmd/local-kafka-agent

.PHONY: dev
dev: deps build-franz webconsole/node_modules ## Run control plane + console together (Ctrl-C stops both)
	@echo "──────────────────────────────────────────────"
	@echo " control plane : $(GATEWAY_URL)  (gRPC :9090)"
	@echo " web console   : $(CONSOLE_URL)"
	@echo " Postgres      : localhost:5432"
	@echo " Ctrl-C stops both."
	@echo "──────────────────────────────────────────────"
	@FRANZ_DB__HOST=localhost ./$(FRANZ_BIN) & echo $$! > .franz.pid; \
	trap 'kill $$(cat .franz.pid) 2>/dev/null; rm -f .franz.pid' EXIT INT TERM; \
	until curl -sf $(GATEWAY_URL)/healthz >/dev/null; do sleep 0.3; done; \
	echo "control plane up — starting console"; \
	$(NPM) run dev

# --- checks ------------------------------------------------------------

.PHONY: test
test: ## Run all tests (Go + console). Set FRANZ_TEST_DB_DSN for DB integration tests.
	go test ./...
	@$(NPM) run test

.PHONY: e2e
e2e: deps build-franz webconsole/node_modules ## Run the Playwright console smoke against a live stack
	@FRANZ_DB__HOST=localhost ./$(FRANZ_BIN) & echo $$! > .franz.pid; \
	trap 'kill $$(cat .franz.pid) 2>/dev/null; rm -f .franz.pid' EXIT INT TERM; \
	until curl -sf $(GATEWAY_URL)/healthz >/dev/null; do sleep 0.3; done; \
	$(NPM) run e2e

.PHONY: agent-e2e
agent-e2e: deps build-franz ## Real-Docker agent smoke: broker up in Docker, client creates a topic, teardown
	@command -v docker >/dev/null || { echo "docker required"; exit 1; }
	@FRANZ_DB__HOST=localhost ./$(FRANZ_BIN) & echo $$! > .franz.pid; \
	trap 'kill $$(cat .franz.pid) 2>/dev/null; rm -f .franz.pid' EXIT INT TERM; \
	until curl -sf $(GATEWAY_URL)/healthz >/dev/null; do sleep 0.3; done; \
	FRANZ_AGENT_E2E=1 go test -count=1 -timeout 8m -run TestLocalDockerEndToEnd ./pkg/localkafka/

.PHONY: lint
lint: ## gofmt + go vet + console lint/typecheck
	@test -z "$$(gofmt -l cmd pkg migrations)" || { echo "gofmt:"; gofmt -l cmd pkg migrations; exit 1; }
	go vet ./...
	@$(NPM) run lint
	@$(NPM) run typecheck

.PHONY: clean
clean: ## Remove build artefacts
	@rm -rf bin webconsole/dist .franz.pid
