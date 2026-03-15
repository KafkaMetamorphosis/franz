SERVICES := postgres kafka-1 kafka-2 kafka-ui

FLYWAY := docker-compose run --rm flyway

.PHONY: deps stop-deps migrate migrate-info migrate-repair unit integration test run

deps:
	@all_running=true; \
	for svc in $(SERVICES); do \
		status=$$(docker-compose ps --status running --services 2>/dev/null | grep -x "$$svc"); \
		if [ -z "$$status" ]; then \
			all_running=false; \
			break; \
		fi; \
	done; \
	if [ "$$all_running" = true ]; then \
		echo "Dependencies already running"; \
	else \
		echo "Starting dependencies..."; \
		docker-compose up -d; \
	fi

stop-deps:
	docker-compose down

migrate: deps
	$(FLYWAY) migrate

migrate-info: deps
	$(FLYWAY) info

migrate-repair: deps
	$(FLYWAY) repair

unit:
	lein test :only grete-samsa.unit

integration: deps migrate
	lein test :only grete-samsa.integration

test:
	lein test

run: deps
	lein trampoline run
