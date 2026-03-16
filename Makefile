SERVICES := postgres

FLYWAY := docker-compose run --rm flyway

.PHONY: deps stop-deps wait-for-db migrate migrate-info migrate-repair unit integration test run seed reset-db

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

wait-for-db: deps
	@echo "Waiting for database to be ready..."; \
	until docker-compose exec -T postgres pg_isready -U franz -d franz -q; do sleep 1; done; \
	echo "Database is ready."

migrate: wait-for-db
	$(FLYWAY) migrate

migrate-info: deps
	$(FLYWAY) info

migrate-repair: deps
	$(FLYWAY) repair

unit:
	lein test :franz.unit

integration: migrate
	lein test :franz.integration

test:
	lein test

seed: wait-for-db
	lein seed

reset-db: wait-for-db
	$(FLYWAY) clean migrate


run: wait-for-db
	lein run
