SERVICES := postgres

FLYWAY      := docker-compose run --rm flyway
FLYWAY_TEST := docker-compose run --rm flyway-test

.PHONY: deps stop-deps wait-for-db migrate migrate-info migrate-repair unit integration test run seed reset-db db-reset create-test-db migrate-test db-reset-test

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

create-test-db: wait-for-db
	@docker-compose exec -T postgres psql -U franz -c "CREATE DATABASE franz_test;" 2>/dev/null || echo "franz_test already exists"

migrate-test: create-test-db
	$(FLYWAY_TEST) migrate

db-reset-test:
	@echo "deleting database franz_test"
	@docker-compose exec -T postgres psql -U franz -c "DROP DATABASE franz_test;" 2>/dev/null || echo "franz_test already dropped"

integration: db-reset-test migrate-test
	lein test :franz.integration

test: unit integration

seed: wait-for-db
	lein seed

reset-db: wait-for-db
	$(FLYWAY) clean migrate

db-reset: wait-for-db
	@docker-compose exec -T postgres psql -U franz -d franz -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	$(FLYWAY) migrate
	lein seed


run: wait-for-db
	lein run
