.PHONY: dev-setup

dev-setup:
	sudo bash scripts/dev_setup.sh

dev-dep-start:
	docker-compose -f docker-compose.yaml up -d

dev-dep-stop:
	docker-compose -f docker-compose.yaml down

dev-dep-logs:
	docker-compose -f docker-compose.yaml logs -f

