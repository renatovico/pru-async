SHELL = /bin/bash
.ONESHELL:
.DEFAULT_GOAL: help

help: ## Prints available commands
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n"} /^[.a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

processors.up: ## Start the payment processor service
	docker compose -f docker-compose.processor.yml up -d

processors.down: ## Stop the payment processor service
	docker compose -f docker-compose.processor.yml down --remove-orphans

processors.logs: ## View logs for the payment processor service
	docker compose -f docker-compose.processor.yml logs -f

compose.down: ## Stop all services and remove containers
	@docker compose down --remove-orphans

compose.logs: ## View logs for all services
	@docker compose logs -f

api.setup: ## Set up the API service
	@docker compose build
	@docker compose run --rm api01 bundle

start.dev: ## Start the development environment
	@docker compose up -d nginx

api.bash: ## Open a bash shell in the API container
	@docker compose run --rm api01 bash

docker.stats: ## Show docker stats
	@docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

processors.test: ## Test payment processor endpoints (health and payments)
	@./scripts/test-health.sh
	@./scripts/test-processors.sh

processors.purge: ## Purge payment processor data
	@./scripts/purge-processors.sh

api.test.payments: ## Test POST /payments endpoint via nginx
	@./scripts/test-api-payments.sh

api.test.summary: ## Test GET /payments-summary endpoint via nginx
	@./scripts/test-api-summary.sh
