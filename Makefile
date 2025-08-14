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
	@docker compose run --rm ruby bundle

start.dev: ## Start the development environment
	@make processors.up
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

api.test.payments: ## Test POST /payments endpoint via proxy
	@./scripts/test-api-payments.sh

api.test.summary: ## Test GET /payments-summary endpoint via proxy
	@./scripts/test-api-summary.sh

api.test.purge: ## Test POST /purge-payments endpoint via proxy
	@./scripts/test-api-purge.sh

api.test.e2e: ## Run end-to-end tests for the API
	@./scripts/e2e.sh

rinha: ## Run k6 performance test (Rinha de Backend)
	@./scripts/reset.sh
	@./scripts/rinha.sh

rinha.official: ## Run official Rinha test with scoring
	@./scripts/reset.sh
	@./scripts/run-local-test.sh

docker.build: ## Build the docker image
	@docker build -t renatoelias/pru-async --target prod --platform linux/amd64 .

docker.push: ## Push the docker image
	@docker push renatoelias/pru-async

rinha.official.prod: ## Run official Rinha using the production image
	@docker compose -f docker-compose.prod.yml down --remove-orphans
	@docker compose -f docker-compose.processor.yml down --remove-orphans

	# Build and push the image
	@make docker.build
	@make docker.push
	@docker rmi renatoelias/pru-async

	@docker compose -f docker-compose.processor.yml up -d
	@docker compose -f docker-compose.prod.yml up -d nginx

	@./scripts/purge-processors.sh
	@./scripts/test-api-purge.sh
	@./scripts/run-local-test.sh
