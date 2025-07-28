.PHONY: help build test test-unit test-integration test-all clean start-postgres stop-postgres

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build
build: ## Build the project
	go build -v ./...

# Testing
test: test-unit test-integration ## Run all tests

test-unit: ## Run unit tests only
	go test -race -coverprofile=coverage.out -covermode=atomic ./...

test-integration: ## Run integration tests only (requires PostgreSQL)
	@echo "Running integration tests..."
	@if ! docker compose -f integration_test/docker-compose.test.yaml ps | grep -q "Up"; then \
		echo "PostgreSQL is not running. Starting it..."; \
		make start-postgres; \
		sleep 5; \
	fi
	TEST_DATABASE_URL="host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable" \
	go test -tags=integration -race -v ./integration_test

test-all: test-unit test-integration ## Run both unit and integration tests

# PostgreSQL management
start-postgres: ## Start PostgreSQL for integration testing
	docker compose -f integration_test/docker-compose.test.yaml up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@timeout 30 sh -c 'until docker compose -f integration_test/docker-compose.test.yaml exec postgres pg_isready -U test -d eventstore_test > /dev/null 2>&1; do sleep 1; done'
	@echo "PostgreSQL is ready!"

stop-postgres: ## Stop PostgreSQL
	docker compose -f integration_test/docker-compose.test.yaml down

# Clean up
clean: ## Clean build artifacts and stop services
	go clean
	rm -f coverage.out
	docker compose -f integration_test/docker-compose.test.yaml down -v

# Development workflow
dev-setup: start-postgres ## Set up development environment
	@echo "Development environment ready!"

dev-test: ## Run tests in development (starts postgres if needed)
	@if ! docker compose -f integration_test/docker-compose.test.yaml ps | grep -q "Up"; then \
		make start-postgres; \
	fi
	make test-all

# Linting and formatting
vet: ## Run go vet
	go vet ./...

fmt: ## Format code
	go fmt ./...

# Examples
run-hello-world: ## Run the hello-world example
	cd examples/hello-world && go run main.go

run-postgres-example: start-postgres ## Run the PostgreSQL example
	cd examples/postgres-example && \
	TEST_DATABASE_URL="host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable" \
	go run main.go