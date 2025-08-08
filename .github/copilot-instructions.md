# Go Simple EventStore

Go Simple EventStore is a lightweight Go library providing a unified interface for event stores across various databases, supporting both in-memory and PostgreSQL backends with event sourcing and CQRS patterns.

**Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.**

## Working Effectively

### Prerequisites and Setup
- Requires Go 1.22 or higher (tested with Go 1.24+)
- Docker and Docker Compose for PostgreSQL integration tests
- PostgreSQL 15+ for integration features

### Build and Test Process
**CRITICAL: NEVER CANCEL any build or test commands. Set timeouts of 5+ minutes.**

Bootstrap and build the repository:
```bash
# Verify dependencies (takes ~1 second)
go mod verify

# Initial build with dependency download (takes ~15 seconds)
go build -v ./...

# Subsequent builds (takes ~1 second)  
make build
```

Run all validation steps:
```bash
# Format code (takes <1 second)
go fmt ./...
make fmt

# Vet code for common errors (takes ~2 seconds)
go vet ./...
make vet

# Run unit tests (takes ~15 seconds, NEVER CANCEL)
go test -race -coverprofile=coverage.out -covermode=atomic ./...
make test-unit

# Start PostgreSQL for integration tests (takes ~7 seconds)
make start-postgres

# Run integration tests (takes ~5 seconds, NEVER CANCEL)
go test -tags=integration -race ./integration_test -v
make test-integration

# Run all tests (takes ~20 seconds total, NEVER CANCEL)
make test

# Stop PostgreSQL when done
make stop-postgres
```

### Running Examples
All examples work correctly and demonstrate real functionality:

```bash
# In-memory example (takes <1 second)
make run-hello-world
cd examples/hello-world && go run main.go

# Consumer/subscription example (takes ~1 second)  
make run-consumer-example
cd examples/consumer-example && go run main.go

# PostgreSQL example (requires PostgreSQL running, takes <1 second)
make run-postgres-example
cd examples/postgres-example && \
TEST_DATABASE_URL="host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable" \
go run main.go
```

## Manual Validation Requirements

**Always manually validate changes by running complete end-to-end scenarios after making code modifications.**

### Core EventStore Validation
After changes to core EventStore interface or implementations:
1. Run `make run-hello-world` - verify events are appended and loaded correctly
2. Run `make run-consumer-example` - verify event consumption and subscriptions work
3. Test optimistic concurrency by running postgres example twice
4. Verify version-based loading works in both forward and reverse directions

### PostgreSQL Integration Validation  
After changes to PostgreSQL adapter:
1. Start fresh PostgreSQL: `make stop-postgres && make start-postgres`
2. Run integration tests: `make test-integration`
3. Run PostgreSQL example: `make run-postgres-example`
4. Verify schema initialization, concurrent access, and subscription polling

### Consumer/Subscription Validation
After changes to EventConsumer or EventSubscription:
1. Run consumer example: `make run-consumer-example`
2. Verify both polling (`Retrieve`) and real-time (`Subscribe`) work
3. Test timestamp-based filtering and batch size limits
4. Verify subscription cleanup and error handling

## CI/CD Requirements

Always run these commands before committing or the CI workflows will fail:
```bash
# Required for CI success (takes ~20 seconds total)
go mod verify
go build -v ./...
go vet ./...
go fmt ./...
make test-unit
make test-integration  # Requires PostgreSQL
```

CI workflow runs on Go 1.22 and 1.23 with PostgreSQL 15-alpine service.

## Repository Structure and Key Locations

### Core Interface Files
- `eventstore.go` - Main EventStore interface and Event struct  
- `consumer.go` - EventConsumer and EventSubscription interfaces

### Implementation Packages
- `memory/` - In-memory adapter for testing/development
  - `memory.go` - InMemoryEventStore implementation
  - `memory_test.go` - Unit tests
- `postgres/` - PostgreSQL adapter for production
  - `postgres.go` - PostgresEventStore implementation  
  - `consumer.go` - PostgresEventConsumer implementation
  - `postgres_test.go` - Unit tests
  - `store.go` - Database schema and utilities

### Examples and Tests
- `examples/hello-world/` - Basic EventStore usage demo
- `examples/consumer-example/` - Event consumption and subscriptions demo  
- `examples/postgres-example/` - PostgreSQL backend demo
- `integration_test/` - Integration tests requiring PostgreSQL
  - `docker-compose.test.yaml` - PostgreSQL test setup

### Build and Configuration
- `Makefile` - Build automation with helpful targets
- `go.mod` - Go module definition (requires github.com/lib/pq v1.10.9)
- `.github/workflows/` - CI/CD workflows for main and PR branches

## Common Development Patterns

### Adding New Adapters
When implementing new database adapters:
1. Implement the `EventStore` interface from `eventstore.go`
2. Implement the `EventConsumer` interface from `consumer.go` if supporting consumption
3. Add comprehensive unit tests following `memory/memory_test.go` pattern
4. Add integration tests in `integration_test/` if needed
5. Add example in `examples/` demonstrating usage
6. Update README.md with adapter documentation

### Event Stream Operations
Key patterns used throughout the codebase:
- **Append-only storage** - Events are never modified, only appended
- **Stream-based organization** - Events grouped by streamID  
- **Version-based concurrency** - expectedVersion for optimistic locking
- **Cursor-based loading** - ExclusiveStartVersion for pagination
- **Timestamp-based consumption** - FromTimestamp for event consumers

### Testing Strategy
- Unit tests in `*_test.go` files test individual adapters
- Integration tests in `integration_test/` test real database interactions
- Examples serve as both documentation and integration validation
- CI runs both unit and integration tests on multiple Go versions

## Troubleshooting Common Issues

### PostgreSQL Connection Issues
- Ensure Docker is running: `docker version`
- Check PostgreSQL status: `docker compose -f integration_test/docker-compose.test.yaml ps`
- Restart if needed: `make stop-postgres && make start-postgres`
- Default connection: `host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable`

### Build or Test Failures
- Run `go mod verify` to check dependencies
- Run `go clean` to clear build cache
- Ensure Go version 1.22+: `go version` 
- Check for formatting issues: `go fmt ./...`
- Run `go vet ./...` for static analysis

### Coverage Reports
- Unit tests generate `coverage.out` file
- Memory adapter has ~94% coverage
- PostgreSQL adapter has ~15% coverage (integration tests provide additional coverage)
- Examples have 0% unit test coverage (this is expected)

Always validate your changes by running the complete test suite and manually exercising the functionality through the provided examples.