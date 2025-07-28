# PostgreSQL Example

This example demonstrates how to use the go-simple-eventstore library with a PostgreSQL backend.

## Prerequisites

- PostgreSQL server running
- Go 1.21 or later

## Setup PostgreSQL

You can run PostgreSQL using Docker:

```bash
docker run --name postgres-eventstore \
  -e POSTGRES_USER=test \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=eventstore_test \
  -p 5432:5432 \
  -d postgres:15
```

## Running the Example

1. Change to the postgres-example directory:
   ```bash
   cd examples/postgres-example
   ```

2. Run with default PostgreSQL connection:
   ```bash
   go run main.go
   ```

3. Run with custom PostgreSQL connection:
   ```bash
   go run main.go -postgres-conn="host=localhost port=5432 user=myuser password=mypass dbname=mydb sslmode=disable"
   ```

## What the Example Does

1. **Connects to PostgreSQL**: Establishes connection to the PostgreSQL database
2. **Initializes Schema**: Creates the necessary tables and indexes
3. **Appends Events**: Stores domain events (UserCreated, UserEmailChanged) in the database
4. **Loads Events**: Retrieves events from the stream with various filtering options
5. **Demonstrates Concurrency Control**: Shows how optimistic concurrency control works with expected versions

## Key Features Demonstrated

- **Schema Management**: Explicit schema initialization
- **Event Persistence**: Events are stored with full ACID compliance
- **Version Control**: Events are versioned within streams
- **Metadata Support**: Custom metadata is stored as JSONB
- **Optimistic Concurrency**: Expected version checks prevent conflicts
- **Query Options**: Loading events with version filtering and limits

## Output

The example produces detailed output showing:
- Connection establishment
- Schema initialization
- Event appending and loading
- Version-based queries
- Concurrency control validation

All events are persisted in PostgreSQL and can be queried using standard SQL tools.