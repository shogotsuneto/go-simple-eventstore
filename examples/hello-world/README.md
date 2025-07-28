# Hello World Example

This example demonstrates how to use the go-simple-eventstore library with different backends.

## Usage

### Using In-Memory Backend (Default)

```bash
go run main.go
```

or explicitly:

```bash
go run main.go -backend=memory
```

### Using PostgreSQL Backend

First, start the PostgreSQL service using Docker Compose:

```bash
# From the project root
cd integration_test
docker-compose -f docker-compose.test.yaml up -d postgres
```

Wait for PostgreSQL to be ready, then run:

```bash
go run main.go -backend=postgres
```

You can also specify a custom PostgreSQL connection string:

```bash
go run main.go -backend=postgres -postgres-conn="host=localhost port=5432 user=myuser password=mypass dbname=mydb sslmode=disable"
```

## Command-line Options

- `-backend`: Choose the event store backend (`memory` or `postgres`)
- `-postgres-conn`: PostgreSQL connection string (only used with `-backend=postgres`)

## What the Example Does

1. Creates an event store using the specified backend
2. Defines and appends domain events (UserCreated and UserEmailChanged) to a stream
3. Loads and displays the events from the stream
4. Demonstrates version-based event loading
5. Shows how the same interface works across different backends