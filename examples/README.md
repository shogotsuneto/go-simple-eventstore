# Examples

This directory contains example code demonstrating how to use the go-simple-eventstore library with different backends.

## Hello World Example

The `hello-world` example demonstrates the basic usage of the event store with an in-memory backend:

- Creating an in-memory event store
- Appending events to a stream
- Loading events from a stream
- Using version-based filtering

### Running the Example

```bash
cd examples/hello-world
go run main.go
```

## PostgreSQL Example

The `postgres-example` example demonstrates how to use the event store with a PostgreSQL backend:

- Connecting to PostgreSQL database
- Initializing database schema
- Appending events with ACID compliance
- Loading events with complex queries
- Optimistic concurrency control

### Running the Example

```bash
cd examples/postgres-example
go run main.go
```

See the individual README files in each example directory for detailed setup instructions and usage information.