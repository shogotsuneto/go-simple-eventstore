# go-simple-eventstore

A lightweight Go library providing a unified interface for event stores across various databases.

## 🎯 Project Vision

The goal of this project is to create a **simple, minimal, and extensible** interface for storing and loading events in an **append-only** fashion, regardless of the underlying database.

### Why?

- Event Sourcing and CQRS patterns often require an event store.
- Existing solutions are either complex or tied to a single backend.
- This library aims to stay **minimal** and **clear**, focusing on the core: `Append` and `Load`.

## 🔧 Core Interface

```go
package eventstore

type EventStore interface {
    // Append adds new events to the given stream.
    // expectedVersion is used for optimistic concurrency control:
    // - If expectedVersion is -1, the stream can be in any state (no concurrency check)
    // - If expectedVersion is 0, the stream must not exist (stream creation)  
    // - If expectedVersion > 0, the stream must be at exactly that version
    Append(streamID string, events []Event, expectedVersion int) error

    // Load retrieves events for the given stream using the specified options.
    Load(streamID string, opts LoadOptions) ([]Event, error)
}
```

## 🔌 Backend Adapters

### Implemented
- **In-Memory** - Simple in-memory implementation (suitable for testing and development)
- **PostgreSQL** - Reliable relational database adapter with full ACID compliance

### Work In Progress (WIP)
- **DynamoDB** - AWS NoSQL database adapter (WIP)
- **More adapters coming** - Extensible design allows for easy addition of new database backends

## 🚀 Getting Started

### Quick Start with In-Memory Backend

```go
package main

import (
    "github.com/shogotsuneto/go-simple-eventstore"
    "github.com/shogotsuneto/go-simple-eventstore/memory"
)

func main() {
    // Create an in-memory event store
    store := memory.NewInMemoryEventStore()
    
    // Define some events
    events := []eventstore.Event{
        {
            Type: "UserCreated",
            Data: []byte(`{"user_id": "123", "name": "John Doe"}`),
            Metadata: map[string]string{"source": "user-service"},
        },
    }
    
    // Append events to a stream
    err := store.Append("user-123", events, -1)
    if err != nil {
        panic(err)
    }
    
    // Load events from the stream
    loadedEvents, err := store.Load("user-123", eventstore.LoadOptions{
        FromVersion: 0,
        Limit: 10,
    })
    if err != nil {
        panic(err)
    }
    
    // Process loaded events...
}
```

### Using PostgreSQL Backend

```go
package main

import (
    "github.com/shogotsuneto/go-simple-eventstore"
    "github.com/shogotsuneto/go-simple-eventstore/postgres"
)

func main() {
    // Create a PostgreSQL event store (default table name is "events")
    store, err := postgres.NewPostgresEventStore(postgres.Config{
        ConnectionString: "host=localhost port=5432 user=postgres password=password dbname=eventstore sslmode=disable",
        TableName:        "my_custom_events", // Custom table name
    })
    if err != nil {
        panic(err)
    }
    defer store.Close()
    
    // Initialize schema with custom table name
    if err := store.InitSchema(); err != nil {
        panic(err)
    }
    
    // Use the same interface as before...
}
```

### Running the Examples

See the [hello-world example](examples/hello-world/) for a complete demonstration of both backends.

### Running PostgreSQL Examples

Make sure you have PostgreSQL running:

```bash
# Start PostgreSQL for testing
make start-postgres

# Run the basic PostgreSQL example
make run-postgres-example
```

## 📋 MVP Scope

This project focuses on the essential functionality needed for event sourcing:

1. **Append-only event storage** - Events are never modified, only appended
2. **Stream-based organization** - Events are organized by stream ID
3. **Cursor-based loading** - Efficient event retrieval with pagination support
4. **Database agnostic** - Unified interface across different storage backends

## 🧪 Testing

### Unit Tests

Run unit tests for all adapters:

```bash
go test ./...
```

### Integration Tests

Integration tests require a PostgreSQL database. You can use Docker Compose to start one:

```bash
cd integration_test
docker compose -f docker-compose.test.yaml up -d postgres
```

Then run the integration tests:

```bash
go test -tags=integration ./integration_test -v
```

## 🤝 Contributing

This project welcomes contributions! The goal is to maintain simplicity while adding support for additional database adapters.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
