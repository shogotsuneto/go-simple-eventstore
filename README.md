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

type EventConsumer interface {
    // Retrieve retrieves events from all streams in a table
    Retrieve(opts ConsumeOptions) ([]Event, error)
    // Subscribe creates a subscription to all streams in a table
    Subscribe(opts ConsumeOptions) (EventSubscription, error)
}

type EventSubscription interface {
    // Events returns a channel that receives events as they are appended to the stream
    Events() <-chan Event
    // Errors returns a channel that receives any errors during subscription
    Errors() <-chan error
    // Close stops the subscription and releases resources
    Close() error
}
```

## ⚙️ Configuration Options

### LoadOptions

Used when loading events from a specific stream via `EventStore.Load()`:

```go
type LoadOptions struct {
    // ExclusiveStartVersion specifies the version to use as exclusive starting point for loading events
    // - In forward loading (Desc=false): gets events with version > ExclusiveStartVersion
    // - In reverse loading (Desc=true): gets events with version < ExclusiveStartVersion (or all if 0)
    ExclusiveStartVersion int64
    
    // Limit specifies the maximum number of events to return
    // - Use 0 for no limit (load all available events)
    // - Use positive integer to limit the batch size
    Limit int
    
    // Desc specifies whether to load events in descending order (from latest to oldest)
    // - When true, loads events in descending order starting from the latest version
    // - When false (default), loads events in ascending order
    Desc bool
}
```

### ConsumeOptions

Used when consuming events from all streams in a table via `EventConsumer.Retrieve()` and `EventConsumer.Subscribe()`:

```go
type ConsumeOptions struct {
    // FromTimestamp specifies where to start consuming events from
    // - Events with timestamps equal to or after this time will be included
    // - Use time.Time{} to start from the earliest available events
    FromTimestamp time.Time
    
    // FromOffset specifies the table-level offset to start consuming events from
    // - Events with offset greater than this value will be included
    // - If both FromTimestamp and FromOffset are specified, FromOffset takes precedence
    // - Use 0 to start from the earliest available events
    FromOffset int64
    
    // BatchSize specifies the maximum number of events to return in each batch
    // - For Retrieve(): maximum events returned per call
    // - For Subscribe(): maximum events delivered per notification
    BatchSize int
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
    
    // Load events from the stream (forward order - default behavior)
    loadedEvents, err := store.Load("user-123", eventstore.LoadOptions{
        ExclusiveStartVersion: 0,
        Limit: 10,
    })
    if err != nil {
        panic(err)
    }
    
    // Load latest events in descending order (newest first)
    latestEvents, err := store.Load("user-123", eventstore.LoadOptions{
        ExclusiveStartVersion: 0,
        Limit: 5,    // Get latest 5 events
        Desc: true,
    })
    if err != nil {
        panic(err)
    }
    
    // Process loaded events...
}
```

### Consuming Events with Retrieve

```go
// Retrieve events from all streams in the table using timestamp
events, err := store.Retrieve(eventstore.ConsumeOptions{
    FromTimestamp: time.Now().Add(-24 * time.Hour), // Last 24 hours
    BatchSize:     100,
})
if err != nil {
    panic(err)
}

// Or retrieve events using table-level offset (more reliable)
events, err = store.Retrieve(eventstore.ConsumeOptions{
    FromOffset: 12345, // All events after offset 12345
    BatchSize:  100,
})
if err != nil {
    panic(err)
}

for _, event := range events {
    // Process each event...
    fmt.Printf("Event offset: %d, type: %s\n", event.Offset, event.Type)
}
```

### Consuming Events with Subscriptions

```go
// Subscribe to events from all streams in the table using timestamp
subscription, err := store.Subscribe(eventstore.ConsumeOptions{
    FromTimestamp: time.Now(), // From now onwards
    BatchSize:     10,
})
if err != nil {
    panic(err)
}
defer subscription.Close()

// Or subscribe using table-level offset for more reliable consumption
subscription, err = store.Subscribe(eventstore.ConsumeOptions{
    FromOffset: 12345, // From offset 12345 onwards
    BatchSize:  10,
})
if err != nil {
    panic(err)
}
defer subscription.Close()

// Handle events as they arrive
go func() {
    for {
        select {
        case event := <-subscription.Events():
            // Process event in real-time
            fmt.Printf("Received: %s (offset: %d)\n", event.Type, event.Offset)
        case err := <-subscription.Errors():
            // Handle subscription errors
            fmt.Printf("Error: %v\n", err)
        }
    }
}()
```

### Using PostgreSQL Backend

```go
package main

import (
    "database/sql"
    "time"
    _ "github.com/lib/pq"
    
    "github.com/shogotsuneto/go-simple-eventstore"
    "github.com/shogotsuneto/go-simple-eventstore/postgres"
)

func main() {
    // Open database connection
    db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=password dbname=eventstore sslmode=disable")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // Initialize schema
    if err := postgres.InitSchema(db, "events"); err != nil {
        panic(err)
    }
    
    // Create producer and consumer
    store := postgres.NewPostgresEventStore(db, "events")
    consumer := postgres.NewPostgresEventConsumer(db, "events", 2*time.Second)
    
    // Use the same interface as before...
}
```

### Running the Examples

See the examples directory for complete demonstrations:

- [hello-world example](examples/hello-world/) - Basic event store operations
- [consumer example](examples/consumer-example/) - Event consumption with polling and subscriptions  
- [offset example](examples/offset-example/) - Table-level offset-based consumption
- [postgres example](examples/postgres-example/) - PostgreSQL backend usage

```bash
# Run the basic hello-world example
make run-hello-world

# Run the consumer example (polling and subscriptions)
make run-consumer-example

# Run the offset example (demonstrates table-level sequence numbers)
make run-offset-example

# Run the PostgreSQL example
make run-postgres-example
```

## 📋 MVP Scope

This project focuses on the essential functionality needed for event sourcing:

1. **Append-only event storage** - Events are never modified, only appended
2. **Stream-based organization** - Events are organized by stream ID
3. **Cursor-based loading** - Efficient event retrieval with pagination support
4. **Database agnostic** - Unified interface across different storage backends
5. **Event consumption** - Support for both polling and subscription-based event consumption
6. **Real-time projections** - Subscribe to events as they are appended for live updates
7. **Table-level sequence numbers** - Reliable cross-stream ordering with offset-based consumption

## 🔢 Table-Level Offset Support

In addition to timestamp-based consumption, this library supports table-level sequence numbers (offsets) for more reliable event consumption:

### Benefits of Offset-Based Consumption

- **Deterministic ordering** - Events are ordered by a table-level sequence number
- **No duplicates** - Unlike timestamp-based consumption, offset guarantees uniqueness
- **Database compatibility** - Works reliably across different databases (PostgreSQL SERIAL, DynamoDB counters, etc.)
- **Perfect for projections** - Ideal for building reliable read models and event projections

### Usage

```go
// Events include both timestamp and offset
type Event struct {
    ID        string
    Type      string
    Data      []byte
    Metadata  map[string]string
    Timestamp time.Time
    Version   int64  // Stream-level version
    Offset    int64  // Table-level sequence number
}

// Consume using offset instead of timestamp
events, err := consumer.Retrieve(eventstore.ConsumeOptions{
    FromOffset: 12345, // Get all events after offset 12345
    BatchSize:  100,
})

// Subscribe using offset for reliable real-time consumption
subscription, err := consumer.Subscribe(eventstore.ConsumeOptions{
    FromOffset: 12345, // Start from offset 12345
    BatchSize:  10,
})
```

### Implementation Details

- **Memory backend**: Uses atomic counter starting at 1
- **PostgreSQL backend**: Leverages existing `SERIAL PRIMARY KEY` column
- **Backward compatibility**: Timestamp-based consumption continues to work
- **Precedence**: When both `FromOffset` and `FromTimestamp` are specified, offset takes precedence

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
