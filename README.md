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
    // Returns the latest version number after successful append.
    // For empty appends (no events), always returns 0.
    // expectedVersion is used for optimistic concurrency control:
    // - If expectedVersion is -1, the stream can be in any state (no concurrency check)
    // - If expectedVersion is 0, the stream must not exist (stream creation)  
    // - If expectedVersion > 0, the stream must be at exactly that version
    Append(streamID string, events []Event, expectedVersion int) (int64, error)

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

## 📝 Append Behavior

The `Append` method has the following behavior:

- **Non-empty appends**: Returns the latest version number after successful append (version of the last event added)
- **Empty appends**: Always returns `0`, regardless of the current stream state
  - Empty append on empty stream: returns `0`
  - Empty append on non-empty stream: returns `0` (not the current version)
- **Event updates**: Events passed to `Append` are updated in-place with their assigned versions. ID and timestamp assignment depends on the implementation
- **Optimistic concurrency**: Uses `expectedVersion` parameter for conflict detection

### Example

```go
// Append some events
events := []eventstore.Event{
    {Type: "UserCreated", Data: userData},
    {Type: "UserEmailChanged", Data: emailData},
}

latestVersion, err := store.Append("user-123", events, -1)
// latestVersion will be 2 (version of the last event)
// events[0].Version will be 1, events[1].Version will be 2
// ID and timestamp assignment depends on the specific implementation

// Empty append always returns 0
version, err := store.Append("user-123", []eventstore.Event{}, -1)
// version will be 0 (not the current stream version)
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
    latestVersion, err := store.Append("user-123", events, -1)
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
// Retrieve events from all streams in the table
events, err := store.Retrieve(eventstore.ConsumeOptions{
    FromTimestamp: time.Now().Add(-24 * time.Hour), // Last 24 hours
    BatchSize:     100,
})
if err != nil {
    panic(err)
}

for _, event := range events {
    // Process each event...
}
```

### Consuming Events with Subscriptions

```go
// Subscribe to events from all streams in the table
subscription, err := store.Subscribe(eventstore.ConsumeOptions{
    FromTimestamp: time.Now(), // From now onwards
    BatchSize:     10,
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
            fmt.Printf("Received: %s\n", event.Type)
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
    config := postgres.Config{
        ConnectionString: "host=localhost port=5432 user=postgres password=password dbname=eventstore sslmode=disable",
        TableName: "events",
    }
    store, err := postgres.NewPostgresEventStore(config)
    if err != nil {
        panic(err)
    }
    consumer, err := postgres.NewPostgresEventConsumer(config, 2*time.Second)
    if err != nil {
        panic(err)
    }
    
    // Use the same interface as before...
}
```

### Running the Examples

See the examples directory for complete demonstrations:

- [hello-world example](examples/hello-world/) - Basic event store operations
- [consumer example](examples/consumer-example/) - Event consumption with polling and subscriptions  
- [postgres example](examples/postgres-example/) - PostgreSQL backend usage

```bash
# Run the basic hello-world example
make run-hello-world

# Run the consumer example (polling and subscriptions)
make run-consumer-example

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
