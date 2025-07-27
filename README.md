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
    Append(streamID string, events []Event) error

    // Load retrieves events for the given stream starting from `fromVersion`.
    Load(streamID string, cursor string, limit int) ([]Event, error)
}
```

## 🔌 Backend Adapters

### Implemented
- **In-Memory** - Simple in-memory implementation (suitable for testing and development)

### Work In Progress (WIP)
- **PostgreSQL** - Reliable relational database adapter (WIP)
- **DynamoDB** - AWS NoSQL database adapter (WIP)
- **More adapters coming** - Extensible design allows for easy addition of new database backends

## 🚀 Getting Started

*Documentation for installation and usage will be added as the library develops.*

## 📋 MVP Scope

This project focuses on the essential functionality needed for event sourcing:

1. **Append-only event storage** - Events are never modified, only appended
2. **Stream-based organization** - Events are organized by stream ID
3. **Cursor-based loading** - Efficient event retrieval with pagination support
4. **Database agnostic** - Unified interface across different storage backends

## 🤝 Contributing

This project welcomes contributions! The goal is to maintain simplicity while adding support for additional database adapters.

## 📄 License

*License information will be added soon.*
