# Hello World Example

This example demonstrates basic usage of the go-simple-eventstore library with an in-memory backend.

## Usage

Change to the hello-world directory and run:

```bash
cd examples/hello-world
go run main.go
```

## What the Example Does

1. Creates an in-memory event store
2. Defines and appends domain events (UserCreated and UserEmailChanged) to a stream
3. Loads and displays the events from the stream
4. Demonstrates version-based event loading
5. Shows basic event store operations

## Features Demonstrated

- **In-Memory Storage**: Fast, lightweight storage for development and testing
- **Event Appending**: Adding events to streams with automatic versioning
- **Event Loading**: Retrieving events with filtering options
- **Domain Events**: Working with structured event data and metadata
- **Stream Operations**: Basic stream-based event storage patterns

For PostgreSQL backend examples, see the `postgres-example` directory.