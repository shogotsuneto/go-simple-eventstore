# Examples

This directory contains example code demonstrating how to use the go-simple-eventstore library.

## Hello World Example

The `hello-world` example demonstrates the basic usage of the event store:

- Creating an in-memory event store
- Appending events to a stream
- Loading events from a stream
- Using cursor-based pagination

### Running the Example

```bash
go run ./examples/hello-world
```

This example shows:
- How to create domain events (UserCreated, UserEmailChanged)
- How to append events to a stream
- How to load all events from a stream
- How to use cursor-based loading for pagination