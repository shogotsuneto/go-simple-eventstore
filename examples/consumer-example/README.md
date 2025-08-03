# Consumer Example

This example demonstrates how to consume events from the event store using both polling and subscription approaches.

## What this example shows

1. **Polling** - Retrieval of events from a stream
   - Poll for all events from the beginning
   - Poll for new events from a specific version
   - Useful for batch processing scenarios

2. **Subscriptions** - Continuous event consumption
   - Subscribe to events from the beginning of a stream
   - Subscribe to events from a specific version
   - Real-time event processing with channels
   - Multiple subscriptions to the same stream

## Features demonstrated

- **Event Consumer Interface** - Both polling and subscription methods
- **Version-based consumption** - Start consuming from a specific event version
- **Batch size control** - Limit the number of events returned in each batch
- **Real-time notifications** - Events are delivered as they are appended
- **Error handling** - Subscription errors are delivered via error channel
- **Resource cleanup** - Proper subscription closure

## Running the example

```bash
# From the root directory
make run-consumer-example

# Or directly
cd examples/consumer-example
go run main.go
```

## Key concepts

### EventConsumer interface
```go
type EventConsumer interface {
    // Poll retrieves events from a stream in a retrieval operation
    Poll(streamID string, opts ConsumeOptions) ([]Event, error)
    // Subscribe creates a subscription to a stream for continuous event consumption
    Subscribe(streamID string, opts ConsumeOptions) (EventSubscription, error)
}
```

### EventSubscription interface
```go
type EventSubscription interface {
    // Events returns a channel that receives events as they are appended to the stream
    Events() <-chan Event
    // Errors returns a channel that receives any errors during subscription
    Errors() <-chan error
    // Close stops the subscription and releases resources
    Close() error
}
```

### ConsumeOptions
```go
type ConsumeOptions struct {
    // FromVersion specifies where to start consuming events from
    FromVersion int64
    // BatchSize specifies the maximum number of events to return in each batch
    BatchSize int
}
```

## When to use each approach

**Use Polling when:**
- Processing events in batches
- Implementing periodic synchronization
- Building read models that don't need real-time updates
- Handling large volumes of historical data

**Use Subscriptions when:**
- Building real-time projections
- Implementing event-driven workflows
- Creating live dashboards or notifications
- Processing events as they arrive