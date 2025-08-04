# Consumer Example

This example demonstrates event consumption from all streams in a table using both retrieve and subscription approaches.

## Features

- **Retrieve** - Batch retrieval of events from all streams
- **Subscribe** - Real-time event consumption from all streams  
- **Timestamp-based filtering** - Consume events from specific timestamps
- **Table-wide consumption** - Process events across multiple streams

## Running the example

```bash
# From the root directory
make run-consumer-example

# Or directly
cd examples/consumer-example
go run main.go
```

## Key interfaces

### EventConsumer
```go
type EventConsumer interface {
    // Retrieve events from all streams in the table
    Retrieve(opts ConsumeOptions) ([]Event, error)
    // Subscribe to all streams in the table  
    Subscribe(opts ConsumeOptions) (EventSubscription, error)
}
```

### ConsumeOptions
```go
type ConsumeOptions struct {
    FromTimestamp time.Time  // Start consuming from this timestamp
    BatchSize     int        // Maximum events per batch
}
```

## Usage patterns

**Retrieve for batch processing:**
- Process historical events in batches
- Build read models from event history
- Periodic data synchronization

**Subscribe for real-time processing:**
- Live projections and dashboards
- Event-driven workflows
- Real-time notifications