# Cursor-based Consumer Example

This example demonstrates cursor-based event consumption from all streams in a table using the new Consumer interface.

## Features

- **Cursor-based fetching** - Precise event positioning with cursors for reliable consumption
- **Incremental processing** - Fetch events incrementally using cursor advancement
- **Cross-stream consumption** - Process events across multiple streams in chronological order
- **Envelope format** - Portable event wrapper with StreamID, CommitTime, EventID, Partition, and Offset

## Running the example

```bash
# From the root directory
make run-consumer-example

# Or directly
cd examples/consumer-example
go run main.go
```

## Key interfaces

### Consumer
```go
type Consumer interface {
    // Fetch up to 'limit' events strictly after 'cursor'.
    // Returns the batch and the *advanced* cursor (position after the last delivered event).
    Fetch(ctx context.Context, cursor Cursor, limit int) (batch []Envelope, next Cursor, err error)

    // Called AFTER the projector has durably saved its own checkpoint.
    // Adapters that need it (such as Kafka groups) should commit; others can no-op.
    Commit(ctx context.Context, cursor Cursor) error
}
```

### Envelope
```go
type Envelope struct {
    Type       string        // domain event type
    Data       []byte        // payload
    Metadata   []byte        // optional

    StreamID   string        // e.g., "card<id>"
    CommitTime time.Time     // db/stream commit/arrival time
    EventID    string        // ULID/KSUID/hash for idempotency (optional)

    // Diagnostics (useful for logs/metrics)
    Partition  string        // "global" | "topic:3" | "shard-000..."
    Offset     string        // "12345" | Kinesis seq | Kafka offset
}
```

### Cursor
```go
type Cursor []byte  // Opaque checkpoint token (adapter-defined)
```

## Usage patterns

**Cursor-based batch processing:**
- Start with `nil` cursor to fetch from beginning
- Use returned cursor to fetch next batch incrementally  
- Call `Commit()` after processing each batch to checkpoint progress
- Resume from saved cursor after restarts

**Benefits over timestamp-based consumption:**
- Precise event positioning eliminates duplicate delivery
- Cursor advancement ensures exactly-once processing semantics
- Cross-adapter portability with opaque cursor format
- Better performance with adapter-optimized cursor implementations