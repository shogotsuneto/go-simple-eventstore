// Package eventstore provides consumer interfaces for event consumption.
package eventstore

import (
	"context"
)

// Cursor is an opaque checkpoint token (adapter-defined: global seq, shard map, Kafka offsets...)
type Cursor []byte

// Envelope is a portable event wrapper. Fill what you can per backend.
type Envelope struct {
	Event Event // The complete event

	StreamID string // e.g., "card<id>"

	// Diagnostics (useful for logs/metrics)
	Partition string // "global" | "topic:3" | "shard-000..."
	Offset    string // "12345" | Kinesis seq | Kafka offset
}

// Consumer is the only thing the projector needs.
type Consumer interface {
	// Fetch up to 'limit' events strictly after 'cursor'.
	// Returns the batch and the *advanced* cursor (position after the last delivered event).
	Fetch(ctx context.Context, cursor Cursor, limit int) (batch []Envelope, next Cursor, err error)

	// Called AFTER the projector has durably saved its own checkpoint.
	// Adapters that need it (such as Kafka groups) should commit; others can no-op.
	Commit(ctx context.Context, cursor Cursor) error
}
