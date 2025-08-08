// Package eventstore provides consumer interfaces for event consumption.
package eventstore

import "time"

// ConsumeOptions contains options for consuming events from a table.
type ConsumeOptions struct {
	// FromTimestamp specifies where to start consuming events from
	FromTimestamp time.Time
	// FromOffset specifies the table-level offset to start consuming events from
	// Events with offset greater than this value will be included
	// If both FromTimestamp and FromOffset are specified, FromOffset takes precedence
	FromOffset int64
	// BatchSize specifies the maximum number of events to return in each batch
	BatchSize int
}

// EventSubscription represents an active subscription to a stream.
type EventSubscription interface {
	// Events returns a channel that receives events as they are appended to the stream
	Events() <-chan Event
	// Errors returns a channel that receives any errors during subscription
	Errors() <-chan error
	// Close stops the subscription and releases resources
	Close() error
}

// EventConsumer defines the interface for consuming events from all streams in a table.
//
// Delivery Guarantees:
// - Events are delivered in chronological order based on timestamp
// - When multiple events have identical timestamps, delivery order is implementation-specific
// - Exactly-once delivery is not guaranteed when using timestamp-based filtering
// - Implementations may deliver the same event multiple times during timestamp boundary conditions
type EventConsumer interface {
	// Retrieve retrieves events from all streams in a retrieval operation
	Retrieve(opts ConsumeOptions) ([]Event, error)
	// Subscribe creates a subscription to all streams for continuous event consumption
	Subscribe(opts ConsumeOptions) (EventSubscription, error)
}
