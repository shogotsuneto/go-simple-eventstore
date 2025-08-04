// Package eventstore provides consumer interfaces for event consumption.
package eventstore

import "time"

// ConsumeOptions contains options for consuming events from a table.
type ConsumeOptions struct {
	// FromTimestamp specifies where to start consuming events from
	FromTimestamp time.Time
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
type EventConsumer interface {
	// Retrieve retrieves events from all streams in a retrieval operation
	Retrieve(opts ConsumeOptions) ([]Event, error)
	// Subscribe creates a subscription to all streams for continuous event consumption
	Subscribe(opts ConsumeOptions) (EventSubscription, error)
}
