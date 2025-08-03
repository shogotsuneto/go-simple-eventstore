// Package eventstore provides consumer interfaces for event consumption.
package eventstore

// ConsumeOptions contains options for consuming events from a stream.
type ConsumeOptions struct {
	// FromVersion specifies where to start consuming events from
	FromVersion int64
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

// EventConsumer defines the interface for consuming events from streams.
type EventConsumer interface {
	// Poll retrieves events from a stream in a one-time polling operation
	Poll(streamID string, opts ConsumeOptions) ([]Event, error)
	// Subscribe creates a subscription to a stream for continuous event consumption
	Subscribe(streamID string, opts ConsumeOptions) (EventSubscription, error)
}