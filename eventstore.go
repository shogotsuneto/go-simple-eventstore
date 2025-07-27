// Package eventstore provides a unified interface for event stores across various databases.
package eventstore

import "time"

// Event represents a single event in the event store.
type Event struct {
	// ID is a unique identifier for the event
	ID string
	// Type describes the kind of event
	Type string
	// Data contains the event payload
	Data []byte
	// Metadata contains additional event information
	Metadata map[string]string
	// Timestamp when the event was created
	Timestamp time.Time
	// Version represents the position of this event in the stream
	Version int64
}

// EventStore defines the core interface for event storage.
type EventStore interface {
	// Append adds new events to the given stream.
	Append(streamID string, events []Event) error

	// Load retrieves events for the given stream starting from the cursor.
	// The cursor parameter specifies where to start loading events from.
	// The limit parameter specifies the maximum number of events to return.
	Load(streamID string, cursor string, limit int) ([]Event, error)
}
