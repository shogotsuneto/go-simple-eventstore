// Package eventstore provides a unified interface for event stores across various databases.
package eventstore

import (
	"errors"
	"fmt"
	"time"
)

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

// LoadOptions contains options for loading events from a stream.
type LoadOptions struct {
	// AfterVersion specifies the version after which to start loading events
	AfterVersion int64
	// Limit specifies the maximum number of events to return
	Limit int
	// Reverse specifies whether to load events in reverse order (from latest to oldest)
	// When true, loads events in descending order starting from the latest version
	// When false (default), loads events in ascending order as before
	Reverse bool
}

// EventStore defines the core interface for event storage.
type EventStore interface {
	// Append adds new events to the given stream.
	// expectedVersion is used for optimistic concurrency control:
	// - If expectedVersion is -1, the stream can be in any state (no concurrency check)
	// - If expectedVersion is 0, the stream must not exist (stream creation)
	// - If expectedVersion > 0, the stream must be at exactly that version
	Append(streamID string, events []Event, expectedVersion int) error

	// Load retrieves events for the given stream using the specified options.
	Load(streamID string, opts LoadOptions) ([]Event, error)
}

// Common error types for event store operations.

// ErrVersionMismatch indicates that the expected version does not match the actual stream version.
type ErrVersionMismatch struct {
	StreamID        string
	ExpectedVersion int
	ActualVersion   int64
}

func (e *ErrVersionMismatch) Error() string {
	return fmt.Sprintf("expected version %d but stream '%s' is at version %d", e.ExpectedVersion, e.StreamID, e.ActualVersion)
}

// ErrStreamAlreadyExists indicates that a stream already exists when it was expected to be new.
type ErrStreamAlreadyExists struct {
	StreamID      string
	ActualVersion int64
}

func (e *ErrStreamAlreadyExists) Error() string {
	return fmt.Sprintf("expected new stream '%s' (version 0) but stream already exists with %d events", e.StreamID, e.ActualVersion)
}

// Sentinel errors for common error conditions.
var (
	// ErrConcurrencyConflict is returned when an optimistic concurrency check fails.
	ErrConcurrencyConflict = errors.New("concurrency conflict detected")
)
