package memory

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// Compile-time interface compliance checks
var _ eventstore.EventStore = (*InMemoryEventStore)(nil)
var _ eventstore.Consumer = (*InMemoryEventStore)(nil)

// InMemoryEventStore is a simple in-memory implementation of both EventStore and Consumer.
// This implementation is suitable for testing and demonstration purposes.
//
// For cursor implementation, this uses the index in the timeline array as the cursor position.
// The cursor is encoded as a little-endian int64 in bytes.
type InMemoryEventStore struct {
	mu       sync.RWMutex
	streams  map[string][]eventstore.Event
	timeline []eventstore.Event // For cross-stream retrieval, ordered by insertion
}

// NewInMemoryEventStore creates a new in-memory event store with both producer and consumer capabilities.
func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{
		streams:  make(map[string][]eventstore.Event),
		timeline: make([]eventstore.Event, 0),
	}
}

// Append adds new events to the given stream and publishes them to the central timeline.
func (s *InMemoryEventStore) Append(streamID string, events []eventstore.Event, expectedVersion int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get current stream (nil if stream does not exist)
	stream := s.streams[streamID]
	currentVersion := int64(len(stream))

	if len(events) == 0 {
		return 0, nil
	}

	// Check expected version for optimistic concurrency control
	if expectedVersion != -1 {
		if expectedVersion == 0 && currentVersion != 0 {
			return 0, &eventstore.ErrStreamAlreadyExists{
				StreamID:      streamID,
				ActualVersion: currentVersion,
			}
		}
		if expectedVersion > 0 && currentVersion != expectedVersion {
			return 0, &eventstore.ErrVersionMismatch{
				StreamID:        streamID,
				ExpectedVersion: expectedVersion,
				ActualVersion:   currentVersion,
			}
		}
	}

	// Set version and timestamp for each event
	// Update the original events with their assigned versions and create copies for storage
	eventsToStore := make([]eventstore.Event, len(events))
	var latestVersion int64
	for i := range events {
		newVersion := currentVersion + int64(i) + 1
		latestVersion = newVersion

		// Update the original event with the assigned version
		events[i].Version = newVersion
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = time.Now()
		}
		if events[i].ID == "" {
			events[i].ID = fmt.Sprintf("%s-%d", streamID, newVersion)
		}

		// Create a copy for storage
		eventCopy := eventstore.Event{
			ID:        events[i].ID,
			Type:      events[i].Type,
			Data:      make([]byte, len(events[i].Data)),
			Timestamp: events[i].Timestamp,
			Version:   newVersion,
		}
		copy(eventCopy.Data, events[i].Data)

		// Copy metadata
		if events[i].Metadata != nil {
			eventCopy.Metadata = make(map[string]string)
			for k, v := range events[i].Metadata {
				eventCopy.Metadata[k] = v
			}
		}

		eventsToStore[i] = eventCopy
	}

	// Append events to stream
	s.streams[streamID] = append(stream, eventsToStore...)

	// Add events to timeline for cross-stream retrieval
	s.addEventsToTimeline(eventsToStore)

	return latestVersion, nil
}

// addEventsToTimeline adds events to the central timeline in insertion order.
// This method must be called while holding the main mutex.
func (s *InMemoryEventStore) addEventsToTimeline(events []eventstore.Event) {
	// Simply append to timeline, maintaining insertion order which approximates chronological order
	for _, event := range events {
		s.timeline = append(s.timeline, event)
	}
}

// cursorToIndex converts a cursor to an index in the timeline.
// If cursor is nil/empty, returns -1 (meaning start from beginning).
func (s *InMemoryEventStore) cursorToIndex(cursor eventstore.Cursor) int64 {
	if len(cursor) == 0 {
		return -1 // Start from beginning
	}
	if len(cursor) != 8 {
		return -1 // Invalid cursor, start from beginning
	}
	return int64(binary.LittleEndian.Uint64(cursor))
}

// indexToCursor converts an index to a cursor.
func (s *InMemoryEventStore) indexToCursor(index int64) eventstore.Cursor {
	cursor := make([]byte, 8)
	binary.LittleEndian.PutUint64(cursor, uint64(index))
	return cursor
}

// eventToEnvelope converts an Event to an Envelope.
func (s *InMemoryEventStore) eventToEnvelope(event eventstore.Event, streamID string) eventstore.Envelope {
	// Encode metadata as JSON bytes if present
	var metadataBytes []byte
	if event.Metadata != nil {
		// Simple encoding: concatenate key=value pairs with newlines
		metadataStr := ""
		for k, v := range event.Metadata {
			if metadataStr != "" {
				metadataStr += "\n"
			}
			metadataStr += k + "=" + v
		}
		metadataBytes = []byte(metadataStr)
	}

	return eventstore.Envelope{
		Type:       event.Type,
		Data:       event.Data,
		Metadata:   metadataBytes,
		StreamID:   streamID,
		CommitTime: event.Timestamp,
		EventID:    event.ID,
		Partition:  "global",
		Offset:     fmt.Sprintf("%d", event.Version),
	}
}

// Fetch up to 'limit' events strictly after 'cursor'.
// Returns the batch and the *advanced* cursor (position after the last delivered event).
func (s *InMemoryEventStore) Fetch(ctx context.Context, cursor eventstore.Cursor, limit int) (batch []eventstore.Envelope, next eventstore.Cursor, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Convert cursor to index
	startIndex := s.cursorToIndex(cursor)

	// Start from the position after the cursor
	startIndex++

	if startIndex < 0 {
		startIndex = 0
	}

	var result []eventstore.Envelope
	lastIndex := startIndex - 1 // Track the last processed index

	// Find events in timeline starting from cursor position
	for i := int(startIndex); i < len(s.timeline) && len(result) < limit; i++ {
		event := s.timeline[i]

		// Find which stream this event belongs to
		var streamID string
		for sid, events := range s.streams {
			for _, streamEvent := range events {
				if streamEvent.ID == event.ID && streamEvent.Version == event.Version {
					streamID = sid
					break
				}
			}
			if streamID != "" {
				break
			}
		}

		envelope := s.eventToEnvelope(event, streamID)
		result = append(result, envelope)
		lastIndex = int64(i)
	}

	// Return the cursor pointing to the last event processed
	nextCursor := s.indexToCursor(lastIndex)
	return result, nextCursor, nil
}

// Commit is called AFTER the projector has durably saved its own checkpoint.
// For in-memory implementation, this is a no-op.
func (s *InMemoryEventStore) Commit(ctx context.Context, cursor eventstore.Cursor) error {
	// No-op for in-memory implementation
	return nil
}

// Load retrieves events for the given stream using the specified options.
func (s *InMemoryEventStore) Load(streamID string, opts eventstore.LoadOptions) ([]eventstore.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, exists := s.streams[streamID]
	if !exists {
		return []eventstore.Event{}, nil
	}

	var result []eventstore.Event

	if opts.Desc {
		// Load events in descending order (from latest to oldest)
		// Start from the end and work backwards
		for i := len(stream) - 1; i >= 0; i-- {
			event := stream[i]
			// In reverse loading: if ExclusiveStartVersion is 0, include all events
			// Otherwise, include events with version < ExclusiveStartVersion
			if opts.ExclusiveStartVersion == 0 || event.Version < opts.ExclusiveStartVersion {
				result = append(result, event)
				if opts.Limit > 0 && len(result) >= opts.Limit {
					break
				}
			}
		}
	} else {
		// Load events in forward order (original behavior)
		for _, event := range stream {
			if event.Version > opts.ExclusiveStartVersion {
				result = append(result, event)
				if opts.Limit > 0 && len(result) >= opts.Limit {
					break
				}
			}
		}
	}

	return result, nil
}
