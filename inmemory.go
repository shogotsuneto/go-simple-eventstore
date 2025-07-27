package eventstore

import (
	"fmt"
	"sync"
	"time"
)

// InMemoryEventStore is a simple in-memory implementation of EventStore.
// This implementation is suitable for testing and demonstration purposes.
type InMemoryEventStore struct {
	mu      sync.RWMutex
	streams map[string][]Event
}

// NewInMemoryEventStore creates a new in-memory event store.
func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{
		streams: make(map[string][]Event),
	}
}

// Append adds new events to the given stream.
func (s *InMemoryEventStore) Append(streamID string, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get current stream or create new one
	stream := s.streams[streamID]

	// Set version and timestamp for each event
	for i := range events {
		events[i].Version = int64(len(stream) + i + 1)
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = time.Now()
		}
		if events[i].ID == "" {
			events[i].ID = fmt.Sprintf("%s-%d", streamID, events[i].Version)
		}
	}

	// Append events to stream
	s.streams[streamID] = append(stream, events...)

	return nil
}

// Load retrieves events for the given stream starting from the version.
func (s *InMemoryEventStore) Load(streamID string, fromVersion int, limit int) ([]Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, exists := s.streams[streamID]
	if !exists {
		return []Event{}, nil
	}

	// Find starting position
	var result []Event
	for _, event := range stream {
		if event.Version > int64(fromVersion) {
			result = append(result, event)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}

	return result, nil
}
