package eventstore

import (
	"fmt"
	"strconv"
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

// Load retrieves events for the given stream starting from the cursor.
func (s *InMemoryEventStore) Load(streamID string, cursor string, limit int) ([]Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, exists := s.streams[streamID]
	if !exists {
		return []Event{}, nil
	}

	// Parse cursor as version number (0 means start from beginning)
	startVersion := int64(0)
	if cursor != "" {
		var err error
		startVersion, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor format: %v", err)
		}
	}

	// Find starting position
	var result []Event
	for _, event := range stream {
		if event.Version > startVersion {
			result = append(result, event)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}

	return result, nil
}
