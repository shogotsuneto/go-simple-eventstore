package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// InMemoryEventStore is a simple in-memory implementation of EventStore.
// This implementation is suitable for testing and demonstration purposes.
type InMemoryEventStore struct {
	mu      sync.RWMutex
	streams map[string][]eventstore.Event
}

// NewInMemoryEventStore creates a new in-memory event store.
func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{
		streams: make(map[string][]eventstore.Event),
	}
}

// Append adds new events to the given stream.
func (s *InMemoryEventStore) Append(streamID string, events []eventstore.Event) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get current stream (nil if stream does not exist)
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

// Load retrieves events for the given stream using the specified options.
func (s *InMemoryEventStore) Load(streamID string, opts eventstore.LoadOptions) ([]eventstore.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, exists := s.streams[streamID]
	if !exists {
		return []eventstore.Event{}, nil
	}

	// Find starting position
	var result []eventstore.Event
	for _, event := range stream {
		if event.Version > opts.FromVersion {
			result = append(result, event)
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}

	return result, nil
}