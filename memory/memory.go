package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// EventStoreConsumer combines both EventStore and EventConsumer interfaces.
// This is useful for implementations that provide both producer and consumer functionality.
type EventStoreConsumer interface {
	eventstore.EventStore
	eventstore.EventConsumer
}

// InMemoryEventStore is a simple in-memory implementation of both EventStore and EventConsumer.
// This implementation is suitable for testing and demonstration purposes.
//
// Note on delivery guarantees: When using timestamp-based filtering, this implementation
// does not guarantee exactly-once delivery. Events with identical timestamps may be
// delivered multiple times to subscriptions. This is acceptable as the implementation
// relies on timestamp precision for event ordering and filtering.
type InMemoryEventStore struct {
	mu            sync.RWMutex
	streams       map[string][]eventstore.Event
	timeline      []eventstore.Event      // For cross-stream retrieval
	subscriptions []*InMemorySubscription // Global list of active subscriptions
	subsMu        sync.RWMutex
	nextOffset    int64                          // Global offset counter for table-level sequence
}

// NewInMemoryEventStore creates a new in-memory event store with both producer and consumer capabilities.
func NewInMemoryEventStore() EventStoreConsumer {
	return &InMemoryEventStore{
		streams:       make(map[string][]eventstore.Event),
		timeline:      make([]eventstore.Event, 0),
		subscriptions: make([]*InMemorySubscription, 0),
		nextOffset:    1, // Start offset at 1 (similar to PostgreSQL SERIAL)
	}
}

// Append adds new events to the given stream and publishes them to the central timeline.
func (s *InMemoryEventStore) Append(streamID string, events []eventstore.Event, expectedVersion int) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get current stream (nil if stream does not exist)
	stream := s.streams[streamID]
	currentVersion := int64(len(stream))

	// Check expected version for optimistic concurrency control
	if expectedVersion != -1 {
		if expectedVersion == 0 && currentVersion != 0 {
			return &eventstore.ErrStreamAlreadyExists{
				StreamID:      streamID,
				ActualVersion: currentVersion,
			}
		}
		if expectedVersion > 0 && currentVersion != int64(expectedVersion) {
			return &eventstore.ErrVersionMismatch{
				StreamID:        streamID,
				ExpectedVersion: expectedVersion,
				ActualVersion:   currentVersion,
			}
		}
	}

	// Set version, timestamp, and offset for each event
	// Make copies to avoid modifying the original events passed by the caller
	eventsToStore := make([]eventstore.Event, len(events))
	for i, event := range events {
		// Create a copy of the event
		eventCopy := eventstore.Event{
			ID:        event.ID,
			Type:      event.Type,
			Data:      make([]byte, len(event.Data)),
			Timestamp: event.Timestamp,
			Version:   currentVersion + int64(i) + 1,
			Offset:    s.nextOffset + int64(i), // Assign table-level offset
		}
		copy(eventCopy.Data, event.Data)

		// Copy metadata
		if event.Metadata != nil {
			eventCopy.Metadata = make(map[string]string)
			for k, v := range event.Metadata {
				eventCopy.Metadata[k] = v
			}
		}

		if eventCopy.Timestamp.IsZero() {
			eventCopy.Timestamp = time.Now()
		}
		if eventCopy.ID == "" {
			eventCopy.ID = fmt.Sprintf("%s-%d", streamID, eventCopy.Version)
		}

		eventsToStore[i] = eventCopy
	}

	// Increment the global offset counter
	s.nextOffset += int64(len(events))

	// Append events to stream
	s.streams[streamID] = append(stream, eventsToStore...)

	// Add events to timeline for cross-stream retrieval
	s.addEventsToTimeline(eventsToStore)

	// Notify subscriptions about new events
	s.notifySubscriptions(eventsToStore)

	return nil
}

// addEventsToTimeline adds events to the central timeline maintaining chronological order.
// This method must be called while holding the main mutex.
func (s *InMemoryEventStore) addEventsToTimeline(events []eventstore.Event) {
	for _, event := range events {
		s.insertEventInTimeline(event)
	}
}

// notifySubscriptions notifies all subscriptions about new events.
func (s *InMemoryEventStore) notifySubscriptions(events []eventstore.Event) {
	s.subsMu.RLock()
	subs := make([]*InMemorySubscription, len(s.subscriptions))
	copy(subs, s.subscriptions)
	s.subsMu.RUnlock()

	for _, sub := range subs {
		for _, event := range events {
			sub.mu.Lock()
			currentFromTimestamp := sub.fromTimestamp
			currentFromOffset := sub.fromOffset
			sub.mu.Unlock()
			
			// Check if event should be included based on offset or timestamp
			include := false
			if currentFromOffset > 0 {
				// Use offset-based filtering if specified
				include = event.Offset > currentFromOffset
			} else {
				// Fall back to timestamp-based filtering
				include = currentFromTimestamp.IsZero() || event.Timestamp.After(currentFromTimestamp) || event.Timestamp.Equal(currentFromTimestamp)
			}
			
			if include {
				select {
				case sub.eventsCh <- event:
					sub.mu.Lock()
					if currentFromOffset > 0 {
						sub.fromOffset = event.Offset
					} else {
						sub.fromTimestamp = event.Timestamp
					}
					sub.mu.Unlock()
				case <-sub.closeCh:
					// Subscription is closed, skip
					continue
				default:
					// Channel is full, skip (could also send error)
					continue
				}
			}
		}
	}
}

// notifySubscriptionsForExisting sends existing events to specific subscriptions
func (s *InMemoryEventStore) notifySubscriptionsForExisting(subs []*InMemorySubscription, events []eventstore.Event) {
	for _, sub := range subs {
		count := 0
		for _, event := range events {
			sub.mu.Lock()
			currentFromTimestamp := sub.fromTimestamp
			currentFromOffset := sub.fromOffset
			sub.mu.Unlock()
			
			// Check if event should be included based on offset or timestamp
			include := false
			if currentFromOffset > 0 {
				// Use offset-based filtering if specified
				include = event.Offset > currentFromOffset
			} else {
				// Fall back to timestamp-based filtering
				include = currentFromTimestamp.IsZero() || event.Timestamp.After(currentFromTimestamp) || event.Timestamp.Equal(currentFromTimestamp)
			}
			
			if include {
				select {
				case sub.eventsCh <- event:
					sub.mu.Lock()
					if currentFromOffset > 0 {
						sub.fromOffset = event.Offset
					} else {
						sub.fromTimestamp = event.Timestamp
					}
					sub.mu.Unlock()
					count++
					if sub.batchSize > 0 && count >= sub.batchSize {
						break // Limit batch if specified
					}
				case <-sub.closeCh:
					// Subscription is closed, skip
					return
				default:
					// Channel is full, skip (could also send error)
					return
				}
			}
		}
	}
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

// Retrieve retrieves events from all streams in a retrieval operation.
func (s *InMemoryEventStore) Retrieve(opts eventstore.ConsumeOptions) ([]eventstore.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []eventstore.Event

	// Filter events from timeline by offset or timestamp
	for _, event := range s.timeline {
		include := false
		
		// If FromOffset is specified, use offset-based filtering (takes precedence)
		if opts.FromOffset > 0 {
			include = event.Offset > opts.FromOffset
		} else {
			// Fall back to timestamp-based filtering
			include = opts.FromTimestamp.IsZero() || event.Timestamp.After(opts.FromTimestamp) || event.Timestamp.Equal(opts.FromTimestamp)
		}
		
		if include {
			result = append(result, event)
		}
	}

	// Apply batch size limit
	if opts.BatchSize > 0 && len(result) > opts.BatchSize {
		result = result[:opts.BatchSize]
	}

	return result, nil
}

// Subscribe creates a subscription to all streams for continuous event consumption.
func (s *InMemoryEventStore) Subscribe(opts eventstore.ConsumeOptions) (eventstore.EventSubscription, error) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := &InMemorySubscription{
		fromTimestamp: opts.FromTimestamp,
		fromOffset:    opts.FromOffset,
		batchSize:     opts.BatchSize,
		eventsCh:      make(chan eventstore.Event, 100), // Buffered channel
		errorsCh:      make(chan error, 10),
		closeCh:       make(chan struct{}),
		store:         s,
	}

	// Add subscription to global list
	s.subscriptions = append(s.subscriptions, sub)

	// Immediately notify about existing events in timeline
	s.mu.RLock()
	existingEvents := make([]eventstore.Event, len(s.timeline))
	copy(existingEvents, s.timeline)
	s.mu.RUnlock()

	// Send existing events through notification system
	if len(existingEvents) > 0 {
		go s.notifySubscriptionsForExisting([]*InMemorySubscription{sub}, existingEvents)
	}

	return sub, nil
}

// insertEventInTimeline inserts an event into the timeline maintaining chronological order.
// This method must be called while holding the main mutex.
func (s *InMemoryEventStore) insertEventInTimeline(event eventstore.Event) {
	// For simplicity, we'll append and then sort if needed
	// In a real implementation, you might use a more efficient insertion
	s.timeline = append(s.timeline, event)

	// Simple insertion sort to maintain chronological order
	for i := len(s.timeline) - 1; i > 0; i-- {
		if s.timeline[i].Timestamp.Before(s.timeline[i-1].Timestamp) {
			s.timeline[i], s.timeline[i-1] = s.timeline[i-1], s.timeline[i]
		} else {
			break
		}
	}
}

// removeSubscription removes a subscription from the store.
func (s *InMemoryEventStore) removeSubscription(sub *InMemorySubscription) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	// Remove subscription from global list
	for i, existing := range s.subscriptions {
		if existing == sub {
			// Remove subscription from slice
			s.subscriptions = append(s.subscriptions[:i], s.subscriptions[i+1:]...)
			break
		}
	}
}

// InMemorySubscription represents an active subscription to all streams in memory.
type InMemorySubscription struct {
	fromTimestamp time.Time
	fromOffset    int64
	batchSize     int
	eventsCh      chan eventstore.Event
	errorsCh      chan error
	closeCh       chan struct{}
	store         *InMemoryEventStore
	closed        bool
	mu            sync.Mutex
}

// Events returns a channel that receives events as they are appended to the stream.
func (s *InMemorySubscription) Events() <-chan eventstore.Event {
	return s.eventsCh
}

// Errors returns a channel that receives any errors during subscription.
func (s *InMemorySubscription) Errors() <-chan error {
	return s.errorsCh
}

// Close stops the subscription and releases resources.
func (s *InMemorySubscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closeCh)
	s.store.removeSubscription(s)

	return nil
}
