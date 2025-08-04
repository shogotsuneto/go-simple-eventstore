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
type InMemoryEventStore struct {
	mu            sync.RWMutex
	streams       map[string][]eventstore.Event
	subscriptions map[string][]*InMemorySubscription
	subsMu        sync.RWMutex
}

// NewInMemoryEventStore creates a new in-memory event store with both producer and consumer capabilities.
func NewInMemoryEventStore() EventStoreConsumer {
	return &InMemoryEventStore{
		streams:       make(map[string][]eventstore.Event),
		subscriptions: make(map[string][]*InMemorySubscription),
	}
}

// Append adds new events to the given stream and notifies subscriptions.
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

	// Notify subscriptions about new events
	s.notifySubscriptions(streamID, events)

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
		if event.Version > opts.AfterVersion {
			result = append(result, event)
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
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

	// Collect events from all streams
	for _, stream := range s.streams {
		for _, event := range stream {
			// Filter by timestamp
			if opts.FromTimestamp.IsZero() || event.Timestamp.After(opts.FromTimestamp) || event.Timestamp.Equal(opts.FromTimestamp) {
				result = append(result, event)
			}
		}
	}

	// Sort events by timestamp (to maintain order across streams)
	// Simple bubble sort for now (could be optimized later)
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].Timestamp.After(result[j].Timestamp) {
				result[i], result[j] = result[j], result[i]
			}
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
		batchSize:     opts.BatchSize,
		eventsCh:      make(chan eventstore.Event, 100), // Buffered channel
		errorsCh:      make(chan error, 10),
		closeCh:       make(chan struct{}),
		notifyCh:      make(chan []eventstore.Event, 50), // Buffered channel for notifications
		store:         s,
	}

	// Add subscription to a global list (using empty string as key for all streams)
	s.subscriptions[""] = append(s.subscriptions[""], sub)

	// Start subscription goroutine
	go sub.start()

	return sub, nil
}

// notifySubscriptions notifies all subscriptions for all streams about new events.
func (s *InMemoryEventStore) notifySubscriptions(streamID string, events []eventstore.Event) {
	s.subsMu.RLock()
	// Get global subscriptions (using empty string as key for all streams)
	subs, exists := s.subscriptions[""]
	if !exists {
		s.subsMu.RUnlock()
		return
	}
	s.subsMu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.notifyCh <- events:
			// Successfully sent notification
		case <-sub.closeCh:
			// Subscription is closed, skip
			continue
		default:
			// Notification channel is full, skip (could also send error)
			continue
		}
	}
}

// removeSubscription removes a subscription from the store.
func (s *InMemoryEventStore) removeSubscription(sub *InMemorySubscription) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	// Remove from global subscriptions (using empty string as key for all streams)
	subs := s.subscriptions[""]
	for i, existing := range subs {
		if existing == sub {
			// Remove subscription from slice
			s.subscriptions[""] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	// Clean up empty subscription lists
	if len(s.subscriptions[""]) == 0 {
		delete(s.subscriptions, "")
	}
}

// InMemorySubscription represents an active subscription to all streams in memory.
type InMemorySubscription struct {
	fromTimestamp time.Time
	batchSize     int
	eventsCh      chan eventstore.Event
	errorsCh      chan error
	closeCh       chan struct{}
	notifyCh      chan []eventstore.Event // Channel for buffering new event notifications
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

// start begins the subscription lifecycle.
func (s *InMemorySubscription) start() {
	// Load existing events from all streams that match our criteria
	s.store.mu.RLock()
	var allEvents []eventstore.Event
	for _, stream := range s.store.streams {
		for _, event := range stream {
			if s.fromTimestamp.IsZero() || event.Timestamp.After(s.fromTimestamp) || event.Timestamp.Equal(s.fromTimestamp) {
				allEvents = append(allEvents, event)
			}
		}
	}
	s.store.mu.RUnlock()

	// Sort events by timestamp to maintain order across streams
	for i := 0; i < len(allEvents); i++ {
		for j := i + 1; j < len(allEvents); j++ {
			if allEvents[i].Timestamp.After(allEvents[j].Timestamp) {
				allEvents[i], allEvents[j] = allEvents[j], allEvents[i]
			}
		}
	}

	// Send initial batch of events
	count := 0
	for _, event := range allEvents {
		select {
		case s.eventsCh <- event:
			s.fromTimestamp = event.Timestamp
			count++
			if s.batchSize > 0 && count >= s.batchSize {
				break // Limit initial batch if specified
			}
		case <-s.closeCh:
			return
		}
	}

	// Handle ongoing notifications
	for {
		select {
		case events := <-s.notifyCh:
			// Process new events from notification
			for _, event := range events {
				// Filter by timestamp instead of version
				if s.fromTimestamp.IsZero() || event.Timestamp.After(s.fromTimestamp) || event.Timestamp.Equal(s.fromTimestamp) {
					select {
					case s.eventsCh <- event:
						s.fromTimestamp = event.Timestamp // Update the subscription's position
					case <-s.closeCh:
						return
					}
				}
			}
		case <-s.closeCh:
			return
		}
	}
}
