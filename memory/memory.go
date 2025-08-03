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

// InMemoryEventConsumer extends InMemoryEventStore with consumer capabilities.
type InMemoryEventConsumer struct {
	*InMemoryEventStore
	subscriptions map[string][]*InMemorySubscription
	subsMu        sync.RWMutex
}

// NewInMemoryEventConsumer creates a new in-memory event store with consumer capabilities.
func NewInMemoryEventConsumer() *InMemoryEventConsumer {
	return &InMemoryEventConsumer{
		InMemoryEventStore: NewInMemoryEventStore(),
		subscriptions:      make(map[string][]*InMemorySubscription),
	}
}

// Append adds new events to the given stream and notifies subscriptions.
func (s *InMemoryEventConsumer) Append(streamID string, events []eventstore.Event, expectedVersion int) error {
	// Call the base implementation
	err := s.InMemoryEventStore.Append(streamID, events, expectedVersion)
	if err != nil {
		return err
	}

	// Notify subscriptions about new events
	s.notifySubscriptions(streamID, events)
	return nil
}

// Poll retrieves events from a stream in a one-time polling operation.
func (s *InMemoryEventConsumer) Poll(streamID string, opts eventstore.ConsumeOptions) ([]eventstore.Event, error) {
	loadOpts := eventstore.LoadOptions{
		FromVersion: opts.FromVersion,
		Limit:       opts.BatchSize,
	}
	return s.Load(streamID, loadOpts)
}

// Subscribe creates a subscription to a stream for continuous event consumption.
func (s *InMemoryEventConsumer) Subscribe(streamID string, opts eventstore.ConsumeOptions) (eventstore.EventSubscription, error) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := &InMemorySubscription{
		streamID:    streamID,
		fromVersion: opts.FromVersion,
		batchSize:   opts.BatchSize,
		eventsCh:    make(chan eventstore.Event, 100), // Buffered channel
		errorsCh:    make(chan error, 10),
		closeCh:     make(chan struct{}),
		store:       s,
	}

	// Add subscription to the list
	s.subscriptions[streamID] = append(s.subscriptions[streamID], sub)

	// Start subscription goroutine
	go sub.start()

	return sub, nil
}

// notifySubscriptions notifies all subscriptions for a stream about new events.
func (s *InMemoryEventConsumer) notifySubscriptions(streamID string, events []eventstore.Event) {
	s.subsMu.RLock()
	subs, exists := s.subscriptions[streamID]
	if !exists {
		s.subsMu.RUnlock()
		return
	}
	s.subsMu.RUnlock()

	for _, sub := range subs {
		for _, event := range events {
			if event.Version > sub.fromVersion {
				select {
				case sub.eventsCh <- event:
					sub.fromVersion = event.Version // Update the subscription's position
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

// removeSubscription removes a subscription from the store.
func (s *InMemoryEventConsumer) removeSubscription(streamID string, sub *InMemorySubscription) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	subs := s.subscriptions[streamID]
	for i, existing := range subs {
		if existing == sub {
			// Remove subscription from slice
			s.subscriptions[streamID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	// Clean up empty subscription lists
	if len(s.subscriptions[streamID]) == 0 {
		delete(s.subscriptions, streamID)
	}
}

// InMemorySubscription represents an active subscription to a stream in memory.
type InMemorySubscription struct {
	streamID    string
	fromVersion int64
	batchSize   int
	eventsCh    chan eventstore.Event
	errorsCh    chan error
	closeCh     chan struct{}
	store       *InMemoryEventConsumer
	closed      bool
	mu          sync.Mutex
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
	s.store.removeSubscription(s.streamID, s)

	return nil
}

// start begins the subscription lifecycle.
func (s *InMemorySubscription) start() {
	// Load existing events that match our criteria
	s.store.mu.RLock()
	stream, exists := s.store.streams[s.streamID]
	if exists {
		for _, event := range stream {
			if event.Version > s.fromVersion {
				select {
				case s.eventsCh <- event:
					s.fromVersion = event.Version
					if s.batchSize > 0 && s.fromVersion >= int64(s.batchSize) {
						break // Limit initial batch if specified
					}
				case <-s.closeCh:
					s.store.mu.RUnlock()
					return
				}
			}
		}
	}
	s.store.mu.RUnlock()

	// Keep subscription alive until closed
	<-s.closeCh
}