package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// PostgresEventConsumer provides consumer capabilities using PostgreSQL.
type PostgresEventConsumer struct {
	*pgClient
	subscriptions map[string][]*PostgresSubscription
	subsMu        sync.RWMutex
}

// NewPostgresEventConsumer creates a new PostgreSQL event consumer with the given configuration.
func NewPostgresEventConsumer(config Config) (*PostgresEventConsumer, error) {
	client, err := newPgClient(config)
	if err != nil {
		return nil, err
	}

	return &PostgresEventConsumer{
		pgClient:      client,
		subscriptions: make(map[string][]*PostgresSubscription),
	}, nil
}

// Retrieve retrieves events from a stream in a retrieval operation.
func (s *PostgresEventConsumer) Retrieve(streamID string, opts eventstore.ConsumeOptions) ([]eventstore.Event, error) {
	loadOpts := eventstore.LoadOptions{
		FromVersion: opts.FromVersion,
		Limit:       opts.BatchSize,
	}
	return s.loadEvents(streamID, loadOpts)
}

// Subscribe creates a subscription to a stream for continuous event consumption.
func (s *PostgresEventConsumer) Subscribe(streamID string, opts eventstore.ConsumeOptions) (eventstore.EventSubscription, error) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	// Set default polling interval if not specified
	pollingInterval := opts.PollingInterval
	if pollingInterval <= 0 {
		pollingInterval = 1 * time.Second
	}

	sub := &PostgresSubscription{
		streamID:        streamID,
		fromVersion:     opts.FromVersion,
		batchSize:       opts.BatchSize,
		pollingInterval: pollingInterval,
		eventsCh:        make(chan eventstore.Event, 100), // Buffered channel
		errorsCh:        make(chan error, 10),
		closeCh:         make(chan struct{}),
		store:           s,
		ctx:             context.Background(),
	}

	// Add subscription to the list
	s.subscriptions[streamID] = append(s.subscriptions[streamID], sub)

	// Start subscription goroutine
	go sub.start()

	return sub, nil
}

// removeSubscription removes a subscription from the store.
func (s *PostgresEventConsumer) removeSubscription(streamID string, sub *PostgresSubscription) {
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

// PostgresSubscription represents an active subscription to a stream in PostgreSQL.
type PostgresSubscription struct {
	streamID        string
	fromVersion     int64
	batchSize       int
	pollingInterval time.Duration
	eventsCh        chan eventstore.Event
	errorsCh        chan error
	closeCh         chan struct{}
	store           *PostgresEventConsumer
	ctx             context.Context
	cancel          context.CancelFunc
	closed          bool
	mu              sync.Mutex
}

// Events returns a channel that receives events as they are appended to the stream.
func (s *PostgresSubscription) Events() <-chan eventstore.Event {
	return s.eventsCh
}

// Errors returns a channel that receives any errors during subscription.
func (s *PostgresSubscription) Errors() <-chan error {
	return s.errorsCh
}

// Close stops the subscription and releases resources.
func (s *PostgresSubscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	close(s.closeCh)
	s.store.removeSubscription(s.streamID, s)

	return nil
}

// start begins the subscription lifecycle with polling.
func (s *PostgresSubscription) start() {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Load existing events first
	s.loadInitialEvents()

	// Start polling for new events
	ticker := time.NewTicker(s.pollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.pollForEvents()
		}
	}
}

// loadInitialEvents loads any existing events that match our criteria.
func (s *PostgresSubscription) loadInitialEvents() {
	// Safety check - don't try to load if store or db is nil
	if s.store == nil || s.store.pgClient == nil || s.store.db == nil {
		return
	}

	batchSize := s.batchSize
	if batchSize == 0 {
		batchSize = 100 // Default batch size
	}

	events, err := s.store.loadEvents(s.streamID, eventstore.LoadOptions{
		FromVersion: s.fromVersion,
		Limit:       batchSize,
	})
	if err != nil {
		select {
		case s.errorsCh <- err:
		case <-s.closeCh:
			return
		}
		return
	}

	for _, event := range events {
		select {
		case s.eventsCh <- event:
			s.fromVersion = event.Version
		case <-s.closeCh:
			return
		}
	}
}

// pollForEvents polls the database for new events.
func (s *PostgresSubscription) pollForEvents() {
	// Safety check - don't try to poll if store or db is nil
	if s.store == nil || s.store.pgClient == nil || s.store.db == nil {
		return
	}

	batchSize := s.batchSize
	if batchSize == 0 {
		batchSize = 100 // Default batch size
	}

	events, err := s.store.loadEvents(s.streamID, eventstore.LoadOptions{
		FromVersion: s.fromVersion,
		Limit:       batchSize,
	})
	if err != nil {
		select {
		case s.errorsCh <- err:
		case <-s.closeCh:
		}
		return
	}

	for _, event := range events {
		select {
		case s.eventsCh <- event:
			s.fromVersion = event.Version
		case <-s.closeCh:
			return
		}
	}
}