package postgres

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// PostgresEventConsumer provides consumer capabilities using PostgreSQL.
type PostgresEventConsumer struct {
	*pgClient
	subscriptions   []*PostgresSubscription // Changed to a single slice since we don't use streamID
	subsMu          sync.RWMutex
	pollingInterval time.Duration
}

// NewPostgresEventConsumer creates a new PostgreSQL event consumer with the given database connection, table name, and polling interval.
func NewPostgresEventConsumer(db *sql.DB, tableName string, pollingInterval time.Duration) eventstore.EventConsumer {
	if tableName == "" {
		tableName = "events"
	}

	if pollingInterval <= 0 {
		pollingInterval = 1 * time.Second
	}

	return &PostgresEventConsumer{
		pgClient: &pgClient{
			db:        db,
			tableName: tableName,
		},
		subscriptions:   []*PostgresSubscription{}, // Changed to a single slice
		pollingInterval: pollingInterval,
	}
}

// Retrieve retrieves events from all streams in a retrieval operation.
func (s *PostgresEventConsumer) Retrieve(opts eventstore.ConsumeOptions) ([]eventstore.Event, error) {
	return s.loadEventsByTimestamp(opts)
}

// Subscribe creates a subscription to all streams for continuous event consumption.
func (s *PostgresEventConsumer) Subscribe(opts eventstore.ConsumeOptions) (eventstore.EventSubscription, error) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := &PostgresSubscription{
		fromTimestamp:     opts.FromTimestamp,
		processedEventIDs: make(map[string]struct{}),
		batchSize:         opts.BatchSize,
		pollingInterval:   s.pollingInterval,
		eventsCh:          make(chan eventstore.Event, 100), // Buffered channel
		errorsCh:          make(chan error, 10),
		closeCh:           make(chan struct{}),
		store:             s,
		ctx:               context.Background(),
	}

	// Add subscription to the list
	s.subscriptions = append(s.subscriptions, sub)

	// Start subscription goroutine
	go sub.start()

	return sub, nil
}

// removeSubscription removes a subscription from the store.
func (s *PostgresEventConsumer) removeSubscription(sub *PostgresSubscription) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	for i, existing := range s.subscriptions {
		if existing == sub {
			// Remove subscription from slice
			s.subscriptions = append(s.subscriptions[:i], s.subscriptions[i+1:]...)
			break
		}
	}
}

// PostgresSubscription represents an active subscription to all streams in PostgreSQL.
type PostgresSubscription struct {
	fromTimestamp     time.Time
	processedEventIDs map[string]struct{}          // Track Event.IDs processed within current timestamp
	batchSize         int
	pollingInterval   time.Duration
	eventsCh          chan eventstore.Event
	errorsCh          chan error
	closeCh           chan struct{}
	store             *PostgresEventConsumer
	ctx               context.Context
	cancel            context.CancelFunc
	closed            bool
	mu                sync.Mutex
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
	s.store.removeSubscription(s)

	return nil
}

// start begins the subscription lifecycle with polling.
func (s *PostgresSubscription) start() {
	s.mu.Lock()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mu.Unlock()

	// Start polling immediately - first poll will load any existing events
	ticker := time.NewTicker(s.pollingInterval)
	defer ticker.Stop()

	// Poll immediately, then continue on ticker
	s.pollForEvents()

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

// pollForEvents polls the database for new events using timestamp-based filtering.
func (s *PostgresSubscription) pollForEvents() {
	// Safety check - don't try to poll if store or db is nil
	if s.store == nil || s.store.pgClient == nil || s.store.db == nil {
		return
	}

	s.mu.Lock()
	batchSize := s.batchSize
	if batchSize == 0 {
		batchSize = 100 // Default batch size
	}

	// Use timestamp-based filtering for polling (including equal timestamps for duplicates)
	events, err := s.store.loadEventsByTimestamp(eventstore.ConsumeOptions{
		FromTimestamp: s.fromTimestamp,
		BatchSize:     batchSize,
	})
	s.mu.Unlock()

	if err != nil {
		select {
		case s.errorsCh <- err:
		case <-s.closeCh:
		}
		return
	}

	for _, event := range events {
		s.mu.Lock()
		
		// Check if we've moved to a new timestamp
		if !event.Timestamp.Equal(s.fromTimestamp) {
			// Clear processed IDs for new timestamp
			s.processedEventIDs = make(map[string]struct{})
		}
		
		// Skip if we've already processed this Event.ID within the current timestamp
		if _, processed := s.processedEventIDs[event.ID]; processed {
			s.mu.Unlock()
			continue
		}
		
		// Mark Event.ID as processed
		s.processedEventIDs[event.ID] = struct{}{}
		s.fromTimestamp = event.Timestamp
		s.mu.Unlock()

		select {
		case s.eventsCh <- event:
		case <-s.closeCh:
			return
		}
	}
}
