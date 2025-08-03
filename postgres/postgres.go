package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/shogotsuneto/go-simple-eventstore"
)

// Config contains configuration options for PostgresEventStore.
type Config struct {
	// ConnectionString is the PostgreSQL connection string
	ConnectionString string
	// TableName is the name of the table to store events. Defaults to "events" if empty.
	TableName string
}

// PostgresEventStore is a PostgreSQL implementation of EventStore.
type PostgresEventStore struct {
	db            *sql.DB
	tableName     string
	subscriptions map[string][]*PostgresSubscription
	subsMu        sync.RWMutex
}

// NewPostgresEventStore creates a new PostgreSQL event store with the given configuration.
func NewPostgresEventStore(config Config) (*PostgresEventStore, error) {
	db, err := sql.Open("postgres", config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	tableName := config.TableName
	if tableName == "" {
		tableName = "events"
	}

	store := &PostgresEventStore{
		db:            db,
		tableName:     tableName,
		subscriptions: make(map[string][]*PostgresSubscription),
	}

	return store, nil
}

// quoteIdentifier properly quotes a PostgreSQL identifier (table name, column name, etc.)
// to handle special characters and prevent SQL injection.
func quoteIdentifier(identifier string) string {
	// Replace any double quotes with double-double quotes to escape them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	// Wrap in double quotes
	return `"` + escaped + `"`
}

// Close closes the database connection.
func (s *PostgresEventStore) Close() error {
	return s.db.Close()
}

// InitSchema creates the necessary tables and indexes if they don't exist.
func (s *PostgresEventStore) InitSchema() error {
	tableName := quoteIdentifier(s.tableName)
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		stream_id VARCHAR(255) NOT NULL,
		version BIGINT NOT NULL,
		event_id VARCHAR(255) NOT NULL,
		event_type VARCHAR(255) NOT NULL,
		event_data BYTEA NOT NULL,
		metadata JSONB,
		timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE INDEX IF NOT EXISTS %s ON %s(stream_id);
	CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(stream_id, version);
	`, tableName,
		quoteIdentifier("idx_"+s.tableName+"_stream_id"), tableName,
		quoteIdentifier("idx_"+s.tableName+"_stream_version"), tableName)

	_, err := s.db.Exec(query)
	return err
}

// Append adds new events to the given stream.
func (s *PostgresEventStore) Append(streamID string, events []eventstore.Event, expectedVersion int) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Lock the stream to prevent concurrent appends
	_, err = tx.Exec("SELECT pg_advisory_xact_lock(hashtext($1))", streamID)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Get the current maximum version for this stream
	var maxVersion int64
	err = tx.QueryRow(fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s WHERE stream_id = $1", quoteIdentifier(s.tableName)), streamID).Scan(&maxVersion)
	if err != nil {
		return fmt.Errorf("failed to get max version: %w", err)
	}

	// Check expected version for optimistic concurrency control
	if expectedVersion != -1 {
		if expectedVersion == 0 && maxVersion != 0 {
			return &eventstore.ErrStreamAlreadyExists{
				StreamID:      streamID,
				ActualVersion: maxVersion,
			}
		}
		if expectedVersion > 0 && maxVersion != int64(expectedVersion) {
			return &eventstore.ErrVersionMismatch{
				StreamID:        streamID,
				ExpectedVersion: expectedVersion,
				ActualVersion:   maxVersion,
			}
		}
	}

	// Prepare the insert statement
	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s (stream_id, version, event_id, event_type, event_data, metadata, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, quoteIdentifier(s.tableName)))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert each event
	for i, event := range events {
		version := maxVersion + int64(i) + 1
		eventID := event.ID
		if eventID == "" {
			eventID = fmt.Sprintf("%s-%d", streamID, version)
		}

		timestamp := event.Timestamp
		if timestamp.IsZero() {
			timestamp = time.Now()
		}

		// Convert metadata to JSON
		var metadataJSON interface{}
		if event.Metadata != nil {
			metadataJSON, err = json.Marshal(event.Metadata)
			if err != nil {
				return fmt.Errorf("failed to marshal metadata: %w", err)
			}
		}

		_, err = stmt.Exec(streamID, version, eventID, event.Type, event.Data, metadataJSON, timestamp)
		if err != nil {
			return fmt.Errorf("failed to insert event: %w", err)
		}
	}

	return tx.Commit()
}

// Load retrieves events for the given stream using the specified options.
func (s *PostgresEventStore) Load(streamID string, opts eventstore.LoadOptions) ([]eventstore.Event, error) {
	query := fmt.Sprintf(`
		SELECT event_id, event_type, event_data, metadata, timestamp, version
		FROM %s
		WHERE stream_id = $1 AND version > $2
		ORDER BY version ASC
	`, quoteIdentifier(s.tableName))

	args := []interface{}{streamID, opts.FromVersion}

	if opts.Limit > 0 {
		query += " LIMIT $3"
		args = append(args, opts.Limit)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []eventstore.Event

	for rows.Next() {
		var event eventstore.Event
		var metadataJSON []byte

		err := rows.Scan(
			&event.ID,
			&event.Type,
			&event.Data,
			&metadataJSON,
			&event.Timestamp,
			&event.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}

		// Unmarshal metadata from JSON
		if metadataJSON != nil {
			err = json.Unmarshal(metadataJSON, &event.Metadata)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
			}
		}

		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return events, nil
}

// Poll retrieves events from a stream in a one-time polling operation.
func (s *PostgresEventStore) Poll(streamID string, opts eventstore.ConsumeOptions) ([]eventstore.Event, error) {
	loadOpts := eventstore.LoadOptions{
		FromVersion: opts.FromVersion,
		Limit:       opts.BatchSize,
	}
	return s.Load(streamID, loadOpts)
}

// Subscribe creates a subscription to a stream for continuous event consumption.
func (s *PostgresEventStore) Subscribe(streamID string, opts eventstore.ConsumeOptions) (eventstore.EventSubscription, error) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	sub := &PostgresSubscription{
		streamID:    streamID,
		fromVersion: opts.FromVersion,
		batchSize:   opts.BatchSize,
		eventsCh:    make(chan eventstore.Event, 100), // Buffered channel
		errorsCh:    make(chan error, 10),
		closeCh:     make(chan struct{}),
		store:       s,
		ctx:         context.Background(),
	}

	// Add subscription to the list
	s.subscriptions[streamID] = append(s.subscriptions[streamID], sub)

	// Start subscription goroutine
	go sub.start()

	return sub, nil
}

// removeSubscription removes a subscription from the store.
func (s *PostgresEventStore) removeSubscription(streamID string, sub *PostgresSubscription) {
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
	streamID    string
	fromVersion int64
	batchSize   int
	eventsCh    chan eventstore.Event
	errorsCh    chan error
	closeCh     chan struct{}
	store       *PostgresEventStore
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	mu          sync.Mutex
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
	ticker := time.NewTicker(1 * time.Second) // Poll every second
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
	if s.store == nil || s.store.db == nil {
		return
	}

	batchSize := s.batchSize
	if batchSize == 0 {
		batchSize = 100 // Default batch size
	}

	events, err := s.store.Load(s.streamID, eventstore.LoadOptions{
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
	if s.store == nil || s.store.db == nil {
		return
	}

	batchSize := s.batchSize
	if batchSize == 0 {
		batchSize = 100 // Default batch size
	}

	events, err := s.store.Load(s.streamID, eventstore.LoadOptions{
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
