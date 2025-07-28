package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgresEventStore is a PostgreSQL implementation of EventStore.
type PostgresEventStore struct {
	db *sql.DB
}

// NewPostgresEventStore creates a new PostgreSQL event store.
func NewPostgresEventStore(connectionString string) (*PostgresEventStore, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &PostgresEventStore{db: db}

	return store, nil
}

// Close closes the database connection.
func (s *PostgresEventStore) Close() error {
	return s.db.Close()
}

// InitSchema creates the necessary tables and indexes if they don't exist.
func (s *PostgresEventStore) InitSchema() error {
	query := `
	CREATE TABLE IF NOT EXISTS events (
		id SERIAL PRIMARY KEY,
		stream_id VARCHAR(255) NOT NULL,
		version BIGINT NOT NULL,
		event_id VARCHAR(255) NOT NULL,
		event_type VARCHAR(255) NOT NULL,
		event_data BYTEA NOT NULL,
		metadata JSONB,
		timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events(stream_id);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_events_stream_version ON events(stream_id, version);
	`

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
	err = tx.QueryRow("SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1", streamID).Scan(&maxVersion)
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
	stmt, err := tx.Prepare(`
		INSERT INTO events (stream_id, version, event_id, event_type, event_data, metadata, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
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
	query := `
		SELECT event_id, event_type, event_data, metadata, timestamp, version
		FROM events
		WHERE stream_id = $1 AND version > $2
		ORDER BY version ASC
	`

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