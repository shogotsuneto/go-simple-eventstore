package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// PostgresEventStore is a PostgreSQL implementation of EventStore.
type PostgresEventStore struct {
	*pgClient
}

// NewPostgresEventStore creates a new PostgreSQL event store with the given database connection and table name.
func NewPostgresEventStore(db *sql.DB, tableName string) eventstore.EventStore {
	if tableName == "" {
		tableName = "events"
	}

	return &PostgresEventStore{
		pgClient: &pgClient{
			db:        db,
			tableName: tableName,
		},
	}
}

// Close closes the database connection.
func (s *PostgresEventStore) Close() error {
	return s.db.Close()
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
	return s.loadEvents(streamID, opts)
}