package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// Compile-time interface compliance check
var _ eventstore.EventStore = (*PostgresEventStore)(nil)

// PostgresEventStore is a PostgreSQL implementation of EventStore.
type PostgresEventStore struct {
	*pgClient
}

// NewPostgresEventStore creates a new PostgreSQL event store with the given configuration.
func NewPostgresEventStore(config Config) (*PostgresEventStore, error) {
	client, err := newPgClient(config)
	if err != nil {
		return nil, err
	}

	return &PostgresEventStore{
		pgClient: client,
	}, nil
}

// Append adds new events to the given stream.
func (s *PostgresEventStore) Append(streamID string, events []eventstore.Event, expectedVersion int) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Lock the stream to prevent concurrent appends
	_, err = tx.Exec("SELECT pg_advisory_xact_lock(hashtext($1))", streamID)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Get the current maximum version for this stream
	var maxVersion int64
	err = tx.QueryRow(fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s WHERE stream_id = $1", quoteIdentifier(s.tableName)), streamID).Scan(&maxVersion)
	if err != nil {
		return 0, fmt.Errorf("failed to get max version: %w", err)
	}

	// Check expected version for optimistic concurrency control
	if expectedVersion != -1 {
		if expectedVersion == 0 && maxVersion != 0 {
			return 0, &eventstore.ErrStreamAlreadyExists{
				StreamID:      streamID,
				ActualVersion: maxVersion,
			}
		}
		if expectedVersion > 0 && maxVersion != int64(expectedVersion) {
			return 0, &eventstore.ErrVersionMismatch{
				StreamID:        streamID,
				ExpectedVersion: expectedVersion,
				ActualVersion:   maxVersion,
			}
		}
	}

	// Prepare and execute the insert statement
	stmt, err := s.prepareInsertStatement(tx)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Insert each event and update the original events with their versions
	var latestVersion int64
	for i := range events {
		version := maxVersion + int64(i) + 1
		latestVersion = version

		// Update the original event with the assigned version
		events[i].Version = version
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = time.Now()
		}
		if events[i].ID == "" {
			events[i].ID = fmt.Sprintf("%s-%d", streamID, version)
		}

		eventID := events[i].ID

		// Convert metadata to JSON
		var metadataJSON interface{}
		if events[i].Metadata != nil {
			metadataJSON, err = json.Marshal(events[i].Metadata)
			if err != nil {
				return 0, fmt.Errorf("failed to marshal metadata: %w", err)
			}
		}

		err = s.insertEvent(stmt, streamID, version, eventID, events[i], metadataJSON)
		if err != nil {
			return 0, fmt.Errorf("failed to insert event: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return latestVersion, nil
}

// prepareInsertStatement creates the appropriate INSERT statement based on timestamp configuration
func (s *PostgresEventStore) prepareInsertStatement(tx *sql.Tx) (*sql.Stmt, error) {
	var insertQuery string
	if !s.useClientGeneratedTimestamps {
		// Let database generate timestamp with DEFAULT CURRENT_TIMESTAMP
		insertQuery = fmt.Sprintf(`
			INSERT INTO %s (stream_id, version, event_id, event_type, event_data, metadata)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, quoteIdentifier(s.tableName))
	} else {
		// Use application-provided timestamp
		insertQuery = fmt.Sprintf(`
			INSERT INTO %s (stream_id, version, event_id, event_type, event_data, metadata, timestamp)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, quoteIdentifier(s.tableName))
	}

	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	return stmt, nil
}

// insertEvent executes the insert statement with appropriate parameters based on timestamp configuration
func (s *PostgresEventStore) insertEvent(stmt *sql.Stmt, streamID string, version int64, eventID string, event eventstore.Event, metadataJSON interface{}) error {
	if !s.useClientGeneratedTimestamps {
		// Insert without timestamp, let database generate it
		_, err := stmt.Exec(streamID, version, eventID, event.Type, event.Data, metadataJSON)
		return err
	} else {
		// Insert with app-generated timestamp
		timestamp := event.Timestamp
		if timestamp.IsZero() {
			timestamp = time.Now()
		}
		_, err := stmt.Exec(streamID, version, eventID, event.Type, event.Data, metadataJSON, timestamp)
		return err
	}
}

// Load retrieves events for the given stream using the specified options.
func (s *PostgresEventStore) Load(streamID string, opts eventstore.LoadOptions) ([]eventstore.Event, error) {
	return s.loadEvents(streamID, opts)
}
