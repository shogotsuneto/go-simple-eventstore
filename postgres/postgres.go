package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
	db        *sql.DB
	tableName string
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
		db:        db,
		tableName: tableName,
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
