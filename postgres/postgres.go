package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/shogotsuneto/go-simple-eventstore"
)

// InitSchema creates the necessary tables and indexes if they don't exist.
// It takes a database connection and table name to initialize the schema.
func InitSchema(db *sql.DB, tableName string) error {
	if tableName == "" {
		tableName = "events"
	}

	quotedTableName := quoteIdentifier(tableName)
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
	`, quotedTableName,
		quoteIdentifier("idx_"+tableName+"_stream_id"), quotedTableName,
		quoteIdentifier("idx_"+tableName+"_stream_version"), quotedTableName)

	_, err := db.Exec(query)
	return err
}

// Config contains configuration options for PostgresEventStore.
type Config struct {
	// ConnectionString is the PostgreSQL connection string
	ConnectionString string
	// TableName is the name of the table to store events. Defaults to "events" if empty.
	TableName string
}

// pgClient contains shared database functionality used by both producer and consumer.
type pgClient struct {
	db        *sql.DB
	tableName string
}

// newPgClient creates a new shared postgres client with the given configuration.
func newPgClient(config Config) (*pgClient, error) {
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

	return &pgClient{
		db:        db,
		tableName: tableName,
	}, nil
}

// quoteIdentifier properly quotes a PostgreSQL identifier (table name, column name, etc.)
// to handle special characters and prevent SQL injection.
func quoteIdentifier(identifier string) string {
	// Replace any double quotes with double-double quotes to escape them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	// Wrap in double quotes
	return `"` + escaped + `"`
}

// loadEvents retrieves events for the given stream using the specified options.
// This is shared functionality used by both producer and consumer.
func (p *pgClient) loadEvents(streamID string, opts eventstore.LoadOptions) ([]eventstore.Event, error) {
	query := fmt.Sprintf(`
		SELECT event_id, event_type, event_data, metadata, timestamp, version
		FROM %s
		WHERE stream_id = $1 AND version > $2
		ORDER BY version ASC
	`, quoteIdentifier(p.tableName))

	args := []interface{}{streamID, opts.FromVersion}

	if opts.Limit > 0 {
		query += " LIMIT $3"
		args = append(args, opts.Limit)
	}

	rows, err := p.db.Query(query, args...)
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
