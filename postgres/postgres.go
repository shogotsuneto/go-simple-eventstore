package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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

// postgresStore contains shared database functionality used by both producer and consumer.
type postgresStore struct {
	db        *sql.DB
	tableName string
}

// newPostgresStore creates a new shared postgres store with the given configuration.
func newPostgresStore(config Config) (*postgresStore, error) {
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

	return &postgresStore{
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
func (p *postgresStore) loadEvents(streamID string, opts eventstore.LoadOptions) ([]eventstore.Event, error) {
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
