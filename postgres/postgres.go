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
// It takes a database connection, table name, and useClientTimestamps flag to initialize the schema.
// tableName must not be empty.
// useClientTimestamps controls whether the timestamp column should have DEFAULT CURRENT_TIMESTAMP.
// When false (default), the database generates timestamps; when true, the application generates them.
func InitSchema(db *sql.DB, tableName string, useClientTimestamps bool) error {
	if tableName == "" {
		return fmt.Errorf("table name must not be empty")
	}

	quotedTableName := quoteIdentifier(tableName)

	// Build timestamp column definition - default is database-generated
	timestampColumn := "timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP"
	if useClientTimestamps {
		timestampColumn = "timestamp TIMESTAMP WITH TIME ZONE NOT NULL"
	}

	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		stream_id VARCHAR(255) NOT NULL,
		version BIGINT NOT NULL,
		event_id VARCHAR(255) NOT NULL,
		event_type VARCHAR(255) NOT NULL,
		event_data BYTEA NOT NULL,
		metadata JSONB,
		%s
	);

	CREATE INDEX IF NOT EXISTS %s ON %s(stream_id);
	CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(stream_id, version);
	CREATE INDEX IF NOT EXISTS %s ON %s(timestamp);
	`, quotedTableName, timestampColumn,
		quoteIdentifier("idx_"+tableName+"_stream_id"), quotedTableName,
		quoteIdentifier("idx_"+tableName+"_stream_version"), quotedTableName,
		quoteIdentifier("idx_"+tableName+"_timestamp"), quotedTableName)

	_, err := db.Exec(query)
	return err
}

// Config contains configuration options for PostgresEventStore.
type Config struct {
	// ConnectionString is the PostgreSQL connection string
	ConnectionString string
	// TableName is the name of the table to store events. Must not be empty.
	TableName string
	// UseClientGeneratedTimestamps controls whether to use client-generated timestamps.
	// When false (default), the database generates timestamps using DEFAULT CURRENT_TIMESTAMP.
	// When true, timestamps are generated in the application layer.
	UseClientGeneratedTimestamps bool
}

// pgClient contains shared database functionality used by both producer and consumer.
type pgClient struct {
	db                           *sql.DB
	tableName                    string
	useClientGeneratedTimestamps bool
}

// newPgClient creates a new shared postgres client with the given configuration.
func newPgClient(config Config) (*pgClient, error) {
	tableName := config.TableName
	if tableName == "" {
		return nil, fmt.Errorf("table name must not be empty")
	}

	db, err := sql.Open("postgres", config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &pgClient{
		db:                           db,
		tableName:                    tableName,
		useClientGeneratedTimestamps: config.UseClientGeneratedTimestamps,
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

// buildLoadQuery constructs the SQL query and arguments for loading events.
func (p *pgClient) buildLoadQuery(streamID string, opts eventstore.LoadOptions) (string, []interface{}) {
	var args []interface{}

	// SELECT clause
	selectClause := "SELECT event_id, event_type, event_data, metadata, timestamp, version"

	// FROM clause
	fromClause := fmt.Sprintf("FROM %s", quoteIdentifier(p.tableName))

	// WHERE clause
	var whereClause string
	if opts.Desc {
		// In reverse loading: if ExclusiveStartVersion is 0, include all events
		// Otherwise, include events with version < ExclusiveStartVersion
		if opts.ExclusiveStartVersion == 0 {
			whereClause = "WHERE stream_id = $1"
			args = []interface{}{streamID}
		} else {
			whereClause = "WHERE stream_id = $1 AND version < $2"
			args = []interface{}{streamID, opts.ExclusiveStartVersion}
		}
	} else {
		whereClause = "WHERE stream_id = $1 AND version > $2"
		args = []interface{}{streamID, opts.ExclusiveStartVersion}
	}

	// ORDER BY clause
	var orderClause string
	if opts.Desc {
		orderClause = "ORDER BY version DESC"
	} else {
		orderClause = "ORDER BY version ASC"
	}

	// Build the main query
	query := fmt.Sprintf("%s\n%s\n%s\n%s", selectClause, fromClause, whereClause, orderClause)

	// LIMIT clause
	if opts.Limit > 0 {
		query += fmt.Sprintf("\nLIMIT $%d", len(args)+1)
		args = append(args, opts.Limit)
	}

	return query, args
}

// loadEvents retrieves events for the given stream using the specified options.
// This is shared functionality used by both producer and consumer.
func (p *pgClient) loadEvents(streamID string, opts eventstore.LoadOptions) ([]eventstore.Event, error) {
	query, args := p.buildLoadQuery(streamID, opts)

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
