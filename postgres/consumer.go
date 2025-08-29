package postgres

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// Compile-time interface compliance check
var _ eventstore.Consumer = (*PostgresEventConsumer)(nil)

// PostgresEventConsumer provides cursor-based consumer capabilities using PostgreSQL.
type PostgresEventConsumer struct {
	*pgClient
}

// parseMetadataJSON parses metadata from JSON string to map[string]string.
func parseMetadataJSON(metadataJSON string) (map[string]string, error) {
	var metadata map[string]string
	err := json.Unmarshal([]byte(metadataJSON), &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata JSON: %w", err)
	}
	return metadata, nil
}

// NewPostgresEventConsumer creates a new PostgreSQL event consumer with the given configuration.
func NewPostgresEventConsumer(config Config) (*PostgresEventConsumer, error) {
	client, err := newPgClient(config)
	if err != nil {
		return nil, err
	}

	return &PostgresEventConsumer{
		pgClient: client,
	}, nil
}

// cursorToID converts a cursor to an ID.
// Cursor format: 8 bytes containing the database row ID
func (s *PostgresEventConsumer) cursorToID(cursor eventstore.Cursor) int64 {
	if len(cursor) < 8 {
		return 0 // Start from beginning
	}
	
	return int64(binary.LittleEndian.Uint64(cursor[:8]))
}

// idToCursor converts an ID to a cursor.
func (s *PostgresEventConsumer) idToCursor(id int64) eventstore.Cursor {
	cursor := make([]byte, 8)
	binary.LittleEndian.PutUint64(cursor, uint64(id))
	return cursor
}

// eventToEnvelope converts an Event to an Envelope.
func (s *PostgresEventConsumer) eventToEnvelope(event eventstore.Event, streamID string) eventstore.Envelope {
	// Encode metadata as JSON bytes if present
	var metadataBytes []byte
	if event.Metadata != nil {
		// Simple encoding: concatenate key=value pairs with newlines
		metadataStr := ""
		for k, v := range event.Metadata {
			if metadataStr != "" {
				metadataStr += "\n"
			}
			metadataStr += k + "=" + v
		}
		metadataBytes = []byte(metadataStr)
	}

	return eventstore.Envelope{
		Type:       event.Type,
		Data:       event.Data,
		Metadata:   metadataBytes,
		StreamID:   streamID,
		CommitTime: event.Timestamp,
		EventID:    event.ID,
		Partition:  s.tableName, // Use table name as partition
		Offset:     fmt.Sprintf("%d", event.Version),
	}
}

// Fetch up to 'limit' events strictly after 'cursor'.
// Returns the batch and the *advanced* cursor (position after the last delivered event).
func (s *PostgresEventConsumer) Fetch(ctx context.Context, cursor eventstore.Cursor, limit int) (batch []eventstore.Envelope, next eventstore.Cursor, err error) {
	cursorID := s.cursorToID(cursor)
	
	query := fmt.Sprintf(`
		SELECT id, stream_id, event_id, event_type, event_data, metadata, timestamp, version
		FROM %s
		WHERE id > $1
		ORDER BY id ASC
		LIMIT $2
	`, s.tableName)

	rows, err := s.db.QueryContext(ctx, query, cursorID, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var result []eventstore.Envelope
	var lastID int64

	for rows.Next() {
		var id int64
		var streamID, eventID, eventType string
		var eventData []byte
		var metadataJSON *string
		var timestamp time.Time
		var version int64

		err := rows.Scan(&id, &streamID, &eventID, &eventType, &eventData, &metadataJSON, &timestamp, &version)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan event row: %w", err)
		}

		// Parse metadata
		var metadata map[string]string
		if metadataJSON != nil && *metadataJSON != "" {
			metadata, err = parseMetadataJSON(*metadataJSON)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
			}
		}

		event := eventstore.Event{
			ID:        eventID,
			Type:      eventType,
			Data:      eventData,
			Metadata:  metadata,
			Timestamp: timestamp,
			Version:   version,
		}

		envelope := s.eventToEnvelope(event, streamID)
		result = append(result, envelope)
		
		lastID = id
	}

	if err = rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Generate next cursor from last event processed
	var nextCursor eventstore.Cursor
	if len(result) > 0 {
		nextCursor = s.idToCursor(lastID)
	} else {
		// No events found, return the original cursor
		nextCursor = cursor
	}

	return result, nextCursor, nil
}

// Commit is called AFTER the projector has durably saved its own checkpoint.
// For PostgreSQL implementation, this is a no-op.
func (s *PostgresEventConsumer) Commit(ctx context.Context, cursor eventstore.Cursor) error {
	// No-op for PostgreSQL implementation
	return nil
}