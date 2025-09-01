# PostgreSQL Schema Documentation

This document describes the table schema requirements for the PostgreSQL adapter of go-simple-eventstore. You can use this information to manually create the event table using SQL instead of relying on the `InitSchema` function.

## Table Schema

The event store requires a single table to store all events. The table name is configurable and can be any valid PostgreSQL identifier.

### Basic Table Structure

```sql
CREATE TABLE your_table_name (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_data BYTEA NOT NULL,
    metadata JSONB,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### Column Descriptions

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| `id` | `SERIAL` | `PRIMARY KEY` | Auto-incrementing unique identifier for each event record |
| `stream_id` | `VARCHAR(255)` | `NOT NULL` | Identifier for the event stream (groups related events) |
| `version` | `BIGINT` | `NOT NULL` | Version number within the stream (used for ordering and concurrency control) |
| `event_id` | `VARCHAR(255)` | `NOT NULL` | Unique identifier for the individual event |
| `event_type` | `VARCHAR(255)` | `NOT NULL` | Type/name of the event (e.g., "UserCreated", "OrderPlaced") |
| `event_data` | `BYTEA` | `NOT NULL` | Serialized event payload (typically JSON stored as bytes) |
| `metadata` | `JSONB` | `NULL` | Optional metadata associated with the event |
| `timestamp` | `TIMESTAMP WITH TIME ZONE` | `NOT NULL` | When the event was created (see timestamp modes below) |

### Required Indexes

The following indexes are essential for performance and data integrity:

```sql
-- Index for efficient stream-based queries
CREATE INDEX idx_your_table_name_stream_id ON your_table_name(stream_id);

-- Unique constraint ensuring no duplicate versions within a stream
-- This enables optimistic concurrency control
CREATE UNIQUE INDEX idx_your_table_name_stream_version ON your_table_name(stream_id, version);

-- Index for timestamp-based queries (used by event consumers)
CREATE INDEX idx_your_table_name_timestamp ON your_table_name(timestamp);
```

**Note**: Replace `your_table_name` with your actual table name in the index names.

## Timestamp Modes

The PostgreSQL adapter supports two timestamp generation modes:

### Database-Generated Timestamps (Default)

This is the recommended mode for most use cases as it provides better consistency and avoids clock skew issues.

```sql
-- Timestamp column with database default
timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
```

**Configuration**: Set `UseClientGeneratedTimestamps: false` (default)

### Client-Generated Timestamps

Use this mode when you need precise control over event timestamps or when events are being migrated from another system.

```sql
-- Timestamp column without default (application must provide value)
timestamp TIMESTAMP WITH TIME ZONE NOT NULL
```

**Configuration**: Set `UseClientGeneratedTimestamps: true`

**Important**: When using client-generated timestamps, your application must always provide a timestamp value when appending events, or you'll get NOT NULL constraint violations.

## Complete Schema Examples

### Example 1: Standard Event Table (Database Timestamps)

```sql
-- Create the events table with database-generated timestamps
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_data BYTEA NOT NULL,
    metadata JSONB,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create performance and integrity indexes
CREATE INDEX idx_events_stream_id ON events(stream_id);
CREATE UNIQUE INDEX idx_events_stream_version ON events(stream_id, version);
CREATE INDEX idx_events_timestamp ON events(timestamp);
```

### Example 2: Custom Table with Client Timestamps

```sql
-- Create a custom-named table with client-generated timestamps
CREATE TABLE user_events (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_data BYTEA NOT NULL,
    metadata JSONB,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Create indexes with matching names
CREATE INDEX idx_user_events_stream_id ON user_events(stream_id);
CREATE UNIQUE INDEX idx_user_events_stream_version ON user_events(stream_id, version);
CREATE INDEX idx_user_events_timestamp ON user_events(timestamp);
```

## Schema Validation

You can verify your manually created schema is compatible by running the integration tests:

```bash
# Start PostgreSQL (if not already running)
cd integration_test
docker compose -f docker-compose.test.yaml up -d postgres

# Run integration tests
go test -tags=integration ./integration_test -v
```

## Common Issues

### Missing Indexes
- **Problem**: Slow queries when loading events from streams
- **Solution**: Ensure the `stream_id` index exists
- **Problem**: Duplicate version errors not caught
- **Solution**: Ensure the unique `(stream_id, version)` index exists

### Timestamp Configuration Mismatch
- **Problem**: NOT NULL constraint violations on timestamp column
- **Solution**: Ensure your table schema matches your `UseClientGeneratedTimestamps` configuration
  - If `UseClientGeneratedTimestamps: false`, use `DEFAULT CURRENT_TIMESTAMP`
  - If `UseClientGeneratedTimestamps: true`, omit the default

### Table Name Issues
- **Problem**: Table not found errors
- **Solution**: Ensure your `Config.TableName` exactly matches your created table name
- **Note**: Table names with special characters should be quoted in DDL

## Migration from InitSchema

If you've been using `InitSchema` and want to switch to manual schema management:

1. **Document current schema**: Use `\d your_table_name` in psql to see the current structure
2. **Create migration scripts**: Write SQL scripts that match the documented schema above
3. **Test thoroughly**: Verify compatibility with your existing data and application code
4. **Update deployment**: Replace `InitSchema` calls with your custom schema creation logic

## PostgreSQL Version Compatibility

This schema is compatible with PostgreSQL 12 and later. Key features used:
- `JSONB` type (PostgreSQL 9.4+)
- `TIMESTAMP WITH TIME ZONE` (PostgreSQL 7.2+)
- `SERIAL` type (PostgreSQL 6.1+)

For older PostgreSQL versions, you may need to adjust the `JSONB` type to `JSON` or `TEXT`.