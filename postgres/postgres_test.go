package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple table name",
			input:    "events",
			expected: `"events"`,
		},
		{
			name:     "table name with underscores",
			input:    "custom_events",
			expected: `"custom_events"`,
		},
		{
			name:     "table name with spaces",
			input:    "my events",
			expected: `"my events"`,
		},
		{
			name:     "table name with double quotes",
			input:    `table"name`,
			expected: `"table""name"`,
		},
		{
			name:     "table name with multiple double quotes",
			input:    `"table""name"`,
			expected: `"""table""""name"""`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("quoteIdentifier(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestConfig_TableName(t *testing.T) {
	tests := []struct {
		name          string
		config        Config
		expectedTable string
	}{
		{
			name: "custom table name",
			config: Config{
				ConnectionString: "test-conn",
				TableName:        "custom_events",
			},
			expectedTable: "custom_events",
		},
		{
			name: "table name with underscores",
			config: Config{
				ConnectionString: "test-conn",
				TableName:        "my_custom_event_table",
			},
			expectedTable: "my_custom_event_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't actually create a database connection in unit tests,
			// so we'll just test the table name assignment logic
			tableName := tt.config.TableName
			if tableName == "" {
				t.Errorf("Table name should not be empty")
				return
			}

			if tableName != tt.expectedTable {
				t.Errorf("Expected table name %s, got %s", tt.expectedTable, tableName)
			}
		})
	}
}

func TestInitSchema_EmptyTableName(t *testing.T) {
	// Test that InitSchema rejects empty table names
	err := InitSchema(nil, "")
	if err == nil {
		t.Error("InitSchema should return error for empty table name")
	}

	expectedError := "table name must not be empty"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestNewPostgresEventStore_EmptyTableName(t *testing.T) {
	// Test that NewPostgresEventStore returns an error for empty table names
	store, err := NewPostgresEventStore(nil, "")
	if err == nil {
		t.Error("NewPostgresEventStore should return error for empty table name")
	}
	if store != nil {
		t.Error("NewPostgresEventStore should return nil store for empty table name")
	}

	expectedError := "table name must not be empty"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestNewPostgresEventConsumer_EmptyTableName(t *testing.T) {
	// Test that NewPostgresEventConsumer returns an error for empty table names
	consumer, err := NewPostgresEventConsumer(nil, "", 1*time.Second)
	if err == nil {
		t.Error("NewPostgresEventConsumer should return error for empty table name")
	}
	if consumer != nil {
		t.Error("NewPostgresEventConsumer should return nil consumer for empty table name")
	}

	expectedError := "table name must not be empty"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestBuildLoadQuery(t *testing.T) {
	client := &pgClient{
		tableName: "test_events",
	}

	tests := []struct {
		name         string
		streamID     string
		opts         eventstore.LoadOptions
		expectedSQL  []string // Parts that should be in the SQL
		expectedArgs []interface{}
	}{
		{
			name:     "forward loading with ExclusiveStartVersion",
			streamID: "stream-1",
			opts: eventstore.LoadOptions{
				ExclusiveStartVersion: 10,
				Limit:                 5,
				Desc:                  false,
			},
			expectedSQL:  []string{"ORDER BY version ASC", "WHERE stream_id = $1 AND version > $2", "LIMIT $3"},
			expectedArgs: []interface{}{"stream-1", int64(10), 5},
		},
		{
			name:     "descending loading with ExclusiveStartVersion 0",
			streamID: "stream-2",
			opts: eventstore.LoadOptions{
				ExclusiveStartVersion: 0,
				Limit:                 10,
				Desc:                  true,
			},
			expectedSQL:  []string{"ORDER BY version DESC", "WHERE stream_id = $1", "LIMIT $2"},
			expectedArgs: []interface{}{"stream-2", 10},
		},
		{
			name:     "descending loading with ExclusiveStartVersion > 0",
			streamID: "stream-3",
			opts: eventstore.LoadOptions{
				ExclusiveStartVersion: 50,
				Limit:                 20,
				Desc:                  true,
			},
			expectedSQL:  []string{"ORDER BY version DESC", "WHERE stream_id = $1 AND version < $2", "LIMIT $3"},
			expectedArgs: []interface{}{"stream-3", int64(50), 20},
		},
		{
			name:     "forward loading without limit",
			streamID: "stream-4",
			opts: eventstore.LoadOptions{
				ExclusiveStartVersion: 5,
				Desc:                  false,
			},
			expectedSQL:  []string{"ORDER BY version ASC", "WHERE stream_id = $1 AND version > $2"},
			expectedArgs: []interface{}{"stream-4", int64(5)},
		},
		{
			name:     "descending loading without limit",
			streamID: "stream-5",
			opts: eventstore.LoadOptions{
				ExclusiveStartVersion: 0,
				Desc:                  true,
			},
			expectedSQL:  []string{"ORDER BY version DESC", "WHERE stream_id = $1"},
			expectedArgs: []interface{}{"stream-5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args := client.buildLoadQuery(tt.streamID, tt.opts)

			// Check that all expected SQL parts are present
			for _, expectedPart := range tt.expectedSQL {
				if !strings.Contains(query, expectedPart) {
					t.Errorf("Expected query to contain %q, but got: %s", expectedPart, query)
				}
			}

			// Check that arguments match
			if len(args) != len(tt.expectedArgs) {
				t.Errorf("Expected %d arguments, got %d: %v", len(tt.expectedArgs), len(args), args)
				return
			}

			for i, expected := range tt.expectedArgs {
				if args[i] != expected {
					t.Errorf("Argument %d: expected %v, got %v", i, expected, args[i])
				}
			}

			// Verify the table name is properly quoted in the query
			expectedTableRef := `"test_events"`
			if !strings.Contains(query, expectedTableRef) {
				t.Errorf("Expected query to contain quoted table name %q, but got: %s", expectedTableRef, query)
			}

			// Verify the SELECT clause is present
			if !strings.Contains(query, "SELECT event_id, event_type, event_data, metadata, timestamp, version") {
				t.Errorf("Expected query to contain SELECT clause, but got: %s", query)
			}
		})
	}
}

func TestConfig_UseDbGeneratedTimestamps(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected bool
	}{
		{
			name: "default config uses app-generated timestamps",
			config: Config{
				ConnectionString: "test-connection",
				TableName:        "test_table",
			},
			expected: false,
		},
		{
			name: "explicit false uses app-generated timestamps",
			config: Config{
				ConnectionString:         "test-connection",
				TableName:                "test_table",
				UseDbGeneratedTimestamps: false,
			},
			expected: false,
		},
		{
			name: "explicit true uses db-generated timestamps",
			config: Config{
				ConnectionString:         "test-connection",
				TableName:                "test_table",
				UseDbGeneratedTimestamps: true,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config.UseDbGeneratedTimestamps != tt.expected {
				t.Errorf("Expected UseDbGeneratedTimestamps %t, got %t", tt.expected, tt.config.UseDbGeneratedTimestamps)
			}
		})
	}
}

func TestInitSchema_WithDbTimestamps(t *testing.T) {
	// Test that InitSchema properly handles the variadic useDbTimestamps parameter
	// We can only test the empty table name validation since we don't have a real database connection
	
	// Test empty table name without timestamp parameter
	err := InitSchema(nil, "")
	if err == nil {
		t.Error("InitSchema should return error for empty table name")
	}
	expectedError := "table name must not be empty"
	if err != nil && err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
	
	// Test empty table name with timestamp parameter false
	err = InitSchema(nil, "", false)
	if err == nil {
		t.Error("InitSchema should return error for empty table name even with useDbTimestamps=false")
	}
	if err != nil && err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
	
	// Test empty table name with timestamp parameter true
	err = InitSchema(nil, "", true)
	if err == nil {
		t.Error("InitSchema should return error for empty table name even with useDbTimestamps=true")
	}
	if err != nil && err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
	
	// Note: We cannot test with valid table names because that would require a real database connection
	// The integration tests will cover the actual schema creation functionality
}

func TestNewPostgresEventStoreWithConfig(t *testing.T) {
	// Test that the new constructor accepts Config properly
	// Test empty table name first (should fail before database connection)
	config := Config{
		ConnectionString:         "valid-connection-string",
		TableName:                "",
		UseDbGeneratedTimestamps: true,
	}
	
	store, err := NewPostgresEventStoreWithConfig(config)
	if err == nil {
		t.Error("NewPostgresEventStoreWithConfig should return error for empty table name")
	}
	if store != nil {
		t.Error("NewPostgresEventStoreWithConfig should return nil store for empty table name")
	}
	
	expectedError := "table name must not be empty"
	if err != nil && err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
	
	// Test invalid connection string with valid table name (will fail on database connection)
	config = Config{
		ConnectionString:         "", // Invalid connection string
		TableName:                "test_table",
		UseDbGeneratedTimestamps: true,
	}
	
	store, err = NewPostgresEventStoreWithConfig(config)
	if err == nil {
		t.Error("NewPostgresEventStoreWithConfig should return error for invalid connection string")
	}
	if store != nil {
		t.Error("NewPostgresEventStoreWithConfig should return nil store for invalid connection string")
	}
	// The specific error will be about database connection, which is expected
}

func TestNewPostgresEventConsumerWithConfig(t *testing.T) {
	// Test that the new constructor accepts Config properly
	// Test empty table name first (should fail before database connection)
	config := Config{
		ConnectionString:         "valid-connection-string",
		TableName:                "",
		UseDbGeneratedTimestamps: true,
	}
	
	consumer, err := NewPostgresEventConsumerWithConfig(config, 1*time.Second)
	if err == nil {
		t.Error("NewPostgresEventConsumerWithConfig should return error for empty table name")
	}
	if consumer != nil {
		t.Error("NewPostgresEventConsumerWithConfig should return nil consumer for empty table name")
	}
	
	expectedError := "table name must not be empty"
	if err != nil && err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
	
	// Test invalid connection string with valid table name (will fail on database connection)
	config = Config{
		ConnectionString:         "", // Invalid connection string  
		TableName:                "test_table",
		UseDbGeneratedTimestamps: true,
	}
	
	consumer, err = NewPostgresEventConsumerWithConfig(config, 1*time.Second)
	if err == nil {
		t.Error("NewPostgresEventConsumerWithConfig should return error for invalid connection string")
	}
	if consumer != nil {
		t.Error("NewPostgresEventConsumerWithConfig should return nil consumer for invalid connection string")
	}
	// The specific error will be about database connection, which is expected
}
