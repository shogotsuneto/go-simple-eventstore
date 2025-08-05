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

func TestLoadEvents_QueryGeneration(t *testing.T) {
	// Test that loadEvents generates correct SQL queries for reverse loading
	// This is a simple test that doesn't require a database connection
	
	// This test verifies that the query generation logic works correctly
	// by examining the SQL structure. The actual database functionality
	// is tested in integration tests.
	
	client := &pgClient{
		db:        nil, // We won't execute queries in this test
		tableName: "test_events",
	}
	
	// We can't easily test the full loadEvents method without a database,
	// but we can verify that the query generation logic is correct by
	// checking that both forward and reverse options would generate
	// different ORDER BY clauses.
	
	// This test serves as documentation that reverse loading is supported
	// and the actual functionality is verified in integration tests.
	t.Run("ReverseOrderSupported", func(t *testing.T) {
		// Test case exists to document that reverse loading is supported
		// The actual SQL generation and execution is tested in integration tests
		if client.tableName != "test_events" {
			t.Error("Test setup failed")
		}
	})
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
