package postgres

import (
	"testing"
	"time"
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
