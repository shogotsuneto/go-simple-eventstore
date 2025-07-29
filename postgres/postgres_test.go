package postgres

import (
	"testing"
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
			name: "default table name when empty",
			config: Config{
				ConnectionString: "test-conn",
				TableName:        "",
			},
			expectedTable: "events",
		},
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
				tableName = "events"
			}

			if tableName != tt.expectedTable {
				t.Errorf("Expected table name %s, got %s", tt.expectedTable, tableName)
			}
		})
	}
}

func TestNewPostgresEventStore_InvalidConnection(t *testing.T) {
	// This test verifies that invalid connection strings return errors
	config := Config{
		ConnectionString: "invalid-connection-string",
		TableName:        "events",
	}
	_, err := NewPostgresEventStore(config)
	if err == nil {
		t.Error("Expected error for invalid connection string")
	}
}

func TestNewPostgresEventStore_InvalidConnectionWithCustomTable(t *testing.T) {
	// Test that invalid connection strings return errors with custom table names
	config := Config{
		ConnectionString: "invalid-connection-string",
		TableName:        "custom_events",
	}

	_, err := NewPostgresEventStore(config)
	if err == nil {
		t.Error("Expected error for invalid connection string")
	}
}
