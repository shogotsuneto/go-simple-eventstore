//go:build integration
// +build integration

package integration_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/postgres"
)

func getTestConnectionString() string {
	// Default connection string for testing
	connStr := "host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable"
	
	// Allow override via environment variable
	if envConnStr := os.Getenv("TEST_DATABASE_URL"); envConnStr != "" {
		connStr = envConnStr
	}
	
	return connStr
}

func setupTestStore(t *testing.T, tableName string) (eventstore.EventStore, *sql.DB) {
	connStr := getTestConnectionString()
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	if err := postgres.InitSchema(db, tableName, false); err != nil {
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	config := postgres.Config{
		ConnectionString:          connStr,
		TableName:                 tableName,
		UseClientGeneratedTimestamps: false, // Use database-generated timestamps (default)
	}
	
	store, err := postgres.NewPostgresEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL event store: %v", err)
	}
	return store, db
}

// Helper function to check if a table exists in the database
func checkTableExists(t *testing.T, connectionString, tableName string) bool {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()
	
	var exists bool
	query := `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_name = $1
	)`
	
	err = db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to check if table exists: %v", err)
	}
	
	return exists
}

// Helper function to count events in a specific table
func countEventsInTable(t *testing.T, connectionString, tableName string) int {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()
	
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count events in table %s: %v", tableName, err)
	}
	
	return count
}

func TestPostgresEventStore_Integration_Append(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()
	defer db.Close()

	events := []eventstore.Event{
		{
			Type: "TestEvent1",
			Data: []byte(`{"test": "data1"}`),
			Metadata: map[string]string{
				"source": "test",
			},
		},
		{
			Type: "TestEvent2",
			Data: []byte(`{"test": "data2"}`),
			Metadata: map[string]string{
				"source": "test",
			},
		},
	}

	streamID := "integration-test-stream-" + time.Now().Format("20060102150405")
	_, err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(loadedEvents) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(loadedEvents))
	}

	// Check that versions were assigned correctly
	if loadedEvents[0].Version != 1 {
		t.Errorf("Expected first event version to be 1, got %d", loadedEvents[0].Version)
	}
	if loadedEvents[1].Version != 2 {
		t.Errorf("Expected second event version to be 2, got %d", loadedEvents[1].Version)
	}

	// Check that IDs were generated
	if loadedEvents[0].ID == "" {
		t.Error("Expected first event to have an ID")
	}
	if loadedEvents[1].ID == "" {
		t.Error("Expected second event to have an ID")
	}

	// Check that timestamps were set
	if loadedEvents[0].Timestamp.IsZero() {
		t.Error("Expected first event to have a timestamp")
	}
	if loadedEvents[1].Timestamp.IsZero() {
		t.Error("Expected second event to have a timestamp")
	}

	// Check event data
	if loadedEvents[0].Type != "TestEvent1" {
		t.Errorf("Expected first event type to be 'TestEvent1', got '%s'", loadedEvents[0].Type)
	}
	if loadedEvents[1].Type != "TestEvent2" {
		t.Errorf("Expected second event type to be 'TestEvent2', got '%s'", loadedEvents[1].Type)
	}
}

func TestPostgresEventStore_Integration_Load_EmptyStream(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()
	defer db.Close()

	streamID := "non-existent-stream-" + time.Now().Format("20060102150405")
	events, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("Expected 0 events for non-existent stream, got %d", len(events))
	}
}

func TestPostgresEventStore_Integration_Load_WithVersion(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()

	// Add some events
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}

	streamID := "version-test-stream-" + time.Now().Format("20060102150405")
	_, err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load events starting from version 1 (should get events 2 and 3)
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 1, Limit: 10})
	if err != nil {
		t.Fatalf("Load with version failed: %v", err)
	}

	if len(loadedEvents) != 2 {
		t.Fatalf("Expected 2 events with afterVersion 1, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Type != "Event2" {
		t.Errorf("Expected first loaded event to be 'Event2', got '%s'", loadedEvents[0].Type)
	}
	if loadedEvents[1].Type != "Event3" {
		t.Errorf("Expected second loaded event to be 'Event3', got '%s'", loadedEvents[1].Type)
	}
}

func TestPostgresEventStore_Integration_Load_WithLimit(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()

	// Add some events
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}

	streamID := "limit-test-stream-" + time.Now().Format("20060102150405")
	_, err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load only 2 events
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 2})
	if err != nil {
		t.Fatalf("Load with limit failed: %v", err)
	}

	if len(loadedEvents) != 2 {
		t.Fatalf("Expected 2 events with limit 2, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Type != "Event1" {
		t.Errorf("Expected first loaded event to be 'Event1', got '%s'", loadedEvents[0].Type)
	}
	if loadedEvents[1].Type != "Event2" {
		t.Errorf("Expected second loaded event to be 'Event2', got '%s'", loadedEvents[1].Type)
	}
}



func TestPostgresEventStore_Integration_ConcurrentAppends(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()

	streamID := "concurrent-test-stream-" + time.Now().Format("20060102150405")
	
	// Create multiple goroutines that append events concurrently
	done := make(chan error, 3)
	
	for i := 0; i < 3; i++ {
		go func(index int) {
			events := []eventstore.Event{
				{
					Type: "ConcurrentEvent",
					Data: []byte(`{"worker": ` + string(rune('0'+index)) + `}`),
					Metadata: map[string]string{
						"worker": string(rune('0' + index)),
					},
				},
			}
			_, err := store.Append(streamID, events, -1)
			done <- err
		}(i)
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 3; i++ {
		err := <-done
		if err != nil {
			t.Errorf("Concurrent append failed: %v", err)
		}
	}
	
	// Verify all events were stored with correct versions
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	
	if len(loadedEvents) != 3 {
		t.Fatalf("Expected 3 events, got %d", len(loadedEvents))
	}
	
	// Check that versions are sequential
	for i, event := range loadedEvents {
		expectedVersion := int64(i + 1)
		if event.Version != expectedVersion {
			t.Errorf("Expected event %d to have version %d, got %d", i, expectedVersion, event.Version)
		}
	}
}

func TestPostgresEventStore_Integration_ExpectedVersion_NewStream(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()

	streamID := "expected-version-new-stream-" + time.Now().Format("20060102150405")
	
	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": "data"}`)},
	}

	// Should succeed when creating a new stream with expectedVersion 0
	_, err := store.Append(streamID, events, 0)
	if err != nil {
		t.Fatalf("Expected successful append to new stream with version 0, got error: %v", err)
	}

	// Verify the event was stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(loadedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Version != 1 {
		t.Errorf("Expected event version to be 1, got %d", loadedEvents[0].Version)
	}

	// Should fail when trying to create the same stream again with expectedVersion 0
	_, err = store.Append(streamID, events, 0)
	if err == nil {
		t.Fatal("Expected error when trying to create existing stream with version 0")
	}
}

func TestPostgresEventStore_Integration_ExpectedVersion_ExactMatch(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()

	streamID := "expected-version-exact-match-" + time.Now().Format("20060102150405")

	// Create initial event
	events1 := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
	}
	_, err := store.Append(streamID, events1, 0)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Should succeed when appending to version 1
	events2 := []eventstore.Event{
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}
	_, err = store.Append(streamID, events2, 1)
	if err != nil {
		t.Fatalf("Expected successful append with correct expected version, got error: %v", err)
	}

	// Verify both events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(loadedEvents) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Version != 1 {
		t.Errorf("Expected first event version to be 1, got %d", loadedEvents[0].Version)
	}
	if loadedEvents[1].Version != 2 {
		t.Errorf("Expected second event version to be 2, got %d", loadedEvents[1].Version)
	}

	// Should fail when trying to append with wrong expected version
	events3 := []eventstore.Event{
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}
	_, err = store.Append(streamID, events3, 1)
	if err == nil {
		t.Fatal("Expected error when appending with wrong expected version")
	}

	// Should also fail when trying to append with a higher expected version
	_, err = store.Append(streamID, events3, 5)
	if err == nil {
		t.Fatal("Expected error when appending with higher expected version")
	}
}

func TestPostgresEventStore_Integration_ExpectedVersion_NoCheck(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()

	streamID := "expected-version-no-check-" + time.Now().Format("20060102150405")
	
	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": "data"}`)},
	}

	// Should always succeed with expectedVersion -1 (no check)
	_, err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Expected successful append with version -1, got error: %v", err)
	}

	// Should succeed again with expectedVersion -1
	_, err = store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Expected successful append with version -1, got error: %v", err)
	}

	// Verify both events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(loadedEvents) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Version != 1 {
		t.Errorf("Expected first event version to be 1, got %d", loadedEvents[0].Version)
	}
	if loadedEvents[1].Version != 2 {
		t.Errorf("Expected second event version to be 2, got %d", loadedEvents[1].Version)
	}
}

func TestPostgresEventStore_Integration_ConcurrencyConflictErrors(t *testing.T) {
	store, db := setupTestStore(t, "test_events")
	defer db.Close()

	streamID := "conflict-test-stream-" + time.Now().Format("20060102150405")
	
	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": "data"}`)},
	}

	// Test ErrStreamAlreadyExists error type
	t.Run("StreamAlreadyExists", func(t *testing.T) {
		// Create stream with expectedVersion 0
		_, err := store.Append(streamID, events, 0)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}

		// Try to create the same stream again with expectedVersion 0
		_, err = store.Append(streamID, events, 0)
		if err == nil {
			t.Fatal("Expected error when trying to create existing stream")
		}

		// Verify it's the correct error type
		var streamExistsErr *eventstore.ErrStreamAlreadyExists
		if !errors.As(err, &streamExistsErr) {
			t.Fatalf("Expected ErrStreamAlreadyExists, got %T: %v", err, err)
		}

		// Verify error details
		if streamExistsErr.StreamID != streamID {
			t.Errorf("Expected StreamID %s, got %s", streamID, streamExistsErr.StreamID)
		}
		if streamExistsErr.ActualVersion != 1 {
			t.Errorf("Expected ActualVersion 1, got %d", streamExistsErr.ActualVersion)
		}

		// Verify error message
		expectedMsg := fmt.Sprintf("expected new stream '%s' (version 0) but stream already exists with 1 events", streamID)
		if streamExistsErr.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, streamExistsErr.Error())
		}
	})

	// Test ErrVersionMismatch error type
	t.Run("VersionMismatch", func(t *testing.T) {
		// Try to append with wrong expected version (should expect 1, not 2)
		_, err := store.Append(streamID, events, 2) // Stream is at version 1, expecting 2
		if err == nil {
			t.Fatal("Expected error when appending with wrong expected version")
		}

		// Verify it's the correct error type
		var versionMismatchErr *eventstore.ErrVersionMismatch
		if !errors.As(err, &versionMismatchErr) {
			t.Fatalf("Expected ErrVersionMismatch, got %T: %v", err, err)
		}

		// Verify error details
		if versionMismatchErr.StreamID != streamID {
			t.Errorf("Expected StreamID %s, got %s", streamID, versionMismatchErr.StreamID)
		}
		if versionMismatchErr.ExpectedVersion != 2 {
			t.Errorf("Expected ExpectedVersion 2, got %d", versionMismatchErr.ExpectedVersion)
		}
		if versionMismatchErr.ActualVersion != 1 {
			t.Errorf("Expected ActualVersion 1, got %d", versionMismatchErr.ActualVersion)
		}

		// Verify error message
		expectedMsg := fmt.Sprintf("expected version 2 but stream '%s' is at version 1", streamID)
		if versionMismatchErr.Error() != expectedMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedMsg, versionMismatchErr.Error())
		}
	})

	// Test another version mismatch scenario
	t.Run("VersionMismatchHigher", func(t *testing.T) {
		// Try to append with higher expected version
		_, err := store.Append(streamID, events, 5) // Stream is at version 1, expecting 5
		if err == nil {
			t.Fatal("Expected error when appending with higher expected version")
		}

		// Verify it's the correct error type
		var versionMismatchErr *eventstore.ErrVersionMismatch
		if !errors.As(err, &versionMismatchErr) {
			t.Fatalf("Expected ErrVersionMismatch, got %T: %v", err, err)
		}

		// Verify error details
		if versionMismatchErr.ExpectedVersion != 5 {
			t.Errorf("Expected ExpectedVersion 5, got %d", versionMismatchErr.ExpectedVersion)
		}
		if versionMismatchErr.ActualVersion != 1 {
			t.Errorf("Expected ActualVersion 1, got %d", versionMismatchErr.ActualVersion)
		}
	})
}

// Tests for configurable table name functionality

func TestPostgresEventStore_Integration_CustomTableName(t *testing.T) {
	customTableName := "custom_events_" + time.Now().Format("20060102150405")
	
	// First check that the custom table doesn't exist
	connStr := getTestConnectionString()
	if checkTableExists(t, connStr, customTableName) {
		t.Fatalf("Custom table %s should not exist before initialization", customTableName)
	}
	
	store, db := setupTestStore(t, customTableName)
	defer db.Close()

	// Check that the custom table was created
	if !checkTableExists(t, connStr, customTableName) {
		t.Fatalf("Custom table %s should exist after initialization", customTableName)
	}

	// Append a single event to test functionality
	events := []eventstore.Event{
		{
			Type: "TestEvent",
			Data: []byte(`{"test": "data"}`),
			Metadata: map[string]string{
				"source": "test",
			},
		},
	}

	streamID := "custom-table-test-stream-" + time.Now().Format("20060102150405")
	_, err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Check that event was stored in the custom table
	quotedCustomTableName := fmt.Sprintf(`"%s"`, customTableName)
	eventCount := countEventsInTable(t, connStr, quotedCustomTableName)
	if eventCount != 1 {
		t.Errorf("Expected 1 event in custom table %s, got %d", customTableName, eventCount)
	}

	// Test that Load also works with the custom table name
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed from custom table: %v", err)
	}

	if len(loadedEvents) != 1 {
		t.Fatalf("Expected 1 event loaded from custom table, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Type != "TestEvent" {
		t.Errorf("Expected event type 'TestEvent', got '%s'", loadedEvents[0].Type)
	}
}

func TestPostgresEventStore_Load_Desc(t *testing.T) {
	tableName := "test_events_desc"
	store, db := setupTestStore(t, tableName)
	defer db.Close()

	// Create test events
	events := []eventstore.Event{
		{
			Type: "Event1",
			Data: []byte(`{"test": "data1"}`),
		},
		{
			Type: "Event2",
			Data: []byte(`{"test": "data2"}`),
		},
		{
			Type: "Event3",
			Data: []byte(`{"test": "data3"}`),
		},
		{
			Type: "Event4",
			Data: []byte(`{"test": "data4"}`),
		},
	}

	streamID := fmt.Sprintf("test-stream-desc-%d", time.Now().UnixNano())
	
	// Append events to stream
	_, err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	t.Run("DescLoadAll", func(t *testing.T) {
		// Load all events in descending order
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			ExclusiveStartVersion: 0,
			Desc:      true,
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loadedEvents) != 4 {
			t.Fatalf("Expected 4 events, got %d", len(loadedEvents))
		}

		// Verify events are in descending order (latest first)
		expectedTypes := []string{"Event4", "Event3", "Event2", "Event1"}
		expectedVersions := []int64{4, 3, 2, 1}

		for i, event := range loadedEvents {
			if event.Type != expectedTypes[i] {
				t.Errorf("Event %d: expected type %s, got %s", i, expectedTypes[i], event.Type)
			}
			if event.Version != expectedVersions[i] {
				t.Errorf("Event %d: expected version %d, got %d", i, expectedVersions[i], event.Version)
			}
		}
	})

	t.Run("DescLoadWithLimit", func(t *testing.T) {
		// Load latest 2 events in descending order
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			ExclusiveStartVersion: 0,
			Limit:        2,
			Desc:      true,
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loadedEvents) != 2 {
			t.Fatalf("Expected 2 events, got %d", len(loadedEvents))
		}

		// Should get the latest 2 events (Event4, Event3)
		expectedTypes := []string{"Event4", "Event3"}
		expectedVersions := []int64{4, 3}

		for i, event := range loadedEvents {
			if event.Type != expectedTypes[i] {
				t.Errorf("Event %d: expected type %s, got %s", i, expectedTypes[i], event.Type)
			}
			if event.Version != expectedVersions[i] {
				t.Errorf("Event %d: expected version %d, got %d", i, expectedVersions[i], event.Version)
			}
		}
	})

	t.Run("DescLoadWithExclusiveStartVersion", func(t *testing.T) {
		// Load events before version 3 in descending order (should get Event2, Event1)
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			ExclusiveStartVersion: 3,
			Desc:      true,
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loadedEvents) != 2 {
			t.Fatalf("Expected 2 events, got %d", len(loadedEvents))
		}

		// Should get Event2, Event1 (versions 2, 1) in descending order
		expectedTypes := []string{"Event2", "Event1"}
		expectedVersions := []int64{2, 1}

		for i, event := range loadedEvents {
			if event.Type != expectedTypes[i] {
				t.Errorf("Event %d: expected type %s, got %s", i, expectedTypes[i], event.Type)
			}
			if event.Version != expectedVersions[i] {
				t.Errorf("Event %d: expected version %d, got %d", i, expectedVersions[i], event.Version)
			}
		}
	})

	t.Run("DefaultBehaviorIsForward", func(t *testing.T) {
		// Verify that default behavior (Desc field not set) is forward loading
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			ExclusiveStartVersion: 0,
			// Desc field omitted, should default to false
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loadedEvents) != 4 {
			t.Fatalf("Expected 4 events, got %d", len(loadedEvents))
		}

		// Should be in forward order
		if loadedEvents[0].Type != "Event1" || loadedEvents[3].Type != "Event4" {
			t.Errorf("Default behavior should load in forward order")
		}
	})
}

func TestPostgresEventStore_Integration_ClientGeneratedTimestamps(t *testing.T) {
	// Test client-generated timestamps (non-default behavior)
	tableName := "events_client_timestamps_test"
	
	t.Run("ClientGeneratedTimestamps", func(t *testing.T) {
		// Setup database connection
		db, err := sql.Open("postgres", getTestConnectionString())
		if err != nil {
			t.Fatalf("Failed to open database connection: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			t.Fatalf("Failed to ping database: %v", err)
		}

		// Initialize schema for client-generated timestamps
		if err := postgres.InitSchema(db, tableName, true); err != nil {
			t.Fatalf("Failed to initialize schema for client-generated timestamps: %v", err)
		}

		// Create event store with client-generated timestamps
		config := postgres.Config{
			ConnectionString:          getTestConnectionString(),
			TableName:                 tableName,
			UseClientGeneratedTimestamps: true,
		}
		store, err := postgres.NewPostgresEventStore(config)
		if err != nil {
			t.Fatalf("Failed to create PostgreSQL event store: %v", err)
		}

		// Test event with client-generated timestamp
		beforeAppend := time.Now()
		events := []eventstore.Event{
			{
				Type: "TestEvent",
				Data: []byte(`{"test": "client-generated"}`),
				Metadata: map[string]string{
					"source": "integration-test",
				},
			},
		}

		streamID := "test-stream-client-" + fmt.Sprintf("%d", time.Now().UnixNano())
		_, err = store.Append(streamID, events, -1)
		if err != nil {
			t.Fatalf("Failed to append events: %v", err)
		}
		afterAppend := time.Now()

		// Load the event back and verify timestamp is within expected range
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
		if err != nil {
			t.Fatalf("Failed to load events: %v", err)
		}

		if len(loadedEvents) != 1 {
			t.Fatalf("Expected 1 event, got %d", len(loadedEvents))
		}

		event := loadedEvents[0]
		if event.Timestamp.Before(beforeAppend) || event.Timestamp.After(afterAppend) {
			t.Errorf("Client-generated timestamp %v should be between %v and %v", 
				event.Timestamp, beforeAppend, afterAppend)
		}

		// Test that client-provided timestamps are used
		fixedTime := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
		eventsWithTimestamp := []eventstore.Event{
			{
				Type:      "TestEventWithTimestamp",
				Data:      []byte(`{"test": "explicit-timestamp"}`),
				Timestamp: fixedTime, // This should be used
			},
		}

		streamID2 := "test-stream-explicit-" + fmt.Sprintf("%d", time.Now().UnixNano())
		_, err = store.Append(streamID2, eventsWithTimestamp, -1)
		if err != nil {
			t.Fatalf("Failed to append events with explicit timestamp: %v", err)
		}

		loadedEvents2, err := store.Load(streamID2, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
		if err != nil {
			t.Fatalf("Failed to load events with explicit timestamp: %v", err)
		}

		if len(loadedEvents2) != 1 {
			t.Fatalf("Expected 1 event, got %d", len(loadedEvents2))
		}

		event2 := loadedEvents2[0]
		// Verify that the client-provided timestamp was used
		if !event2.Timestamp.Equal(fixedTime) {
			t.Errorf("Expected client-provided timestamp %v, but got %v", fixedTime, event2.Timestamp)
		}

		// Clean up table
		_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableName))
		if err != nil {
			t.Fatalf("Failed to clean up table: %v", err)
		}
	})

	t.Run("MismatchedConfiguration_ShouldError", func(t *testing.T) {
		// Test configuration mismatch: schema without default, client expecting database timestamps
		// This should result in an error due to NOT NULL constraint violation
		
		// Setup database connection
		db, err := sql.Open("postgres", getTestConnectionString())
		if err != nil {
			t.Fatalf("Failed to open database connection: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			t.Fatalf("Failed to ping database: %v", err)
		}

		// Initialize schema for client-generated timestamps (no DEFAULT CURRENT_TIMESTAMP)
		if err := postgres.InitSchema(db, tableName, true); err != nil {
			t.Fatalf("Failed to initialize schema for client-generated timestamps: %v", err)
		}

		// Create event store with database-generated timestamps enabled (mismatch!)
		config := postgres.Config{
			ConnectionString:          getTestConnectionString(),
			TableName:                 tableName,
			UseClientGeneratedTimestamps: false, // This is the mismatch - schema expects client timestamps but client expects DB to generate
		}
		store, err := postgres.NewPostgresEventStore(config)
		if err != nil {
			t.Fatalf("Failed to create PostgreSQL event store: %v", err)
		}

		// Try to append events - this should fail with NOT NULL constraint violation
		events := []eventstore.Event{
			{
				Type: "TestEvent",
				Data: []byte(`{"test": "mismatch-config"}`),
				Metadata: map[string]string{
					"source": "integration-test",
				},
			},
		}

		streamID := "test-stream-mismatch-" + fmt.Sprintf("%d", time.Now().UnixNano())
		_, err = store.Append(streamID, events, -1)
		
		// We expect this to fail with a database error about NOT NULL constraint
		if err == nil {
			t.Fatalf("Expected error due to configuration mismatch (schema expecting client timestamps, client expecting DB generation), but got nil")
		}
		
		// Verify it's specifically a NOT NULL constraint error
		errMsg := err.Error()
		if !strings.Contains(strings.ToLower(errMsg), "not null") && 
		   !strings.Contains(strings.ToLower(errMsg), "null value") {
			t.Errorf("Expected NOT NULL constraint error, but got different error: %v", err)
		}

		// Clean up table
		_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableName))
		if err != nil {
			t.Fatalf("Failed to clean up table: %v", err)
		}
	})
}// TestPostgresEventStore_Integration_AppendReturnsVersion tests that Append returns the correct latest version for PostgreSQL
func TestPostgresEventStore_Integration_AppendReturnsVersion(t *testing.T) {
	store, db := setupTestStore(t, "test_version_return")
	defer db.Close()

	streamID := "test-version-return-" + time.Now().Format("20060102150405")

	// Test appending to empty stream
	events1 := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}
	
	latestVersion, err := store.Append(streamID, events1, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	if latestVersion != 2 {
		t.Errorf("Expected latest version 2, got %d", latestVersion)
	}

	// Test appending more events
	events2 := []eventstore.Event{
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}
	
	latestVersion, err = store.Append(streamID, events2, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	if latestVersion != 3 {
		t.Errorf("Expected latest version 3, got %d", latestVersion)
	}
}

// TestPostgresEventStore_Integration_AppendUpdatesEventVersions tests that Append updates the event versions for PostgreSQL
func TestPostgresEventStore_Integration_AppendUpdatesEventVersions(t *testing.T) {
	store, db := setupTestStore(t, "test_version_update")
	defer db.Close()

	streamID := "test-version-update-" + time.Now().Format("20060102150405")

	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}
	
	// Verify events don't have versions initially
	for i, event := range events {
		if event.Version != 0 {
			t.Errorf("Event %d should have version 0 initially, got %d", i, event.Version)
		}
	}
	
	latestVersion, err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	if latestVersion != 3 {
		t.Errorf("Expected latest version 3, got %d", latestVersion)
	}
	
	// Verify events now have correct versions
	expectedVersions := []int64{1, 2, 3}
	for i, event := range events {
		if event.Version != expectedVersions[i] {
			t.Errorf("Event %d should have version %d, got %d", i, expectedVersions[i], event.Version)
		}
	}
	
	// Verify events have IDs assigned if they were empty
	for i, event := range events {
		expectedID := fmt.Sprintf("%s-%d", streamID, expectedVersions[i])
		if event.ID != expectedID {
			t.Errorf("Event %d should have ID %s, got %s", i, expectedID, event.ID)
		}
	}
}