//go:build integration
// +build integration

package integration_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
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
	db, err := sql.Open("postgres", getTestConnectionString())
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	if err := postgres.InitSchema(db, tableName); err != nil {
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	store, err := postgres.NewPostgresEventStore(db, tableName)
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
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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
	events, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load events starting from version 1 (should get events 2 and 3)
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 1, Limit: 10})
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
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load only 2 events
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 2})
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
			done <- store.Append(streamID, events, -1)
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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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
	err := store.Append(streamID, events, 0)
	if err != nil {
		t.Fatalf("Expected successful append to new stream with version 0, got error: %v", err)
	}

	// Verify the event was stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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
	err = store.Append(streamID, events, 0)
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
	err := store.Append(streamID, events1, 0)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Should succeed when appending to version 1
	events2 := []eventstore.Event{
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}
	err = store.Append(streamID, events2, 1)
	if err != nil {
		t.Fatalf("Expected successful append with correct expected version, got error: %v", err)
	}

	// Verify both events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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
	err = store.Append(streamID, events3, 1)
	if err == nil {
		t.Fatal("Expected error when appending with wrong expected version")
	}

	// Should also fail when trying to append with a higher expected version
	err = store.Append(streamID, events3, 5)
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
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Expected successful append with version -1, got error: %v", err)
	}

	// Should succeed again with expectedVersion -1
	err = store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Expected successful append with version -1, got error: %v", err)
	}

	// Verify both events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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
		err := store.Append(streamID, events, 0)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}

		// Try to create the same stream again with expectedVersion 0
		err = store.Append(streamID, events, 0)
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
		err := store.Append(streamID, events, 2) // Stream is at version 1, expecting 2
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
		err := store.Append(streamID, events, 5) // Stream is at version 1, expecting 5
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
	err := store.Append(streamID, events, -1)
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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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

func TestPostgresEventStore_Load_Reverse(t *testing.T) {
	tableName := "test_events_reverse"
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

	streamID := "test-stream-reverse"
	
	// Append events to stream
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	t.Run("ReverseLoadAll", func(t *testing.T) {
		// Load all events in reverse order
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			AfterVersion: 0,
			Reverse:      true,
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loadedEvents) != 4 {
			t.Fatalf("Expected 4 events, got %d", len(loadedEvents))
		}

		// Verify events are in reverse order (latest first)
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

	t.Run("ReverseLoadWithLimit", func(t *testing.T) {
		// Load latest 2 events in reverse order
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			AfterVersion: 0,
			Limit:        2,
			Reverse:      true,
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

	t.Run("ReverseLoadWithAfterVersion", func(t *testing.T) {
		// Load events after version 2 in reverse order (should get Event4, Event3)
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			AfterVersion: 2,
			Reverse:      true,
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loadedEvents) != 2 {
			t.Fatalf("Expected 2 events, got %d", len(loadedEvents))
		}

		// Should get Event4, Event3 (versions 4, 3)
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

	t.Run("ForwardLoadStillWorks", func(t *testing.T) {
		// Verify that forward loading (original behavior) still works
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			AfterVersion: 0,
			Reverse:      false, // Explicitly set to false
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if len(loadedEvents) != 4 {
			t.Fatalf("Expected 4 events, got %d", len(loadedEvents))
		}

		// Verify events are in forward order (oldest first)
		expectedTypes := []string{"Event1", "Event2", "Event3", "Event4"}
		expectedVersions := []int64{1, 2, 3, 4}

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
		// Verify that default behavior (Reverse field not set) is forward loading
		loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{
			AfterVersion: 0,
			// Reverse field omitted, should default to false
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