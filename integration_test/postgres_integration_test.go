//go:build integration
// +build integration

package integration_test

import (
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

func setupTestStore(t *testing.T) *postgres.PostgresEventStore {
	store, err := postgres.NewPostgresEventStore(getTestConnectionString())
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL event store: %v", err)
	}
	
	if err := store.InitSchema(); err != nil {
		t.Fatalf("Failed to initialize schema: %v", err)
	}
	
	return store
}

func setupTestStoreWithConfig(t *testing.T, config postgres.Config) *postgres.PostgresEventStore {
	store, err := postgres.NewPostgresEventStoreWithConfig(config)
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL event store with config: %v", err)
	}
	
	if err := store.InitSchema(); err != nil {
		t.Fatalf("Failed to initialize schema: %v", err)
	}
	
	return store
}

func TestPostgresEventStore_Integration_Append(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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
	store := setupTestStore(t)
	defer store.Close()

	streamID := "non-existent-stream-" + time.Now().Format("20060102150405")
	events, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("Expected 0 events for non-existent stream, got %d", len(events))
	}
}

func TestPostgresEventStore_Integration_Load_WithVersion(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 1, Limit: 10})
	if err != nil {
		t.Fatalf("Load with version failed: %v", err)
	}

	if len(loadedEvents) != 2 {
		t.Fatalf("Expected 2 events with fromVersion 1, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Type != "Event2" {
		t.Errorf("Expected first loaded event to be 'Event2', got '%s'", loadedEvents[0].Type)
	}
	if loadedEvents[1].Type != "Event3" {
		t.Errorf("Expected second loaded event to be 'Event3', got '%s'", loadedEvents[1].Type)
	}
}

func TestPostgresEventStore_Integration_Load_WithLimit(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 2})
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
	store := setupTestStore(t)
	defer store.Close()

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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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
	store := setupTestStore(t)
	defer store.Close()

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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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
	store := setupTestStore(t)
	defer store.Close()

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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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
	store := setupTestStore(t)
	defer store.Close()

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
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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
	store := setupTestStore(t)
	defer store.Close()

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
	
	config := postgres.Config{
		ConnectionString: getTestConnectionString(),
		TableName:        customTableName,
	}
	
	store := setupTestStoreWithConfig(t, config)
	defer store.Close()

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

	streamID := "custom-table-test-stream-" + time.Now().Format("20060102150405")
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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

	// Check event data
	if loadedEvents[0].Type != "TestEvent1" {
		t.Errorf("Expected first event type to be 'TestEvent1', got '%s'", loadedEvents[0].Type)
	}
	if loadedEvents[1].Type != "TestEvent2" {
		t.Errorf("Expected second event type to be 'TestEvent2', got '%s'", loadedEvents[1].Type)
	}
}

func TestPostgresEventStore_Integration_CustomTableName_ConcurrencyControl(t *testing.T) {
	customTableName := "concurrency_events_" + time.Now().Format("20060102150405")
	
	config := postgres.Config{
		ConnectionString: getTestConnectionString(),
		TableName:        customTableName,
	}
	
	store := setupTestStoreWithConfig(t, config)
	defer store.Close()

	streamID := "custom-table-concurrency-test-" + time.Now().Format("20060102150405")
	
	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": "data"}`)},
	}

	// Should succeed when creating a new stream with expectedVersion 0
	err := store.Append(streamID, events, 0)
	if err != nil {
		t.Fatalf("Expected successful append to new stream with version 0, got error: %v", err)
	}

	// Should fail when trying to create the same stream again with expectedVersion 0
	err = store.Append(streamID, events, 0)
	if err == nil {
		t.Fatal("Expected error when trying to create existing stream with version 0")
	}

	// Verify it's the correct error type
	var streamExistsErr *eventstore.ErrStreamAlreadyExists
	if !errors.As(err, &streamExistsErr) {
		t.Fatalf("Expected ErrStreamAlreadyExists, got %T: %v", err, err)
	}
}

func TestPostgresEventStore_Integration_DefaultTableName_BackwardCompatibility(t *testing.T) {
	// Test that the old constructor still works with default table name
	store := setupTestStore(t)
	defer store.Close()

	events := []eventstore.Event{
		{
			Type: "CompatibilityTest",
			Data: []byte(`{"test": "backward_compatibility"}`),
		},
	}

	streamID := "backward-compatibility-test-" + time.Now().Format("20060102150405")
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed with old constructor: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed with old constructor: %v", err)
	}

	if len(loadedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Type != "CompatibilityTest" {
		t.Errorf("Expected event type 'CompatibilityTest', got '%s'", loadedEvents[0].Type)
	}
}

func TestPostgresEventStore_Integration_EmptyTableName_UsesDefault(t *testing.T) {
	// Test that empty table name uses default "events"
	config := postgres.Config{
		ConnectionString: getTestConnectionString(),
		TableName:        "", // Empty table name should use default
	}
	
	store := setupTestStoreWithConfig(t, config)
	defer store.Close()

	events := []eventstore.Event{
		{
			Type: "EmptyTableNameTest",
			Data: []byte(`{"test": "empty_table_name"}`),
		},
	}

	streamID := "empty-table-name-test-" + time.Now().Format("20060102150405")
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Append failed with empty table name: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed with empty table name: %v", err)
	}

	if len(loadedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(loadedEvents))
	}

	if loadedEvents[0].Type != "EmptyTableNameTest" {
		t.Errorf("Expected event type 'EmptyTableNameTest', got '%s'", loadedEvents[0].Type)
	}
}