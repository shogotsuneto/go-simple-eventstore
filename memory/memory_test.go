package memory

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// Tests for basic InMemoryEventStore functionality

func TestInMemoryEventStore_Append(t *testing.T) {
	store := NewInMemoryEventStore()

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

	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
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
}

func TestInMemoryEventStore_Load_EmptyStream(t *testing.T) {
	store := NewInMemoryEventStore()

	events, err := store.Load("non-existent-stream", eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("Expected 0 events for non-existent stream, got %d", len(events))
	}
}

func TestInMemoryEventStore_Load_WithVersion(t *testing.T) {
	store := NewInMemoryEventStore()

	// Add some events
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}

	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load events starting from version 1 (should get events 2 and 3)
	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{ExclusiveStartVersion: 1, Limit: 10})
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

func TestInMemoryEventStore_Load_WithLimit(t *testing.T) {
	store := NewInMemoryEventStore()

	// Add some events
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}

	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load only 2 events
	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 2})
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

func TestInMemoryEventStore_AppendEmpty(t *testing.T) {
	store := NewInMemoryEventStore()

	_, err := store.Append("test-stream", []eventstore.Event{}, -1)
	if err != nil {
		t.Fatalf("Append empty events failed: %v", err)
	}

	events, err := store.Load("test-stream", eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("Expected 0 events after appending empty slice, got %d", len(events))
	}
}

func TestInMemoryEventStore_PreservesEventData(t *testing.T) {
	store := NewInMemoryEventStore()

	originalEvent := eventstore.Event{
		ID:   "custom-id",
		Type: "TestEvent",
		Data: []byte(`{"custom": "data"}`),
		Metadata: map[string]string{
			"custom": "metadata",
		},
		Timestamp: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	_, err := store.Append("test-stream", []eventstore.Event{originalEvent}, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(loadedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(loadedEvents))
	}

	loadedEvent := loadedEvents[0]

	// Check that custom values were preserved
	if loadedEvent.ID != "custom-id" {
		t.Errorf("Expected ID 'custom-id', got '%s'", loadedEvent.ID)
	}
	if loadedEvent.Type != "TestEvent" {
		t.Errorf("Expected Type 'TestEvent', got '%s'", loadedEvent.Type)
	}
	if !reflect.DeepEqual(loadedEvent.Data, []byte(`{"custom": "data"}`)) {
		t.Errorf("Expected Data to be preserved, got %s", string(loadedEvent.Data))
	}
	if !reflect.DeepEqual(loadedEvent.Metadata, map[string]string{"custom": "metadata"}) {
		t.Errorf("Expected Metadata to be preserved, got %v", loadedEvent.Metadata)
	}
	if !loadedEvent.Timestamp.Equal(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("Expected Timestamp to be preserved, got %v", loadedEvent.Timestamp)
	}
}

func TestInMemoryEventStore_ExpectedVersion_NewStream(t *testing.T) {
	store := NewInMemoryEventStore()

	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": "data"}`)},
	}

	// Should succeed when creating a new stream with expectedVersion 0
	_, err := store.Append("new-stream", events, 0)
	if err != nil {
		t.Fatalf("Expected successful append to new stream with version 0, got error: %v", err)
	}

	// Should fail when trying to create the same stream again with expectedVersion 0
	_, err = store.Append("new-stream", events, 0)
	if err == nil {
		t.Fatal("Expected error when trying to create existing stream with version 0")
	}
}

func TestInMemoryEventStore_ExpectedVersion_ExactMatch(t *testing.T) {
	store := NewInMemoryEventStore()

	// Create initial event
	events1 := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
	}
	_, err := store.Append("test-stream", events1, 0)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Should succeed when appending to version 1
	events2 := []eventstore.Event{
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}
	_, err = store.Append("test-stream", events2, 1)
	if err != nil {
		t.Fatalf("Expected successful append with correct expected version, got error: %v", err)
	}

	// Should fail when trying to append with wrong expected version
	events3 := []eventstore.Event{
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}
	_, err = store.Append("test-stream", events3, 1)
	if err == nil {
		t.Fatal("Expected error when appending with wrong expected version")
	}
}

func TestInMemoryEventStore_ExpectedVersion_NoCheck(t *testing.T) {
	store := NewInMemoryEventStore()

	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": "data"}`)},
	}

	// Should always succeed with expectedVersion -1 (no check)
	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Expected successful append with version -1, got error: %v", err)
	}

	// Should succeed again with expectedVersion -1
	_, err = store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Expected successful append with version -1, got error: %v", err)
	}
}

func TestInMemoryEventStore_ConcurrencyConflictErrors(t *testing.T) {
	store := NewInMemoryEventStore()

	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": "data"}`)},
	}

	// Test ErrStreamAlreadyExists error type
	t.Run("StreamAlreadyExists", func(t *testing.T) {
		streamID := "conflict-stream-1"

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
		streamID := "conflict-stream-2"

		// Create stream first
		_, err := store.Append(streamID, events, 0)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}

		// Try to append with wrong expected version (should expect 1, not 2)
		_, err = store.Append(streamID, events, 2) // Stream is at version 1, expecting 2
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
		streamID := "conflict-stream-3"

		// Create stream first
		_, err := store.Append(streamID, events, 0)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}

		// Try to append with higher expected version
		_, err = store.Append(streamID, events, 5) // Stream is at version 1, expecting 5
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

// Tests for InMemoryEventStore consumer functionality
// Tests for reverse load functionality

func TestInMemoryEventStore_Load_Desc(t *testing.T) {
	store := NewInMemoryEventStore()

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

	// Append events to stream
	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	t.Run("DescLoadAll", func(t *testing.T) {
		// Load all events in descending order
		loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{
			ExclusiveStartVersion: 0,
			Desc:                  true,
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
		loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{
			ExclusiveStartVersion: 0,
			Limit:                 2,
			Desc:                  true,
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
		loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{
			ExclusiveStartVersion: 3,
			Desc:                  true,
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
		loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{
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

// TestInMemoryEventStore_AppendReturnsVersion tests that Append returns the correct latest version
func TestInMemoryEventStore_AppendReturnsVersion(t *testing.T) {
	store := NewInMemoryEventStore()

	// Test appending to empty stream
	events1 := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}

	latestVersion, err := store.Append("test-stream", events1, -1)
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

	latestVersion, err = store.Append("test-stream", events2, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if latestVersion != 3 {
		t.Errorf("Expected latest version 3, got %d", latestVersion)
	}
}

// TestInMemoryEventStore_AppendUpdatesEventVersions tests that Append updates the event versions
func TestInMemoryEventStore_AppendUpdatesEventVersions(t *testing.T) {
	store := NewInMemoryEventStore()

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

	latestVersion, err := store.Append("test-stream", events, -1)
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
		expectedID := fmt.Sprintf("test-stream-%d", expectedVersions[i])
		if event.ID != expectedID {
			t.Errorf("Event %d should have ID %s, got %s", i, expectedID, event.ID)
		}
	}
}

// TestInMemoryEventStore_AppendEmptyReturnsZero tests that appending empty slice always returns zero
func TestInMemoryEventStore_AppendEmptyReturnsZero(t *testing.T) {
	store := NewInMemoryEventStore()

	// Test empty append on empty stream (should return 0)
	latestVersion, err := store.Append("test-stream", []eventstore.Event{}, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if latestVersion != 0 {
		t.Errorf("Expected latest version 0 for empty append on empty stream, got %d", latestVersion)
	}

	// Add some events to the stream
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}

	_, err = store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Test empty append on non-empty stream (should return 0)
	latestVersion, err = store.Append("test-stream", []eventstore.Event{}, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if latestVersion != 0 {
		t.Errorf("Expected latest version 0 for empty append on non-empty stream, got %d", latestVersion)
	}
}
