package eventstore

import (
	"reflect"
	"testing"
	"time"
)

func TestInMemoryEventStore_Append(t *testing.T) {
	store := NewInMemoryEventStore()

	events := []Event{
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

	err := store.Append("test-stream", events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load("test-stream", LoadOptions{FromVersion: 0, Limit: 10})
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

	events, err := store.Load("non-existent-stream", LoadOptions{FromVersion: 0, Limit: 10})
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
	events := []Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}

	err := store.Append("test-stream", events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load events starting from version 1 (should get events 2 and 3)
	loadedEvents, err := store.Load("test-stream", LoadOptions{FromVersion: 1, Limit: 10})
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

func TestInMemoryEventStore_Load_WithLimit(t *testing.T) {
	store := NewInMemoryEventStore()

	// Add some events
	events := []Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}

	err := store.Append("test-stream", events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load only 2 events
	loadedEvents, err := store.Load("test-stream", LoadOptions{FromVersion: 0, Limit: 2})
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

	err := store.Append("test-stream", []Event{})
	if err != nil {
		t.Fatalf("Append empty events failed: %v", err)
	}

	events, err := store.Load("test-stream", LoadOptions{FromVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("Expected 0 events after appending empty slice, got %d", len(events))
	}
}

func TestInMemoryEventStore_PreservesEventData(t *testing.T) {
	store := NewInMemoryEventStore()

	originalEvent := Event{
		ID:   "custom-id",
		Type: "TestEvent",
		Data: []byte(`{"custom": "data"}`),
		Metadata: map[string]string{
			"custom": "metadata",
		},
		Timestamp: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	err := store.Append("test-stream", []Event{originalEvent})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	loadedEvents, err := store.Load("test-stream", LoadOptions{FromVersion: 0, Limit: 10})
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
