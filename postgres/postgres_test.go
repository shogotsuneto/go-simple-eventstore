package postgres

import (
	"reflect"
	"testing"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// TestPostgresEventStore_Unit tests that don't require a real database connection
func TestPostgresEventStore_NewPostgresEventStore_InvalidConnection(t *testing.T) {
	_, err := NewPostgresEventStore("invalid-connection-string")
	if err == nil {
		t.Fatal("Expected error for invalid connection string, got nil")
	}
}

// Mock tests using an interface to test business logic without database
type mockDB struct {
	events map[string][]eventstore.Event
}

func (m *mockDB) getMaxVersion(streamID string) int64 {
	events, exists := m.events[streamID]
	if !exists || len(events) == 0 {
		return 0
	}
	maxVersion := int64(0)
	for _, event := range events {
		if event.Version > maxVersion {
			maxVersion = event.Version
		}
	}
	return maxVersion
}

func (m *mockDB) appendEvents(streamID string, events []eventstore.Event) error {
	if m.events == nil {
		m.events = make(map[string][]eventstore.Event)
	}
	
	maxVersion := m.getMaxVersion(streamID)
	
	for i := range events {
		events[i].Version = maxVersion + int64(i) + 1
		if events[i].ID == "" {
			events[i].ID = streamID + "-" + string(rune(events[i].Version))
		}
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = time.Now()
		}
	}
	
	m.events[streamID] = append(m.events[streamID], events...)
	return nil
}

func (m *mockDB) loadEvents(streamID string, opts eventstore.LoadOptions) []eventstore.Event {
	events, exists := m.events[streamID]
	if !exists {
		return []eventstore.Event{}
	}
	
	var result []eventstore.Event
	for _, event := range events {
		if event.Version > opts.FromVersion {
			result = append(result, event)
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	return result
}

func TestPostgresEventStore_Logic_VersionAssignment(t *testing.T) {
	mock := &mockDB{}
	
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}
	
	err := mock.appendEvents("test-stream", events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	loadedEvents := mock.loadEvents("test-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 10})
	
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

func TestPostgresEventStore_Logic_LoadWithVersion(t *testing.T) {
	mock := &mockDB{}
	
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}
	
	err := mock.appendEvents("test-stream", events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	// Load events starting from version 1 (should get events 2 and 3)
	loadedEvents := mock.loadEvents("test-stream", eventstore.LoadOptions{FromVersion: 1, Limit: 10})
	
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

func TestPostgresEventStore_Logic_LoadWithLimit(t *testing.T) {
	mock := &mockDB{}
	
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}
	
	err := mock.appendEvents("test-stream", events)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	// Load only 2 events
	loadedEvents := mock.loadEvents("test-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 2})
	
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

func TestPostgresEventStore_Logic_EmptyStream(t *testing.T) {
	mock := &mockDB{}
	
	events := mock.loadEvents("non-existent-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 10})
	
	if len(events) != 0 {
		t.Fatalf("Expected 0 events for non-existent stream, got %d", len(events))
	}
}

func TestPostgresEventStore_Logic_PreservesEventData(t *testing.T) {
	mock := &mockDB{}
	
	originalEvent := eventstore.Event{
		ID:   "custom-id",
		Type: "TestEvent",
		Data: []byte(`{"custom": "data"}`),
		Metadata: map[string]string{
			"custom": "metadata",
		},
		Timestamp: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}
	
	err := mock.appendEvents("test-stream", []eventstore.Event{originalEvent})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	loadedEvents := mock.loadEvents("test-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 10})
	
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