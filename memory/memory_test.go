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

	err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify events were stored
	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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

	events, err := store.Load("non-existent-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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

	err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load events starting from version 1 (should get events 2 and 3)
	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{FromVersion: 1, Limit: 10})
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
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"test": "data1"}`)},
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}

	err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load only 2 events
	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 2})
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

	err := store.Append("test-stream", []eventstore.Event{}, -1)
	if err != nil {
		t.Fatalf("Append empty events failed: %v", err)
	}

	events, err := store.Load("test-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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

	err := store.Append("test-stream", []eventstore.Event{originalEvent}, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	loadedEvents, err := store.Load("test-stream", eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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
	err := store.Append("new-stream", events, 0)
	if err != nil {
		t.Fatalf("Expected successful append to new stream with version 0, got error: %v", err)
	}

	// Should fail when trying to create the same stream again with expectedVersion 0
	err = store.Append("new-stream", events, 0)
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
	err := store.Append("test-stream", events1, 0)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Should succeed when appending to version 1
	events2 := []eventstore.Event{
		{Type: "Event2", Data: []byte(`{"test": "data2"}`)},
	}
	err = store.Append("test-stream", events2, 1)
	if err != nil {
		t.Fatalf("Expected successful append with correct expected version, got error: %v", err)
	}

	// Should fail when trying to append with wrong expected version
	events3 := []eventstore.Event{
		{Type: "Event3", Data: []byte(`{"test": "data3"}`)},
	}
	err = store.Append("test-stream", events3, 1)
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
	err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Expected successful append with version -1, got error: %v", err)
	}

	// Should succeed again with expectedVersion -1
	err = store.Append("test-stream", events, -1)
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
		streamID := "conflict-stream-2"

		// Create stream first
		err := store.Append(streamID, events, 0)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}

		// Try to append with wrong expected version (should expect 1, not 2)
		err = store.Append(streamID, events, 2) // Stream is at version 1, expecting 2
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
		err := store.Append(streamID, events, 0)
		if err != nil {
			t.Fatalf("Failed to create stream: %v", err)
		}

		// Try to append with higher expected version
		err = store.Append(streamID, events, 5) // Stream is at version 1, expecting 5
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

// Tests for InMemoryEventConsumer functionality

func TestInMemoryEventConsumer_Poll(t *testing.T) {
	store := NewInMemoryEventConsumer()

	// Add some events to the store
	events := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123"}`)},
		{Type: "UserUpdated", Data: []byte(`{"user_id": "123", "name": "John"}`)},
	}

	err := store.Append("user-123", events, -1)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Test polling from version 0
	polledEvents, err := store.Poll("user-123", eventstore.ConsumeOptions{
		FromVersion: 0,
		BatchSize:   10,
	})
	if err != nil {
		t.Fatalf("Failed to poll events: %v", err)
	}

	if len(polledEvents) != 2 {
		t.Errorf("Expected 2 events, got %d", len(polledEvents))
	}

	// Test polling from version 1 (should get only the second event)
	polledEvents, err = store.Poll("user-123", eventstore.ConsumeOptions{
		FromVersion: 1,
		BatchSize:   10,
	})
	if err != nil {
		t.Fatalf("Failed to poll events: %v", err)
	}

	if len(polledEvents) != 1 {
		t.Errorf("Expected 1 event, got %d", len(polledEvents))
	}

	if polledEvents[0].Type != "UserUpdated" {
		t.Errorf("Expected UserUpdated event, got %s", polledEvents[0].Type)
	}
}

func TestInMemoryEventConsumer_Subscribe(t *testing.T) {
	store := NewInMemoryEventConsumer()

	// Subscribe to a stream
	subscription, err := store.Subscribe("user-123", eventstore.ConsumeOptions{
		FromVersion: 0,
		BatchSize:   10,
	})
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}
	defer subscription.Close()

	// Add some events
	events := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123"}`)},
		{Type: "UserUpdated", Data: []byte(`{"user_id": "123", "name": "John"}`)},
	}

	err = store.Append("user-123", events, -1)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Read events from subscription
	receivedEvents := make([]eventstore.Event, 0)
	timeout := time.After(2 * time.Second)

	for len(receivedEvents) < 2 {
		select {
		case event := <-subscription.Events():
			receivedEvents = append(receivedEvents, event)
		case err := <-subscription.Errors():
			t.Fatalf("Subscription error: %v", err)
		case <-timeout:
			t.Fatalf("Timeout waiting for events. Received %d events", len(receivedEvents))
		}
	}

	if len(receivedEvents) != 2 {
		t.Errorf("Expected 2 events, got %d", len(receivedEvents))
	}

	if receivedEvents[0].Type != "UserCreated" {
		t.Errorf("Expected first event to be UserCreated, got %s", receivedEvents[0].Type)
	}

	if receivedEvents[1].Type != "UserUpdated" {
		t.Errorf("Expected second event to be UserUpdated, got %s", receivedEvents[1].Type)
	}
}

func TestInMemoryEventConsumer_Subscribe_WithFromVersion(t *testing.T) {
	store := NewInMemoryEventConsumer()

	// Add some initial events
	initialEvents := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123"}`)},
		{Type: "UserUpdated", Data: []byte(`{"user_id": "123", "name": "John"}`)},
	}

	err := store.Append("user-123", initialEvents, -1)
	if err != nil {
		t.Fatalf("Failed to append initial events: %v", err)
	}

	// Subscribe from version 1 (should only get events after version 1)
	subscription, err := store.Subscribe("user-123", eventstore.ConsumeOptions{
		FromVersion: 1,
		BatchSize:   10,
	})
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}
	defer subscription.Close()

	// Should immediately receive the second event (version 2)
	timeout := time.After(1 * time.Second)
	select {
	case event := <-subscription.Events():
		if event.Type != "UserUpdated" {
			t.Errorf("Expected UserUpdated event, got %s", event.Type)
		}
		if event.Version != 2 {
			t.Errorf("Expected version 2, got %d", event.Version)
		}
	case err := <-subscription.Errors():
		t.Fatalf("Subscription error: %v", err)
	case <-timeout:
		t.Fatalf("Timeout waiting for initial event")
	}

	// Add another event
	newEvents := []eventstore.Event{
		{Type: "UserDeleted", Data: []byte(`{"user_id": "123"}`)},
	}

	err = store.Append("user-123", newEvents, -1)
	if err != nil {
		t.Fatalf("Failed to append new events: %v", err)
	}

	// Should receive the new event
	timeout = time.After(1 * time.Second)
	select {
	case event := <-subscription.Events():
		if event.Type != "UserDeleted" {
			t.Errorf("Expected UserDeleted event, got %s", event.Type)
		}
	case err := <-subscription.Errors():
		t.Fatalf("Subscription error: %v", err)
	case <-timeout:
		t.Fatalf("Timeout waiting for new event")
	}
}

func TestInMemoryEventConsumer_Subscribe_Close(t *testing.T) {
	store := NewInMemoryEventConsumer()

	subscription, err := store.Subscribe("user-123", eventstore.ConsumeOptions{
		FromVersion: 0,
		BatchSize:   10,
	})
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Close the subscription
	err = subscription.Close()
	if err != nil {
		t.Errorf("Failed to close subscription: %v", err)
	}

	// Closing again should not error
	err = subscription.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}

	// Verify subscription is removed from store
	store.subsMu.RLock()
	subs := store.subscriptions["user-123"]
	store.subsMu.RUnlock()

	if len(subs) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(subs))
	}
}