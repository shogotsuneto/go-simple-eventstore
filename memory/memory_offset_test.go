package memory

import (
	"testing"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// TestInMemoryEventStore_OffsetSupport tests that events have proper offset values
func TestInMemoryEventStore_OffsetSupport(t *testing.T) {
	store := NewInMemoryEventStore()

	events1 := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"msg": "first"}`), Metadata: map[string]string{"source": "test"}},
		{Type: "Event2", Data: []byte(`{"msg": "second"}`), Metadata: map[string]string{"source": "test"}},
	}

	events2 := []eventstore.Event{
		{Type: "Event3", Data: []byte(`{"msg": "third"}`), Metadata: map[string]string{"source": "test"}},
	}

	// Append first batch to stream1
	err := store.Append("stream-1", events1, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Append second batch to stream2
	err = store.Append("stream-2", events2, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Load all events from both streams to check offsets
	stream1Events, err := store.Load("stream-1", eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	stream2Events, err := store.Load("stream-2", eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify offsets are assigned properly
	expectedOffsets := []int64{1, 2, 3}
	allEvents := append(stream1Events, stream2Events...)

	if len(allEvents) != 3 {
		t.Fatalf("Expected 3 events total, got %d", len(allEvents))
	}

	// Check that offsets are sequential and unique across streams
	for i, event := range allEvents {
		if event.Offset != expectedOffsets[i] {
			t.Errorf("Event %d: expected offset %d, got %d", i, expectedOffsets[i], event.Offset)
		}
	}
}

// TestInMemoryEventStore_RetrieveByOffset tests offset-based retrieval
func TestInMemoryEventStore_RetrieveByOffset(t *testing.T) {
	store := NewInMemoryEventStore()

	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"msg": "first"}`)},
		{Type: "Event2", Data: []byte(`{"msg": "second"}`)},
		{Type: "Event3", Data: []byte(`{"msg": "third"}`)},
	}

	// Append events to different streams
	err := store.Append("stream-1", events[:2], -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	err = store.Append("stream-2", events[2:], -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Test retrieve with offset (should get events after offset 1)
	retrievedEvents, err := store.Retrieve(eventstore.ConsumeOptions{
		FromOffset: 1,
		BatchSize:  10,
	})
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	// Should get events with offset 2 and 3
	if len(retrievedEvents) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(retrievedEvents))
	}

	if retrievedEvents[0].Offset != 2 {
		t.Errorf("First retrieved event: expected offset 2, got %d", retrievedEvents[0].Offset)
	}

	if retrievedEvents[1].Offset != 3 {
		t.Errorf("Second retrieved event: expected offset 3, got %d", retrievedEvents[1].Offset)
	}

	// Test retrieve from offset 0 (should get all events)
	allEvents, err := store.Retrieve(eventstore.ConsumeOptions{
		FromOffset: 0,
		BatchSize:  10,
	})
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if len(allEvents) != 3 {
		t.Fatalf("Expected 3 events, got %d", len(allEvents))
	}
}

// TestInMemoryEventStore_SubscribeByOffset tests offset-based subscription
func TestInMemoryEventStore_SubscribeByOffset(t *testing.T) {
	store := NewInMemoryEventStore()

	// First, add some initial events
	initialEvents := []eventstore.Event{
		{Type: "InitialEvent1", Data: []byte(`{"msg": "init1"}`)},
		{Type: "InitialEvent2", Data: []byte(`{"msg": "init2"}`)},
	}

	err := store.Append("stream-1", initialEvents, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Subscribe starting from offset 1 (should get event with offset 2)
	sub, err := store.Subscribe(eventstore.ConsumeOptions{
		FromOffset: 1,
		BatchSize:  5,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer sub.Close()

	// Collect initial event (should be event with offset 2)
	event := <-sub.Events()
	if event.Offset != 2 {
		t.Errorf("Expected initial event offset 2, got %d", event.Offset)
	}

	// Add a new event
	newEvents := []eventstore.Event{
		{Type: "NewEvent", Data: []byte(`{"msg": "new"}`)},
	}

	err = store.Append("stream-2", newEvents, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Should receive the new event
	event = <-sub.Events()
	if event.Offset != 3 {
		t.Errorf("Expected new event offset 3, got %d", event.Offset)
	}

	if event.Type != "NewEvent" {
		t.Errorf("Expected event type 'NewEvent', got %q", event.Type)
	}
}