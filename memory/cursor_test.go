package memory

import (
	"context"
	"testing"

	"github.com/shogotsuneto/go-simple-eventstore"
)

// Tests for cursor-based consumer functionality

func TestInMemoryEventStore_Fetch_EmptyCursor(t *testing.T) {
	store := NewInMemoryEventStore()
	ctx := context.Background()

	// Fetch from empty store with nil cursor
	batch, cursor, err := store.Fetch(ctx, nil, 10)
	if err != nil {
		t.Fatalf("Failed to fetch from empty store: %v", err)
	}

	if len(batch) != 0 {
		t.Errorf("Expected 0 events from empty store, got %d", len(batch))
	}

	if len(cursor) == 0 {
		t.Error("Expected non-empty cursor even for empty results")
	}
}

func TestInMemoryEventStore_Fetch_WithEvents(t *testing.T) {
	store := NewInMemoryEventStore()
	ctx := context.Background()

	// Add some events to different streams
	events1 := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123"}`)},
	}
	events2 := []eventstore.Event{
		{Type: "OrderCreated", Data: []byte(`{"order_id": "456"}`)},
	}

	_, err := store.Append("user-123", events1, -1)
	if err != nil {
		t.Fatalf("Failed to append events to user-123: %v", err)
	}

	_, err = store.Append("order-456", events2, -1)
	if err != nil {
		t.Fatalf("Failed to append events to order-456: %v", err)
	}

	// Fetch all events
	batch, cursor, err := store.Fetch(ctx, nil, 10)
	if err != nil {
		t.Fatalf("Failed to fetch events: %v", err)
	}

	if len(batch) != 2 {
		t.Errorf("Expected 2 events, got %d", len(batch))
	}

	// Verify envelope structure
	envelope := batch[0]
	if envelope.Type != "UserCreated" {
		t.Errorf("Expected Type 'UserCreated', got '%s'", envelope.Type)
	}
	if envelope.StreamID != "user-123" {
		t.Errorf("Expected StreamID 'user-123', got '%s'", envelope.StreamID)
	}
	if envelope.Partition != "global" {
		t.Errorf("Expected Partition 'global', got '%s'", envelope.Partition)
	}
	if envelope.EventID != "user-123-1" {
		t.Errorf("Expected EventID 'user-123-1', got '%s'", envelope.EventID)
	}

	// Verify cursor is not empty
	if len(cursor) == 0 {
		t.Error("Expected non-empty cursor")
	}
}

func TestInMemoryEventStore_Fetch_WithCursor(t *testing.T) {
	store := NewInMemoryEventStore()
	ctx := context.Background()

	// Add initial events
	events1 := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123"}`)},
	}
	events2 := []eventstore.Event{
		{Type: "OrderCreated", Data: []byte(`{"order_id": "456"}`)},
	}

	_, err := store.Append("user-123", events1, -1)
	if err != nil {
		t.Fatalf("Failed to append events to user-123: %v", err)
	}
	_, err = store.Append("order-456", events2, -1)
	if err != nil {
		t.Fatalf("Failed to append events to order-456: %v", err)
	}

	// Fetch first batch
	batch1, cursor1, err := store.Fetch(ctx, nil, 1)
	if err != nil {
		t.Fatalf("Failed to fetch first batch: %v", err)
	}

	if len(batch1) != 1 {
		t.Errorf("Expected 1 event in first batch, got %d", len(batch1))
	}

	// Fetch next batch from cursor
	batch2, cursor2, err := store.Fetch(ctx, cursor1, 1)
	if err != nil {
		t.Fatalf("Failed to fetch second batch: %v", err)
	}

	if len(batch2) != 1 {
		t.Errorf("Expected 1 event in second batch, got %d", len(batch2))
	}

	// Ensure we got different events
	if batch1[0].EventID == batch2[0].EventID {
		t.Error("Expected different events in batches")
	}

	// Add more events after cursor2
	events3 := []eventstore.Event{
		{Type: "ProductCreated", Data: []byte(`{"product_id": "789"}`)},
	}
	_, err = store.Append("product-789", events3, -1)
	if err != nil {
		t.Fatalf("Failed to append events to product-789: %v", err)
	}

	// Fetch from cursor2 should get the new event
	batch3, _, err := store.Fetch(ctx, cursor2, 10)
	if err != nil {
		t.Fatalf("Failed to fetch third batch: %v", err)
	}

	if len(batch3) != 1 {
		t.Errorf("Expected 1 new event, got %d", len(batch3))
	}
	if batch3[0].Type != "ProductCreated" {
		t.Errorf("Expected ProductCreated event, got %s", batch3[0].Type)
	}
}

func TestInMemoryEventStore_Fetch_Limit(t *testing.T) {
	store := NewInMemoryEventStore()
	ctx := context.Background()

	// Add 5 events
	for i := 0; i < 5; i++ {
		events := []eventstore.Event{
			{Type: "TestEvent", Data: []byte(`{"id": ` + string(rune('0'+i)) + `}`)},
		}
		_, err := store.Append("test-"+string(rune('0'+i)), events, -1)
		if err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}

	// Fetch with limit 3
	batch, _, err := store.Fetch(ctx, nil, 3)
	if err != nil {
		t.Fatalf("Failed to fetch with limit: %v", err)
	}

	if len(batch) != 3 {
		t.Errorf("Expected 3 events with limit 3, got %d", len(batch))
	}
}

func TestInMemoryEventStore_Commit(t *testing.T) {
	store := NewInMemoryEventStore()
	ctx := context.Background()

	// Add an event
	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": true}`)},
	}
	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Failed to append event: %v", err)
	}

	// Get cursor
	_, cursor, err := store.Fetch(ctx, nil, 1)
	if err != nil {
		t.Fatalf("Failed to fetch event: %v", err)
	}

	// Commit should not fail (no-op for memory implementation)
	err = store.Commit(ctx, cursor)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

func TestInMemoryEventStore_Fetch_InvalidCursor(t *testing.T) {
	store := NewInMemoryEventStore()
	ctx := context.Background()

	// Add an event
	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": true}`)},
	}
	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Failed to append event: %v", err)
	}

	// Try with invalid cursor (wrong size)
	invalidCursor := []byte{1, 2, 3} // Not 8 bytes
	batch, cursor, err := store.Fetch(ctx, invalidCursor, 10)
	if err != nil {
		t.Fatalf("Failed to fetch with invalid cursor: %v", err)
	}

	// Should treat invalid cursor as start from beginning
	if len(batch) != 1 {
		t.Errorf("Expected 1 event with invalid cursor, got %d", len(batch))
	}

	if len(cursor) == 0 {
		t.Error("Expected non-empty cursor")
	}
}
