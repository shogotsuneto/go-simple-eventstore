//go:build integration
// +build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/postgres"
)

// setupTestConsumerForOffset creates a PostgreSQL event consumer for offset testing
func setupTestConsumerForOffset(t *testing.T, tableName string) (eventstore.EventConsumer, *sql.DB) {
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

	consumer, err := postgres.NewPostgresEventConsumer(db, tableName, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL event consumer: %v", err)
	}
	return consumer, db
}

// TestPostgresOffset_BasicOffsetSupport tests that PostgreSQL events have proper offset values
func TestPostgresOffset_BasicOffsetSupport(t *testing.T) {
	tableName := fmt.Sprintf("test_offset_events_%d", time.Now().UnixNano())
	store, db := setupTestStore(t, tableName)
	defer db.Close()

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

	// Verify offsets are assigned properly (PostgreSQL SERIAL starts at 1)
	allEvents := append(stream1Events, stream2Events...)

	if len(allEvents) != 3 {
		t.Fatalf("Expected 3 events total, got %d", len(allEvents))
	}

	// Check that offsets are sequential and unique across streams
	// Note: Order might not be guaranteed, so we just check that offsets are in range 1-3
	offsetsSeen := make(map[int64]bool)
	for _, event := range allEvents {
		if event.Offset < 1 || event.Offset > 3 {
			t.Errorf("Event offset %d is out of expected range 1-3", event.Offset)
		}
		
		if offsetsSeen[event.Offset] {
			t.Errorf("Duplicate offset %d found", event.Offset)
		}
		offsetsSeen[event.Offset] = true
	}
}

// TestPostgresOffset_RetrieveByOffset tests offset-based retrieval
func TestPostgresOffset_RetrieveByOffset(t *testing.T) {
	tableName := fmt.Sprintf("test_retrieve_offset_%d", time.Now().UnixNano())
	consumer, db := setupTestConsumerForOffset(t, tableName)
	defer db.Close()

	// Also need a store to add events
	store, err := postgres.NewPostgresEventStore(db, tableName)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"msg": "first"}`)},
		{Type: "Event2", Data: []byte(`{"msg": "second"}`)},
		{Type: "Event3", Data: []byte(`{"msg": "third"}`)},
	}

	// Append events to different streams
	err = store.Append("stream-1", events[:2], -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	err = store.Append("stream-2", events[2:], -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Wait a bit for events to be committed
	time.Sleep(50 * time.Millisecond)

	// Test retrieve with offset (should get events after offset 1)
	retrievedEvents, err := consumer.Retrieve(eventstore.ConsumeOptions{
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

	// Events should be ordered by offset
	if retrievedEvents[0].Offset <= 1 {
		t.Errorf("First retrieved event offset should be > 1, got %d", retrievedEvents[0].Offset)
	}

	if retrievedEvents[1].Offset <= retrievedEvents[0].Offset {
		t.Errorf("Second event offset (%d) should be > first event offset (%d)", 
			retrievedEvents[1].Offset, retrievedEvents[0].Offset)
	}

	// Test retrieve from offset 0 (should get all events)
	allEvents, err := consumer.Retrieve(eventstore.ConsumeOptions{
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

// TestPostgresOffset_SubscribeByOffset tests offset-based subscription
func TestPostgresOffset_SubscribeByOffset(t *testing.T) {
	tableName := fmt.Sprintf("test_subscribe_offset_%d", time.Now().UnixNano())
	consumer, db := setupTestConsumerForOffset(t, tableName)
	defer db.Close()

	// Also need a store to add events
	store, err := postgres.NewPostgresEventStore(db, tableName)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// First, add some initial events
	initialEvents := []eventstore.Event{
		{Type: "InitialEvent1", Data: []byte(`{"msg": "init1"}`)},
		{Type: "InitialEvent2", Data: []byte(`{"msg": "init2"}`)},
	}

	err = store.Append("stream-1", initialEvents, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Subscribe starting from offset 1 (should get event with offset >= 2)
	sub, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromOffset: 1,
		BatchSize:  5,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer sub.Close()

	// Collect initial event (should be event with offset 2)
	select {
	case event := <-sub.Events():
		if event.Offset <= 1 {
			t.Errorf("Expected initial event offset > 1, got %d", event.Offset)
		}
		t.Logf("Received initial event with offset %d", event.Offset)
	case err := <-sub.Errors():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for initial event")
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
	select {
	case event := <-sub.Events():
		if event.Type != "NewEvent" {
			t.Errorf("Expected event type 'NewEvent', got %q", event.Type)
		}
		if event.Offset <= 2 {
			t.Errorf("Expected new event offset > 2, got %d", event.Offset)
		}
		t.Logf("Received new event with offset %d", event.Offset)
	case err := <-sub.Errors():
		t.Fatalf("Subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for new event")
	}
}

// TestPostgresOffset_BackwardCompatibility ensures timestamp-based consumption still works
func TestPostgresOffset_BackwardCompatibility(t *testing.T) {
	tableName := fmt.Sprintf("test_compat_%d", time.Now().UnixNano())
	consumer, db := setupTestConsumerForOffset(t, tableName)
	defer db.Close()

	// Also need a store to add events
	store, err := postgres.NewPostgresEventStore(db, tableName)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Record time before adding events
	beforeTime := time.Now()
	time.Sleep(10 * time.Millisecond)

	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"msg": "first"}`)},
		{Type: "Event2", Data: []byte(`{"msg": "second"}`)},
	}

	err = store.Append("stream-1", events, -1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Wait a bit for events to be committed
	time.Sleep(50 * time.Millisecond)

	// Test retrieve with timestamp (legacy behavior)
	retrievedEvents, err := consumer.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: beforeTime,
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if len(retrievedEvents) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(retrievedEvents))
	}

	// Test subscription with timestamp
	sub, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: beforeTime,
		BatchSize:     5,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer sub.Close()

	// Should receive existing events
	for i := 0; i < 2; i++ {
		select {
		case event := <-sub.Events():
			t.Logf("Received event via timestamp subscription: %s", event.Type)
		case err := <-sub.Errors():
			t.Fatalf("Subscription error: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for timestamp-based event")
		}
	}
}