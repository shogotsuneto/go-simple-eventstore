// +build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/postgres"
)

func setupCursorTestConsumer(t *testing.T) (eventstore.Consumer, eventstore.EventStore, *sql.DB) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable"
	}

	config := postgres.Config{
		ConnectionString:             databaseURL,
		TableName:                    "cursor_test_events",
		UseClientGeneratedTimestamps: false,
	}

	// Connect to database for setup
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Initialize schema
	err = postgres.InitSchema(db, config.TableName, config.UseClientGeneratedTimestamps)
	if err != nil {
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	// Clean up existing data
	_, err = db.Exec("DELETE FROM " + config.TableName)
	if err != nil {
		t.Fatalf("Failed to clean up test data: %v", err)
	}

	consumer, err := postgres.NewPostgresEventConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	store, err := postgres.NewPostgresEventStore(config)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	return consumer, store, db
}

func TestPostgresConsumer_Cursor_BasicFetch(t *testing.T) {
	consumer, store, db := setupCursorTestConsumer(t)
	defer db.Close()

	ctx := context.Background()

	// Add test events
	events1 := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123", "name": "John"}`)},
	}
	events2 := []eventstore.Event{
		{Type: "UserUpdated", Data: []byte(`{"user_id": "123", "name": "Jane"}`)},
	}

	_, err := store.Append("user-123", events1, -1)
	if err != nil {
		t.Fatalf("Failed to append first event: %v", err)
	}

	_, err = store.Append("user-123", events2, -1)
	if err != nil {
		t.Fatalf("Failed to append second event: %v", err)
	}

	// Fetch from beginning
	batch, cursor, err := consumer.Fetch(ctx, nil, 10)
	if err != nil {
		t.Fatalf("Failed to fetch events: %v", err)
	}

	if len(batch) != 2 {
		t.Errorf("Expected 2 events, got %d", len(batch))
	}

	if len(cursor) == 0 {
		t.Error("Expected non-empty cursor")
	}

	// Verify envelope structure
	envelope := batch[0]
	if envelope.Type != "UserCreated" {
		t.Errorf("Expected Type 'UserCreated', got '%s'", envelope.Type)
	}
	if envelope.StreamID != "user-123" {
		t.Errorf("Expected StreamID 'user-123', got '%s'", envelope.StreamID)
	}
	if envelope.Partition == "" {
		t.Error("Expected non-empty partition")
	}
	if envelope.EventID == "" {
		t.Error("Expected non-empty event ID")
	}
}

func TestPostgresConsumer_Cursor_IncrementalFetch(t *testing.T) {
	consumer, store, db := setupCursorTestConsumer(t)
	defer db.Close()

	ctx := context.Background()

	// Add initial events
	events1 := []eventstore.Event{
		{Type: "OrderCreated", Data: []byte(`{"order_id": "456"}`)},
	}
	_, err := store.Append("order-456", events1, -1)
	if err != nil {
		t.Fatalf("Failed to append initial event: %v", err)
	}

	// Fetch first batch
	batch1, cursor1, err := consumer.Fetch(ctx, nil, 10)
	if err != nil {
		t.Fatalf("Failed to fetch first batch: %v", err)
	}

	if len(batch1) != 1 {
		t.Errorf("Expected 1 event in first batch, got %d", len(batch1))
	}

	// Add more events
	events2 := []eventstore.Event{
		{Type: "OrderShipped", Data: []byte(`{"order_id": "456", "tracking": "123ABC"}`)},
	}
	_, err = store.Append("order-456", events2, -1)
	if err != nil {
		t.Fatalf("Failed to append second event: %v", err)
	}

	// Fetch from cursor (should get new events)
	batch2, cursor2, err := consumer.Fetch(ctx, cursor1, 10)
	if err != nil {
		t.Fatalf("Failed to fetch second batch: %v", err)
	}

	if len(batch2) != 1 {
		t.Errorf("Expected 1 new event in second batch, got %d", len(batch2))
	}

	if batch2[0].Type != "OrderShipped" {
		t.Errorf("Expected OrderShipped event, got %s", batch2[0].Type)
	}

	// Fetch again from same cursor should get nothing
	batch3, _, err := consumer.Fetch(ctx, cursor2, 10)
	if err != nil {
		t.Fatalf("Failed to fetch third batch: %v", err)
	}

	if len(batch3) != 0 {
		t.Errorf("Expected 0 events in third batch, got %d", len(batch3))
	}
}

func TestPostgresConsumer_Cursor_Commit(t *testing.T) {
	consumer, store, db := setupCursorTestConsumer(t)
	defer db.Close()

	ctx := context.Background()

	// Add test event
	events := []eventstore.Event{
		{Type: "TestEvent", Data: []byte(`{"test": true}`)},
	}
	_, err := store.Append("test-stream", events, -1)
	if err != nil {
		t.Fatalf("Failed to append event: %v", err)
	}

	// Fetch event
	_, cursor, err := consumer.Fetch(ctx, nil, 1)
	if err != nil {
		t.Fatalf("Failed to fetch event: %v", err)
	}

	// Commit cursor (should not fail)
	err = consumer.Commit(ctx, cursor)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

func TestPostgresConsumer_Cursor_Limit(t *testing.T) {
	consumer, store, db := setupCursorTestConsumer(t)
	defer db.Close()

	ctx := context.Background()

	// Add multiple events
	for i := 0; i < 5; i++ {
		events := []eventstore.Event{
			{Type: "TestEvent", Data: []byte(`{"id": ` + string(rune('0'+i)) + `}`)},
		}
		_, err := store.Append("test-"+string(rune('0'+i)), events, -1)
		if err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}

	// Fetch with limit
	batch, _, err := consumer.Fetch(ctx, nil, 3)
	if err != nil {
		t.Fatalf("Failed to fetch with limit: %v", err)
	}

	if len(batch) != 3 {
		t.Errorf("Expected 3 events with limit, got %d", len(batch))
	}
}