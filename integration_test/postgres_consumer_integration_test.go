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

func setupTestConsumer(t *testing.T) (eventstore.EventConsumer, eventstore.EventStore, *sql.DB) {
	tableName := fmt.Sprintf("consumer_events_%d", time.Now().UnixNano())
	return setupTestConsumerWithTableName(t, tableName, 1*time.Second)
}

func setupTestConsumerWithTableName(t *testing.T, tableName string, pollingInterval time.Duration) (eventstore.EventConsumer, eventstore.EventStore, *sql.DB) {
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

	store := postgres.NewPostgresEventStore(db, tableName)
	consumer := postgres.NewPostgresEventConsumer(db, tableName, pollingInterval)
	return consumer, store, db
}

func TestPostgresEventConsumer_Integration_Retrieve_EmptyTable(t *testing.T) {
	consumer, _, db := setupTestConsumer(t)
	defer db.Close()

	events, err := consumer.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: time.Now().Add(-1 * time.Hour),
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("Expected 0 events from empty table, got %d", len(events))
	}
}

func TestPostgresEventConsumer_Integration_Retrieve_WithTimestamp(t *testing.T) {
	consumer, store, db := setupTestConsumer(t)
	defer db.Close()

	// Record a timestamp before adding events
	beforeEvents := time.Now()
	time.Sleep(10 * time.Millisecond) // Small delay to ensure timestamp difference

	// Add events to different streams
	events1 := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123", "name": "John"}`)},
		{Type: "UserUpdated", Data: []byte(`{"user_id": "123", "name": "John Doe"}`)},
	}
	events2 := []eventstore.Event{
		{Type: "OrderCreated", Data: []byte(`{"order_id": "456", "user_id": "123"}`)},
	}

	streamID1 := "user-123-" + time.Now().Format("20060102150405")
	streamID2 := "order-456-" + time.Now().Format("20060102150405")

	err := store.Append(streamID1, events1, -1)
	if err != nil {
		t.Fatalf("Failed to append events to stream1: %v", err)
	}

	err = store.Append(streamID2, events2, -1)
	if err != nil {
		t.Fatalf("Failed to append events to stream2: %v", err)
	}

	time.Sleep(10 * time.Millisecond) // Small delay to ensure timestamp difference
	afterEvents := time.Now()

	// Retrieve events from before any were added (should get all)
	allEvents, err := consumer.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: beforeEvents,
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Retrieve all events failed: %v", err)
	}

	if len(allEvents) != 3 {
		t.Fatalf("Expected 3 events from all streams, got %d", len(allEvents))
	}

	// Verify events are ordered by timestamp
	for i := 1; i < len(allEvents); i++ {
		if allEvents[i].Timestamp.Before(allEvents[i-1].Timestamp) {
			t.Errorf("Events not ordered by timestamp: event %d (%v) before event %d (%v)",
				i, allEvents[i].Timestamp, i-1, allEvents[i-1].Timestamp)
		}
	}

	// Retrieve events from after all were added (should get none)
	noEvents, err := consumer.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: afterEvents,
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Retrieve future events failed: %v", err)
	}

	if len(noEvents) != 0 {
		t.Fatalf("Expected 0 events from future timestamp, got %d", len(noEvents))
	}
}

func TestPostgresEventConsumer_Integration_Retrieve_WithBatchSize(t *testing.T) {
	consumer, store, db := setupTestConsumer(t)
	defer db.Close()

	// Add multiple events to test batch size limit
	events := []eventstore.Event{
		{Type: "Event1", Data: []byte(`{"id": 1}`)},
		{Type: "Event2", Data: []byte(`{"id": 2}`)},
		{Type: "Event3", Data: []byte(`{"id": 3}`)},
		{Type: "Event4", Data: []byte(`{"id": 4}`)},
		{Type: "Event5", Data: []byte(`{"id": 5}`)},
	}

	streamID := "batch-test-stream-" + time.Now().Format("20060102150405")
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	// Retrieve with batch size of 3
	retrievedEvents, err := consumer.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: time.Now().Add(-1 * time.Hour),
		BatchSize:     3,
	})
	if err != nil {
		t.Fatalf("Retrieve with batch size failed: %v", err)
	}

	if len(retrievedEvents) != 3 {
		t.Fatalf("Expected 3 events due to batch size limit, got %d", len(retrievedEvents))
	}

	// Verify the first 3 events were returned
	expectedTypes := []string{"Event1", "Event2", "Event3"}
	for i, event := range retrievedEvents {
		if event.Type != expectedTypes[i] {
			t.Errorf("Expected event type %s at index %d, got %s", expectedTypes[i], i, event.Type)
		}
	}
}

func TestPostgresEventConsumer_Integration_Subscribe_RealtimeEvents(t *testing.T) {
	consumer, store, db := setupTestConsumerWithTableName(t,
		fmt.Sprintf("subscribe_events_%d", time.Now().UnixNano()),
		100*time.Millisecond) // Fast polling for testing
	defer db.Close()

	// Subscribe from current time
	subscription, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: time.Now(),
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer subscription.Close()

	// Add events after subscription is created
	go func() {
		time.Sleep(50 * time.Millisecond) // Small delay to ensure subscription is active

		events := []eventstore.Event{
			{Type: "RealtimeEvent1", Data: []byte(`{"message": "hello"}`)},
			{Type: "RealtimeEvent2", Data: []byte(`{"message": "world"}`)},
		}

		streamID := "realtime-stream-" + time.Now().Format("20060102150405")
		if err := store.Append(streamID, events, -1); err != nil {
			t.Errorf("Failed to append realtime events: %v", err)
		}
	}()

	// Collect events from subscription
	var receivedEvents []eventstore.Event
	timeout := time.After(2 * time.Second)

	for len(receivedEvents) < 2 {
		select {
		case event := <-subscription.Events():
			receivedEvents = append(receivedEvents, event)
		case err := <-subscription.Errors():
			t.Fatalf("Subscription error: %v", err)
		case <-timeout:
			t.Fatalf("Timeout waiting for events. Received %d events, expected 2", len(receivedEvents))
		}
	}

	// Verify received events
	if len(receivedEvents) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(receivedEvents))
	}

	expectedTypes := []string{"RealtimeEvent1", "RealtimeEvent2"}
	for i, event := range receivedEvents {
		if event.Type != expectedTypes[i] {
			t.Errorf("Expected event type %s at index %d, got %s", expectedTypes[i], i, event.Type)
		}
	}
}

func TestPostgresEventConsumer_Integration_Subscribe_ExistingEvents(t *testing.T) {
	consumer, store, db := setupTestConsumer(t)
	defer db.Close()

	// Add events before subscription
	beforeTime := time.Now()
	time.Sleep(10 * time.Millisecond)

	events := []eventstore.Event{
		{Type: "ExistingEvent1", Data: []byte(`{"existing": true}`)},
		{Type: "ExistingEvent2", Data: []byte(`{"existing": true}`)},
	}

	streamID := "existing-stream-" + time.Now().Format("20060102150405")
	err := store.Append(streamID, events, -1)
	if err != nil {
		t.Fatalf("Failed to append existing events: %v", err)
	}

	// Subscribe from before events were added
	subscription, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: beforeTime,
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer subscription.Close()

	// Collect existing events from subscription
	var receivedEvents []eventstore.Event
	timeout := time.After(2 * time.Second)

	for len(receivedEvents) < 2 {
		select {
		case event := <-subscription.Events():
			receivedEvents = append(receivedEvents, event)
		case err := <-subscription.Errors():
			t.Fatalf("Subscription error: %v", err)
		case <-timeout:
			t.Fatalf("Timeout waiting for existing events. Received %d events, expected 2", len(receivedEvents))
		}
	}

	// Verify received events
	if len(receivedEvents) != 2 {
		t.Fatalf("Expected 2 existing events, got %d", len(receivedEvents))
	}

	expectedTypes := []string{"ExistingEvent1", "ExistingEvent2"}
	for i, event := range receivedEvents {
		if event.Type != expectedTypes[i] {
			t.Errorf("Expected event type %s at index %d, got %s", expectedTypes[i], i, event.Type)
		}
	}
}

func TestPostgresEventConsumer_Integration_Subscribe_MultipleStreams(t *testing.T) {
	consumer, store, db := setupTestConsumerWithTableName(t,
		fmt.Sprintf("multistream_events_%d", time.Now().UnixNano()),
		50*time.Millisecond) // Fast polling for testing
	defer db.Close()

	beforeTime := time.Now()

	// Add events to multiple streams
	stream1Events := []eventstore.Event{
		{Type: "User1Created", Data: []byte(`{"user_id": "user1"}`)},
	}
	stream2Events := []eventstore.Event{
		{Type: "User2Created", Data: []byte(`{"user_id": "user2"}`)},
	}
	stream3Events := []eventstore.Event{
		{Type: "OrderCreated", Data: []byte(`{"order_id": "order1"}`)},
	}

	streamID1 := "users-1-" + time.Now().Format("20060102150405")
	streamID2 := "users-2-" + time.Now().Format("20060102150405")
	streamID3 := "orders-1-" + time.Now().Format("20060102150405")

	err := store.Append(streamID1, stream1Events, -1)
	if err != nil {
		t.Fatalf("Failed to append to stream1: %v", err)
	}

	err = store.Append(streamID2, stream2Events, -1)
	if err != nil {
		t.Fatalf("Failed to append to stream2: %v", err)
	}

	err = store.Append(streamID3, stream3Events, -1)
	if err != nil {
		t.Fatalf("Failed to append to stream3: %v", err)
	}

	// Subscribe to all streams
	subscription, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: beforeTime,
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer subscription.Close()

	// Collect events from all streams
	var receivedEvents []eventstore.Event
	timeout := time.After(2 * time.Second)

	for len(receivedEvents) < 3 {
		select {
		case event := <-subscription.Events():
			receivedEvents = append(receivedEvents, event)
		case err := <-subscription.Errors():
			t.Fatalf("Subscription error: %v", err)
		case <-timeout:
			t.Fatalf("Timeout waiting for multi-stream events. Received %d events, expected 3", len(receivedEvents))
		}
	}

	// Verify we got events from all streams
	if len(receivedEvents) != 3 {
		t.Fatalf("Expected 3 events from multiple streams, got %d", len(receivedEvents))
	}

	// Verify we got the expected event types (order may vary due to timestamp proximity)
	receivedTypes := make(map[string]bool)
	for _, event := range receivedEvents {
		receivedTypes[event.Type] = true
	}

	expectedTypes := []string{"User1Created", "User2Created", "OrderCreated"}
	for _, expectedType := range expectedTypes {
		if !receivedTypes[expectedType] {
			t.Errorf("Expected to receive event type %s", expectedType)
		}
	}
}

func TestPostgresEventConsumer_Integration_Subscribe_PollingInterval(t *testing.T) {
	// Test with longer polling interval to verify timing
	pollingInterval := 500 * time.Millisecond
	consumer, store, db := setupTestConsumerWithTableName(t,
		fmt.Sprintf("polling_events_%d", time.Now().UnixNano()),
		pollingInterval)
	defer db.Close()

	// Subscribe from current time
	subscription, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: time.Now(),
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer subscription.Close()

	// Add event and measure time to receive it
	startTime := time.Now()

	go func() {
		time.Sleep(100 * time.Millisecond) // Add event after subscription starts

		events := []eventstore.Event{
			{Type: "PollingTestEvent", Data: []byte(`{"test": true}`)},
		}

		streamID := "polling-stream-" + time.Now().Format("20060102150405")
		if err := store.Append(streamID, events, -1); err != nil {
			t.Errorf("Failed to append polling test event: %v", err)
		}
	}()

	// Wait for the event
	timeout := time.After(2 * time.Second)

	select {
	case event := <-subscription.Events():
		elapsed := time.Since(startTime)

		if event.Type != "PollingTestEvent" {
			t.Errorf("Expected PollingTestEvent, got %s", event.Type)
		}

		// The event should be received within a reasonable timeframe considering polling interval
		// It should take at least 100ms (our delay) but not much more than pollingInterval + delay
		minExpected := 100 * time.Millisecond
		maxExpected := pollingInterval + 200*time.Millisecond // Some buffer for processing

		if elapsed < minExpected {
			t.Errorf("Event received too quickly: %v (expected at least %v)", elapsed, minExpected)
		}
		if elapsed > maxExpected {
			t.Errorf("Event received too slowly: %v (expected at most %v)", elapsed, maxExpected)
		}

	case err := <-subscription.Errors():
		t.Fatalf("Subscription error: %v", err)
	case <-timeout:
		t.Fatalf("Timeout waiting for polling test event")
	}
}

func TestPostgresEventConsumer_Integration_Subscribe_Close(t *testing.T) {
	consumer, _, db := setupTestConsumer(t)
	defer db.Close()

	// Create subscription
	subscription, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: time.Now(),
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Close subscription
	err = subscription.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify channels are closed (should not block)
	select {
	case _, ok := <-subscription.Events():
		if ok {
			t.Error("Events channel should be closed after Close()")
		}
	case <-time.After(100 * time.Millisecond):
		// This is fine - channel might not be closed immediately but should not block
	}

	// Second close should be idempotent
	err = subscription.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

func TestPostgresEventConsumer_Integration_Retrieve_CrossStreamConsumption(t *testing.T) {
	consumer, store, db := setupTestConsumer(t)
	defer db.Close()

	beforeTime := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Create events in different streams representing a business process
	// User stream
	userEvents := []eventstore.Event{
		{Type: "UserRegistered", Data: []byte(`{"user_id": "123", "email": "test@example.com"}`)},
		{Type: "UserEmailVerified", Data: []byte(`{"user_id": "123"}`)},
	}

	// Order stream
	orderEvents := []eventstore.Event{
		{Type: "OrderCreated", Data: []byte(`{"order_id": "456", "user_id": "123", "amount": 100}`)},
		{Type: "OrderPaid", Data: []byte(`{"order_id": "456", "payment_method": "card"}`)},
	}

	// Inventory stream
	inventoryEvents := []eventstore.Event{
		{Type: "InventoryReserved", Data: []byte(`{"product_id": "789", "quantity": 1, "order_id": "456"}`)},
	}

	userStreamID := "user-123-" + time.Now().Format("20060102150405")
	orderStreamID := "order-456-" + time.Now().Format("20060102150405")
	inventoryStreamID := "inventory-789-" + time.Now().Format("20060102150405")

	// Add events across different streams
	err := store.Append(userStreamID, userEvents, -1)
	if err != nil {
		t.Fatalf("Failed to append user events: %v", err)
	}

	err = store.Append(orderStreamID, orderEvents, -1)
	if err != nil {
		t.Fatalf("Failed to append order events: %v", err)
	}

	err = store.Append(inventoryStreamID, inventoryEvents, -1)
	if err != nil {
		t.Fatalf("Failed to append inventory events: %v", err)
	}

	// Retrieve all events across streams
	allEvents, err := consumer.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: beforeTime,
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Cross-stream retrieve failed: %v", err)
	}

	// Verify we got all events from all streams
	if len(allEvents) != 5 {
		t.Fatalf("Expected 5 events across all streams, got %d", len(allEvents))
	}

	// Verify we have events from all different streams
	eventTypes := make(map[string]bool)
	for _, event := range allEvents {
		eventTypes[event.Type] = true

		// Verify each event has proper metadata
		if event.ID == "" {
			t.Errorf("Event %s missing ID", event.Type)
		}
		if event.Timestamp.IsZero() {
			t.Errorf("Event %s missing timestamp", event.Type)
		}
		if event.Version == 0 {
			t.Errorf("Event %s missing version", event.Type)
		}
	}

	expectedTypes := []string{"UserRegistered", "UserEmailVerified", "OrderCreated", "OrderPaid", "InventoryReserved"}
	for _, expectedType := range expectedTypes {
		if !eventTypes[expectedType] {
			t.Errorf("Missing expected event type: %s", expectedType)
		}
	}

	// Verify events are ordered by timestamp (chronological order of business process)
	for i := 1; i < len(allEvents); i++ {
		if allEvents[i].Timestamp.Before(allEvents[i-1].Timestamp) {
			t.Errorf("Events not ordered by timestamp: event %d (%v, %s) before event %d (%v, %s)",
				i, allEvents[i].Timestamp, allEvents[i].Type,
				i-1, allEvents[i-1].Timestamp, allEvents[i-1].Type)
		}
	}
}

// TestPostgresEventConsumer_Integration_NoDuplicateEvents tests that subscriptions don't receive duplicate events
// when using timestamp-based filtering with Event.ID tracking for duplicates within the same timestamp.
func TestPostgresEventConsumer_Integration_NoDuplicateEvents(t *testing.T) {
	consumer, store, db := setupTestConsumer(t)
	defer db.Close()

	// Create events with very close timestamps to test duplicate handling
	baseTime := time.Now().UTC().Truncate(time.Millisecond)
	
	events := []eventstore.Event{
		{
			ID:        "event-1",
			Type:      "TestEvent",
			Data:      []byte(`{"message": "Event 1"}`),
			Timestamp: baseTime,
		},
		{
			ID:        "event-2",
			Type:      "TestEvent",
			Data:      []byte(`{"message": "Event 2"}`),
			Timestamp: baseTime, // Same timestamp as event-1
		},
		{
			ID:        "event-3",
			Type:      "TestEvent",
			Data:      []byte(`{"message": "Event 3"}`),
			Timestamp: baseTime.Add(1 * time.Millisecond),
		},
	}

	// Add events to store
	err := store.Append("test-stream-1", events[:2], -1)
	if err != nil {
		t.Fatalf("Failed to append first batch of events: %v", err)
	}

	// Subscribe from beginning
	subscription, err := consumer.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: baseTime,
		BatchSize:     10,
	})
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}
	defer subscription.Close()

	// Collect initial events
	var receivedEvents []eventstore.Event
	timeout := time.After(2 * time.Second)

	// Wait for initial events
	for len(receivedEvents) < 2 {
		select {
		case event := <-subscription.Events():
			receivedEvents = append(receivedEvents, event)
		case err := <-subscription.Errors():
			t.Fatalf("Subscription error: %v", err)
		case <-timeout:
			t.Fatalf("Timeout waiting for initial events, got %d events", len(receivedEvents))
		}
	}

	// Add third event after subscription is active
	err = store.Append("test-stream-2", events[2:3], -1)
	if err != nil {
		t.Fatalf("Failed to append third event: %v", err)
	}

	// Wait for the third event to be delivered
	timeout = time.After(3 * time.Second)
	for len(receivedEvents) < 3 {
		select {
		case event := <-subscription.Events():
			receivedEvents = append(receivedEvents, event)
		case err := <-subscription.Errors():
			t.Fatalf("Subscription error: %v", err)
		case <-timeout:
			t.Fatalf("Timeout waiting for third event, got %d events", len(receivedEvents))
		}
	}

	// Check for no additional duplicate events
	timeout = time.After(1 * time.Second)
	select {
	case event := <-subscription.Events():
		t.Errorf("Received unexpected duplicate event: %+v", event)
	case err := <-subscription.Errors():
		t.Fatalf("Subscription error: %v", err)
	case <-timeout:
		// This is expected - no more events should arrive
	}

	// Verify we received exactly 3 events with correct IDs
	if len(receivedEvents) != 3 {
		t.Errorf("Expected exactly 3 events, got %d", len(receivedEvents))
	}

	expectedIDs := []string{"event-1", "event-2", "event-3"}
	for i, event := range receivedEvents {
		if i < len(expectedIDs) && event.ID != expectedIDs[i] {
			t.Errorf("Event %d: expected ID %s, got %s", i, expectedIDs[i], event.ID)
		}
	}

	// Verify events are ordered correctly
	for i := 1; i < len(receivedEvents); i++ {
		if receivedEvents[i].Timestamp.Before(receivedEvents[i-1].Timestamp) {
			t.Errorf("Events not ordered by timestamp: event %d (%v) before event %d (%v)",
				i, receivedEvents[i].Timestamp, i-1, receivedEvents[i-1].Timestamp)
		}
	}
}
