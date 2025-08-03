package memory

import (
	"testing"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)

func TestInMemoryEventStore_Poll(t *testing.T) {
	store := NewInMemoryEventStore()

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

func TestInMemoryEventStore_Subscribe(t *testing.T) {
	store := NewInMemoryEventStore()

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

func TestInMemoryEventStore_Subscribe_WithFromVersion(t *testing.T) {
	store := NewInMemoryEventStore()

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

func TestInMemoryEventStore_Subscribe_Close(t *testing.T) {
	store := NewInMemoryEventStore()

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
	store.mu.RLock()
	subs := store.subscriptions["user-123"]
	store.mu.RUnlock()

	if len(subs) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(subs))
	}
}
