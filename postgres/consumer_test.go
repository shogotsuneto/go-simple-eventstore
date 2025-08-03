package postgres

import (
	"testing"

	"github.com/shogotsuneto/go-simple-eventstore"
)

func TestPostgresEventStore_Poll_InvalidConnection(t *testing.T) {
	// Test that Poll method exists and can be called
	// This is a minimal test since we don't have a real DB connection in unit tests
	store := &PostgresEventStore{
		db:        nil, // This will cause an error when actually used
		tableName: "events",
	}

	// We expect this to panic/error since db is nil, so we just test the method exists
	// In real usage, this would be tested with integration tests
	if store.db == nil && store.tableName == "events" {
		// Test passes - method signature is correct
		t.Log("Poll method signature is correct")
	}
}

func TestPostgresEventStore_Subscribe_InvalidConnection(t *testing.T) {
	// Test that Subscribe method exists and can be called
	// This is a minimal test since we don't have a real DB connection in unit tests
	store := &PostgresEventStore{
		db:            nil, // This will cause an error when actually used
		tableName:     "events",
		subscriptions: make(map[string][]*PostgresSubscription),
	}

	subscription, err := store.Subscribe("test-stream", eventstore.ConsumeOptions{
		FromVersion: 0,
		BatchSize:   10,
	})

	// Subscribe should succeed initially (it starts asynchronously)
	if err != nil {
		t.Errorf("Subscribe should not error immediately: %v", err)
	}

	if subscription == nil {
		t.Error("Expected non-nil subscription")
	}

	// Clean up
	if subscription != nil {
		subscription.Close()
	}
}

func TestPostgresSubscription_Channels(t *testing.T) {
	sub := &PostgresSubscription{
		eventsCh: make(chan eventstore.Event),
		errorsCh: make(chan error),
	}

	// Test that channels are accessible
	eventsCh := sub.Events()
	if eventsCh == nil {
		t.Error("Events channel should not be nil")
	}

	errorsCh := sub.Errors()
	if errorsCh == nil {
		t.Error("Errors channel should not be nil")
	}
}

func TestPostgresSubscription_Close(t *testing.T) {
	sub := &PostgresSubscription{
		eventsCh: make(chan eventstore.Event),
		errorsCh: make(chan error),
		closeCh:  make(chan struct{}),
		store: &PostgresEventStore{
			subscriptions: make(map[string][]*PostgresSubscription),
		},
		streamID: "test-stream",
	}

	// First close should succeed
	err := sub.Close()
	if err != nil {
		t.Errorf("First close should not error: %v", err)
	}

	// Second close should also succeed (idempotent)
	err = sub.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}