package postgres

import (
	"testing"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
)



func TestPostgresEventConsumer_Subscribe_InvalidConnection(t *testing.T) {
	// Test that Subscribe method exists and can be called
	// This is a minimal test since we don't have a real DB connection in unit tests
	store := &PostgresEventConsumer{
		pgClient: &pgClient{
			db:        nil, // This will cause an error when actually used
			tableName: "events",
		},
		subscriptions:   []*PostgresSubscription{}, // Updated to slice
		pollingInterval: 2 * time.Second,
	}

	subscription, err := store.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: time.Time{}, // Updated to use FromTimestamp
		BatchSize:     10,
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

func TestPostgresEventConsumer_Subscribe_ConfigurablePollingInterval(t *testing.T) {
	// Test that polling interval can be configured at consumer level
	customInterval := 5 * time.Second
	store := &PostgresEventConsumer{
		pgClient: &pgClient{
			db:        nil, // This will cause an error when actually used
			tableName: "events",
		},
		subscriptions:   []*PostgresSubscription{}, // Updated to slice
		pollingInterval: customInterval,
	}

	// Test subscription creation
	subscription, err := store.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: time.Time{}, // Updated to use FromTimestamp
		BatchSize:     10,
	})

	// Subscribe should succeed initially (it starts asynchronously)
	if err != nil {
		t.Errorf("Subscribe should not error immediately: %v", err)
	}

	if subscription == nil {
		t.Error("Expected non-nil subscription")
	}

	// Verify the subscription has the correct polling interval from the consumer
	pgSub, ok := subscription.(*PostgresSubscription)
	if !ok {
		t.Error("Expected PostgresSubscription type")
	} else {
		if pgSub.pollingInterval != customInterval {
			t.Errorf("Expected polling interval %v, got %v", customInterval, pgSub.pollingInterval)
		}
	}

	// Clean up
	if subscription != nil {
		subscription.Close()
	}
}

func TestPostgresEventConsumer_Subscribe_DefaultPollingInterval(t *testing.T) {
	// Test that default polling interval is used when not specified in constructor
	store := &PostgresEventConsumer{
		pgClient: &pgClient{
			db:        nil, // This will cause an error when actually used
			tableName: "events",
		},
		subscriptions:   []*PostgresSubscription{}, // Updated to slice
		pollingInterval: 1 * time.Second,           // Default interval
	}

	// Test subscription creation
	subscription, err := store.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: time.Time{}, // Updated to use FromTimestamp
		BatchSize:     10,
	})

	// Subscribe should succeed initially (it starts asynchronously)
	if err != nil {
		t.Errorf("Subscribe should not error immediately: %v", err)
	}

	if subscription == nil {
		t.Error("Expected non-nil subscription")
	}

	// Verify the subscription has the default polling interval
	pgSub, ok := subscription.(*PostgresSubscription)
	if !ok {
		t.Error("Expected PostgresSubscription type")
	} else {
		expectedDefault := 1 * time.Second
		if pgSub.pollingInterval != expectedDefault {
			t.Errorf("Expected default polling interval %v, got %v", expectedDefault, pgSub.pollingInterval)
		}
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
		store: &PostgresEventConsumer{
			subscriptions: []*PostgresSubscription{}, // Updated to slice
		},
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

func TestNewPostgresEventConsumer_PollingInterval(t *testing.T) {
	// Test that NewPostgresEventConsumer correctly sets polling interval
	customInterval := 3 * time.Second
	consumer := NewPostgresEventConsumer(nil, "events", customInterval)

	pgConsumer, ok := consumer.(*PostgresEventConsumer)
	if !ok {
		t.Error("Expected PostgresEventConsumer type")
		return
	}

	if pgConsumer.pollingInterval != customInterval {
		t.Errorf("Expected polling interval %v, got %v", customInterval, pgConsumer.pollingInterval)
	}
}

func TestNewPostgresEventConsumer_DefaultPollingInterval(t *testing.T) {
	// Test that NewPostgresEventConsumer uses default when interval is 0
	consumer := NewPostgresEventConsumer(nil, "events", 0)

	pgConsumer, ok := consumer.(*PostgresEventConsumer)
	if !ok {
		t.Error("Expected PostgresEventConsumer type")
		return
	}

	expectedDefault := 1 * time.Second
	if pgConsumer.pollingInterval != expectedDefault {
		t.Errorf("Expected default polling interval %v, got %v", expectedDefault, pgConsumer.pollingInterval)
	}
}
