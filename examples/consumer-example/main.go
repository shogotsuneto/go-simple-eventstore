package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	eventstore "github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/memory"
)

// UserCreated represents a domain event when a user is created
type UserCreated struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
}

// UserEmailChanged represents a domain event when a user's email is updated
type UserEmailChanged struct {
	UserID   string `json:"user_id"`
	NewEmail string `json:"new_email"`
	OldEmail string `json:"old_email"`
}

func main() {
	fmt.Println("🚀 Go Simple EventStore - Consumer Example")
	fmt.Println("==========================================")

	// Create a new in-memory event store with consumer capabilities
	store := memory.NewInMemoryEventConsumer()

	// First, let's demonstrate polling
	fmt.Println("\n📊 Demonstrating Polling...")
	demonstratePolling(store)

	// Then, let's demonstrate subscriptions
	fmt.Println("\n📡 Demonstrating Subscriptions...")
	demonstrateSubscriptions(store)

	fmt.Println("\n🎉 Consumer example completed successfully!")
}

func demonstratePolling(store *memory.InMemoryEventConsumer) {
	// Add some initial events
	userCreatedData, _ := json.Marshal(UserCreated{
		UserID: "user-456",
		Name:   "Jane Doe",
		Email:  "jane@example.com",
	})

	events := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData,
			Metadata: map[string]string{
				"source":  "user-service",
				"version": "1.0",
			},
		},
	}

	streamID := "user-456"
	err := store.Append(streamID, events, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Printf("Added 1 event to stream '%s'\n", streamID)

	// Poll for events from the beginning
	fmt.Println("Polling for events from the beginning...")
	polledEvents, err := store.Poll(streamID, eventstore.ConsumeOptions{
		FromVersion: 0,
		BatchSize:   10,
	})
	if err != nil {
		log.Fatalf("Failed to poll events: %v", err)
	}

	fmt.Printf("Polled %d events:\n", len(polledEvents))
	for _, event := range polledEvents {
		fmt.Printf("  - %s (Version %d): %s\n", event.Type, event.Version, string(event.Data))
	}

	// Add more events
	userEmailChangedData, _ := json.Marshal(UserEmailChanged{
		UserID:   "user-456",
		NewEmail: "jane.doe@example.com",
		OldEmail: "jane@example.com",
	})

	moreEvents := []eventstore.Event{
		{
			Type: "UserEmailChanged",
			Data: userEmailChangedData,
			Metadata: map[string]string{
				"source":  "user-service",
				"version": "1.0",
			},
		},
	}

	err = store.Append(streamID, moreEvents, -1)
	if err != nil {
		log.Fatalf("Failed to append more events: %v", err)
	}

	// Poll for new events only (from version 1)
	fmt.Println("Polling for new events only (from version 1)...")
	newEvents, err := store.Poll(streamID, eventstore.ConsumeOptions{
		FromVersion: 1,
		BatchSize:   10,
	})
	if err != nil {
		log.Fatalf("Failed to poll new events: %v", err)
	}

	fmt.Printf("Polled %d new events:\n", len(newEvents))
	for _, event := range newEvents {
		fmt.Printf("  - %s (Version %d): %s\n", event.Type, event.Version, string(event.Data))
	}
}

func demonstrateSubscriptions(store *memory.InMemoryEventConsumer) {
	streamID := "user-789"

	// Create a subscription starting from the beginning
	fmt.Printf("Creating subscription to stream '%s'...\n", streamID)
	subscription, err := store.Subscribe(streamID, eventstore.ConsumeOptions{
		FromVersion: 0,
		BatchSize:   10,
	})
	if err != nil {
		log.Fatalf("Failed to create subscription: %v", err)
	}
	defer subscription.Close()

	// Start a goroutine to handle events
	eventCount := 0
	go func() {
		fmt.Println("Listening for events...")
		for {
			select {
			case event := <-subscription.Events():
				eventCount++
				fmt.Printf("  📧 Received event: %s (Version %d)\n", event.Type, event.Version)
				fmt.Printf("     Data: %s\n", string(event.Data))
			case err := <-subscription.Errors():
				fmt.Printf("  ❌ Subscription error: %v\n", err)
				return
			}
		}
	}()

	// Add some events after subscription is created
	fmt.Println("Adding events to the stream...")

	userCreatedData, _ := json.Marshal(UserCreated{
		UserID: "user-789",
		Name:   "Alice Smith",
		Email:  "alice@example.com",
	})

	events1 := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData,
			Metadata: map[string]string{
				"source": "user-service",
			},
		},
	}

	err = store.Append(streamID, events1, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	// Wait a bit for the event to be processed
	time.Sleep(100 * time.Millisecond)

	// Add another event
	userEmailChangedData, _ := json.Marshal(UserEmailChanged{
		UserID:   "user-789",
		NewEmail: "alice.smith@example.com",
		OldEmail: "alice@example.com",
	})

	events2 := []eventstore.Event{
		{
			Type: "UserEmailChanged",
			Data: userEmailChangedData,
			Metadata: map[string]string{
				"source": "user-service",
			},
		},
	}

	err = store.Append(streamID, events2, -1)
	if err != nil {
		log.Fatalf("Failed to append second event: %v", err)
	}

	// Wait for events to be processed
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("✅ Subscription processed %d events\n", eventCount)

	// Demonstrate subscription from a specific version
	fmt.Println("\nCreating subscription from version 1...")
	subscription2, err := store.Subscribe(streamID, eventstore.ConsumeOptions{
		FromVersion: 1, // Start from version 1, should only get the second event
		BatchSize:   10,
	})
	if err != nil {
		log.Fatalf("Failed to create second subscription: %v", err)
	}
	defer subscription2.Close()

	// Handle events from the second subscription
	go func() {
		for {
			select {
			case event := <-subscription2.Events():
				fmt.Printf("  📧 Second subscription received: %s (Version %d)\n", event.Type, event.Version)
			case err := <-subscription2.Errors():
				fmt.Printf("  ❌ Second subscription error: %v\n", err)
				return
			}
		}
	}()

	// Wait for the subscription to process existing events
	time.Sleep(100 * time.Millisecond)

	fmt.Println("✅ Both subscriptions are working correctly")
}
