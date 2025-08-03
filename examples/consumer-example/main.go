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

	// Create a new in-memory event store with both producer and consumer capabilities
	store := memory.NewInMemoryEventStore()

	// First, let's demonstrate retrieving
	fmt.Println("\n📊 Demonstrating Retrieving...")
	demonstrateRetrieving(store)

	// Then, let's demonstrate subscriptions
	fmt.Println("\n📡 Demonstrating Subscriptions...")
	demonstrateSubscriptions(store)

	fmt.Println("\n🎉 Consumer example completed successfully!")
}

func demonstrateRetrieving(store memory.EventStoreConsumer) {
	// Add some initial events to different streams
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

	streamID1 := "user-456"
	err := store.Append(streamID1, events, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Printf("Added 1 event to stream '%s'\n", streamID1)

	// Add events to another stream
	userCreatedData2, _ := json.Marshal(UserCreated{
		UserID: "user-789",
		Name:   "John Smith",
		Email:  "john@example.com",
	})

	events2 := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData2,
			Metadata: map[string]string{
				"source":  "user-service",
				"version": "1.0",
			},
		},
	}

	streamID2 := "user-789"
	err = store.Append(streamID2, events2, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Printf("Added 1 event to stream '%s'\n", streamID2)

	// Retrieve events from all streams
	fmt.Println("Retrieving events from all streams...")
	retrievedEvents, err := store.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: time.Time{}, // From the beginning
		BatchSize:     10,
	})
	if err != nil {
		log.Fatalf("Failed to retrieve events: %v", err)
	}

	fmt.Printf("Retrieved %d events:\n", len(retrievedEvents))
	for _, event := range retrievedEvents {
		fmt.Printf("  - %s (Version %d, Timestamp %s): %s\n", event.Type, event.Version, event.Timestamp.Format(time.RFC3339), string(event.Data))
	}

	// Add more events after a small delay
	time.Sleep(100 * time.Millisecond)
	startTime := time.Now()
	
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

	err = store.Append(streamID1, moreEvents, -1)
	if err != nil {
		log.Fatalf("Failed to append more events: %v", err)
	}

	// Retrieve new events only (from startTime)
	fmt.Printf("Retrieving new events only (from %s)...\n", startTime.Format(time.RFC3339))
	newEvents, err := store.Retrieve(eventstore.ConsumeOptions{
		FromTimestamp: startTime,
		BatchSize:     10,
	})
	if err != nil {
		log.Fatalf("Failed to retrieve new events: %v", err)
	}

	fmt.Printf("Retrieved %d new events:\n", len(newEvents))
	for _, event := range newEvents {
		fmt.Printf("  - %s (Version %d, Timestamp %s): %s\n", event.Type, event.Version, event.Timestamp.Format(time.RFC3339), string(event.Data))
	}
}

func demonstrateSubscriptions(store memory.EventStoreConsumer) {
	streamID1 := "user-789"
	streamID2 := "user-890"

	// Create a subscription starting from the beginning (all streams)
	fmt.Println("Creating subscription to all streams...")
	subscription, err := store.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: time.Time{}, // From the beginning
		BatchSize:     10,
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
				fmt.Printf("  📧 Received event: %s (Version %d, Timestamp %s)\n", event.Type, event.Version, event.Timestamp.Format(time.RFC3339))
				fmt.Printf("     Data: %s\n", string(event.Data))
			case err := <-subscription.Errors():
				fmt.Printf("  ❌ Subscription error: %v\n", err)
				return
			}
		}
	}()

	// Add some events to different streams after subscription is created
	fmt.Println("Adding events to multiple streams...")

	userCreatedData1, _ := json.Marshal(UserCreated{
		UserID: "user-789",
		Name:   "Alice Smith",
		Email:  "alice@example.com",
	})

	events1 := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData1,
			Metadata: map[string]string{
				"source": "user-service",
			},
		},
	}

	err = store.Append(streamID1, events1, -1)
	if err != nil {
		log.Fatalf("Failed to append events to stream 1: %v", err)
	}

	// Wait a bit for the event to be processed
	time.Sleep(100 * time.Millisecond)

	// Add event to another stream
	userCreatedData2, _ := json.Marshal(UserCreated{
		UserID: "user-890",
		Name:   "Bob Johnson",
		Email:  "bob@example.com",
	})

	events2 := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData2,
			Metadata: map[string]string{
				"source": "user-service",
			},
		},
	}

	err = store.Append(streamID2, events2, -1)
	if err != nil {
		log.Fatalf("Failed to append events to stream 2: %v", err)
	}

	// Wait a bit and add another event
	time.Sleep(100 * time.Millisecond)

	userEmailChangedData, _ := json.Marshal(UserEmailChanged{
		UserID:   "user-789",
		NewEmail: "alice.smith@example.com",
		OldEmail: "alice@example.com",
	})

	events3 := []eventstore.Event{
		{
			Type: "UserEmailChanged",
			Data: userEmailChangedData,
			Metadata: map[string]string{
				"source": "user-service",
			},
		},
	}

	err = store.Append(streamID1, events3, -1)
	if err != nil {
		log.Fatalf("Failed to append third event: %v", err)
	}

	// Wait for events to be processed
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("✅ Subscription processed %d events\n", eventCount)

	// Demonstrate subscription from a specific timestamp
	fmt.Println("\nCreating subscription from a specific timestamp...")
	startTime := time.Now()
	
	subscription2, err := store.Subscribe(eventstore.ConsumeOptions{
		FromTimestamp: startTime, // Start from now, should only get new events
		BatchSize:     10,
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
				fmt.Printf("  📧 Second subscription received: %s (Version %d, Timestamp %s)\n", event.Type, event.Version, event.Timestamp.Format(time.RFC3339))
			case err := <-subscription2.Errors():
				fmt.Printf("  ❌ Second subscription error: %v\n", err)
				return
			}
		}
	}()

	// Add a new event after the second subscription
	time.Sleep(100 * time.Millisecond)
	
	finalEventData, _ := json.Marshal(UserEmailChanged{
		UserID:   "user-890",
		NewEmail: "bob.johnson@example.com",
		OldEmail: "bob@example.com",
	})

	finalEvents := []eventstore.Event{
		{
			Type: "UserEmailChanged",
			Data: finalEventData,
			Metadata: map[string]string{
				"source": "user-service",
			},
		},
	}

	err = store.Append(streamID2, finalEvents, -1)
	if err != nil {
		log.Fatalf("Failed to append final event: %v", err)
	}

	// Wait for the subscription to process new events
	time.Sleep(200 * time.Millisecond)

	fmt.Println("✅ Both subscriptions are working correctly")
}
