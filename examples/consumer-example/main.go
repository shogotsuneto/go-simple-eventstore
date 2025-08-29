package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	eventstore "github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/memory"
)

// UserCreated represents a domain event when a user is created
type UserCreated struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
}

func main() {
	fmt.Println("🚀 Go Simple EventStore - Cursor-based Consumer Example")
	fmt.Println("=======================================================")

	// Create a new in-memory event store with both producer and consumer capabilities
	store := memory.NewInMemoryEventStore()

	// Add some initial events to different streams
	userCreatedData1, _ := json.Marshal(UserCreated{
		UserID: "user-456",
		Name:   "Jane Doe",
		Email:  "jane@example.com",
	})

	events1 := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData1,
			Metadata: map[string]string{
				"source":  "user-service",
				"version": "1.0",
			},
		},
	}

	streamID1 := "user-456"
	_, err := store.Append(streamID1, events1, -1)
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
	_, err = store.Append(streamID2, events2, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Printf("Added 1 event to stream '%s'\n", streamID2)

	// Demonstrate cursor-based fetching
	fmt.Println("\n📊 Demonstrating Cursor-based Fetching...")

	ctx := context.Background()

	// Fetch from beginning (nil cursor)
	batch1, cursor1, err := store.Fetch(ctx, nil, 10)
	if err != nil {
		log.Fatalf("Failed to fetch events: %v", err)
	}

	fmt.Printf("Fetched %d events from beginning:\n", len(batch1))
	for i, envelope := range batch1 {
		fmt.Printf("  %d. %s from stream '%s' (EventID: %s, Partition: %s)\n",
			i+1, envelope.Type, envelope.StreamID, envelope.EventID, envelope.Partition)
		fmt.Printf("     Data: %s\n", string(envelope.Data))
	}

	// Add more events
	userCreatedData3, _ := json.Marshal(UserCreated{
		UserID: "user-890",
		Name:   "Alice Johnson",
		Email:  "alice@example.com",
	})

	events3 := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData3,
			Metadata: map[string]string{
				"source": "user-service",
			},
		},
	}

	streamID3 := "user-890"
	_, err = store.Append(streamID3, events3, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Printf("\nAdded 1 event to stream '%s'\n", streamID3)

	// Fetch from the cursor (should get new events)
	batch2, cursor2, err := store.Fetch(ctx, cursor1, 10)
	if err != nil {
		log.Fatalf("Failed to fetch events from cursor: %v", err)
	}

	fmt.Printf("Fetched %d new events from cursor:\n", len(batch2))
	for i, envelope := range batch2 {
		fmt.Printf("  %d. %s from stream '%s' (EventID: %s)\n",
			i+1, envelope.Type, envelope.StreamID, envelope.EventID)
		fmt.Printf("     Data: %s\n", string(envelope.Data))
	}

	// Demonstrate commit (no-op for memory implementation)
	err = store.Commit(ctx, cursor2)
	if err != nil {
		log.Fatalf("Failed to commit cursor: %v", err)
	}

	fmt.Println("✅ Cursor committed successfully")

	// Try fetching again from the same cursor (should get empty result)
	batch3, _, err := store.Fetch(ctx, cursor2, 10)
	if err != nil {
		log.Fatalf("Failed to fetch events from cursor: %v", err)
	}

	fmt.Printf("Fetched %d events from same cursor (should be 0): %d\n", len(batch3), len(batch3))

	fmt.Println("\n🎉 Cursor-based consumer example completed successfully!")
}
