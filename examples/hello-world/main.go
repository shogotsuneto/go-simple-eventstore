package main

import (
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

// UserEmailChanged represents a domain event when a user's email is updated
type UserEmailChanged struct {
	UserID   string `json:"user_id"`
	NewEmail string `json:"new_email"`
	OldEmail string `json:"old_email"`
}

func main() {
	fmt.Println("🚀 Go Simple EventStore - Hello World Example")
	fmt.Println("============================================")

	// Create a new in-memory event store
	store := memory.NewInMemoryEventStore()

	// Create some domain events
	userCreatedData, _ := json.Marshal(UserCreated{
		UserID: "user-123",
		Name:   "John Doe",
		Email:  "john@example.com",
	})

	userEmailChangedData, _ := json.Marshal(UserEmailChanged{
		UserID:   "user-123",
		NewEmail: "john.doe@example.com",
		OldEmail: "john@example.com",
	})

	// Define events
	events := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData,
			Metadata: map[string]string{
				"source": "user-service",
				"reason": "registration",
			},
		},
		{
			Type: "UserEmailChanged",
			Data: userEmailChangedData,
			Metadata: map[string]string{
				"source": "user-service",
				"reason": "profile-update",
			},
		},
	}

	// Append events to a stream
	streamID := "user-123"
	fmt.Printf("\n📝 Appending %d events to stream '%s'...\n", len(events), streamID)

	err := store.Append(streamID, events, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Println("✅ Events appended successfully!")

	// Load events from the stream
	fmt.Printf("\n📖 Loading events from stream '%s'...\n", streamID)

	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 0, Limit: 10})
	if err != nil {
		log.Fatalf("Failed to load events: %v", err)
	}

	fmt.Printf("✅ Loaded %d events:\n\n", len(loadedEvents))

	// Display the loaded events
	for i, event := range loadedEvents {
		fmt.Printf("Event #%d:\n", i+1)
		fmt.Printf("  ID: %s\n", event.ID)
		fmt.Printf("  Type: %s\n", event.Type)
		fmt.Printf("  Version: %d\n", event.Version)
		fmt.Printf("  Timestamp: %s\n", event.Timestamp.Format("2006-01-02 15:04:05"))
		fmt.Printf("  Data: %s\n", string(event.Data))
		fmt.Printf("  Metadata: %v\n", event.Metadata)
		fmt.Println()
	}

	// Demonstrate version-based loading
	fmt.Println("🔍 Demonstrating version-based loading...")
	fmt.Println("Loading events starting from version 1:")

	versionEvents, err := store.Load(streamID, eventstore.LoadOptions{ExclusiveStartVersion: 1, Limit: 1})
	if err != nil {
		log.Fatalf("Failed to load events with version: %v", err)
	}

	fmt.Printf("✅ Loaded %d event(s) with version:\n", len(versionEvents))
	for _, event := range versionEvents {
		fmt.Printf("  Event Type: %s, Version: %d\n", event.Type, event.Version)
	}

	fmt.Println("\n🎉 Hello World example completed successfully!")
}
