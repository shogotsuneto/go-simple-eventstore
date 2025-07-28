package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	eventstore "github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/memory"
	"github.com/shogotsuneto/go-simple-eventstore/postgres"
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
	// Command-line flags for backend selection
	var backend = flag.String("backend", "memory", "Event store backend: memory or postgres")
	var pgConnStr = flag.String("postgres-conn", "host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable", "PostgreSQL connection string")
	flag.Parse()

	fmt.Println("🚀 Go Simple EventStore - Hello World Example")
	fmt.Println("============================================")
	fmt.Printf("Using backend: %s\n", *backend)

	// Create event store based on backend selection
	var store eventstore.EventStore
	var err error

	switch *backend {
	case "memory":
		store = memory.NewInMemoryEventStore()
		fmt.Println("✅ In-memory event store created")
	case "postgres":
		pgStore, pgErr := postgres.NewPostgresEventStore(*pgConnStr)
		if pgErr != nil {
			log.Fatalf("Failed to create PostgreSQL event store: %v", pgErr)
		}
		store = pgStore
		defer pgStore.Close()
		fmt.Println("✅ PostgreSQL event store created")
	default:
		fmt.Printf("❌ Unknown backend: %s\n", *backend)
		fmt.Println("Available backends: memory, postgres")
		os.Exit(1)
	}

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

	err = store.Append(streamID, events)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Println("✅ Events appended successfully!")

	// Load events from the stream
	fmt.Printf("\n📖 Loading events from stream '%s'...\n", streamID)

	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
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

	versionEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 1, Limit: 1})
	if err != nil {
		log.Fatalf("Failed to load events with version: %v", err)
	}

	fmt.Printf("✅ Loaded %d event(s) with version:\n", len(versionEvents))
	for _, event := range versionEvents {
		fmt.Printf("  Event Type: %s, Version: %d\n", event.Type, event.Version)
	}

	fmt.Println("\n🎉 Hello World example completed successfully!")
}
