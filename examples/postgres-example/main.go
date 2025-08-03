package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	_ "github.com/lib/pq" // PostgreSQL driver
	eventstore "github.com/shogotsuneto/go-simple-eventstore"
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
	// Command-line flags for PostgreSQL connection and custom table name
	var pgConnStr = flag.String("postgres-conn", "host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable", "PostgreSQL connection string")
	var tableName = flag.String("table-name", "", "Custom table name for storing events (uses default 'events' if not specified)")
	flag.Parse()

	fmt.Println("🚀 Go Simple EventStore - PostgreSQL Example")
	fmt.Println("============================================")

	var store eventstore.EventStore
	var db *sql.DB
	var err error

	// Create PostgreSQL connection
	fmt.Printf("Connecting to PostgreSQL...\n")
	db, err = sql.Open("postgres", *pgConnStr)
	if err != nil {
		log.Fatalf("Failed to open PostgreSQL connection: %v", err)
	}
	defer db.Close()

	// Test the connection
	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}

	// Initialize the database schema
	if *tableName != "" {
		fmt.Printf("🔧 Initializing database schema with table '%s'...\n", *tableName)
	} else {
		fmt.Println("🔧 Initializing database schema with default table 'events'...")
	}
	if err := postgres.InitSchema(db, *tableName); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	// Create PostgreSQL event store
	store = postgres.NewPostgresEventStore(db, *tableName)

	fmt.Println("✅ PostgreSQL event store ready")

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

	err = store.Append(streamID, events, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Println("✅ Events appended successfully!")

	// Load events from the stream
	fmt.Printf("\n📖 Loading events from stream '%s'...\n", streamID)

	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 0, Limit: 10})
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

	versionEvents, err := store.Load(streamID, eventstore.LoadOptions{AfterVersion: 1, Limit: 1})
	if err != nil {
		log.Fatalf("Failed to load events with version: %v", err)
	}

	fmt.Printf("✅ Loaded %d event(s) with version:\n", len(versionEvents))
	for _, event := range versionEvents {
		fmt.Printf("  Event Type: %s, Version: %d\n", event.Type, event.Version)
	}

	// Demonstrate optimistic concurrency control
	fmt.Println("\n🔐 Demonstrating optimistic concurrency control...")

	// First append with expected version 2 (should succeed since we have 2 events)
	moreEvents := []eventstore.Event{
		{
			Type: "UserActivated",
			Data: []byte(`{"user_id": "user-123", "activated": true}`),
			Metadata: map[string]string{
				"source": "user-service",
				"reason": "activation",
			},
		},
	}

	err = store.Append(streamID, moreEvents, 2)
	if err != nil {
		log.Printf("Expected append failed: %v", err)
	} else {
		fmt.Println("✅ Append with expected version 2 succeeded")
	}

	// Try to append with wrong expected version (should fail)
	err = store.Append(streamID, moreEvents, 1)
	if err != nil {
		fmt.Printf("✅ Append with wrong expected version correctly failed: %v\n", err)
	} else {
		fmt.Println("❌ Append with wrong expected version should have failed")
	}

	fmt.Println("\n🎉 PostgreSQL example completed successfully!")

	if *tableName != "" {
		fmt.Printf("💡 Events were stored in custom table '%s'. You can run this example with different table names using the -table-name flag.\n", *tableName)
	} else {
		fmt.Println("💡 Events were stored in the default 'events' table. You can use a custom table name with the -table-name flag.")
		fmt.Println("   Example: go run main.go -table-name=my_custom_events")
	}
}
