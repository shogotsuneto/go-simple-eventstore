package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	eventstore "github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/postgres"
)

// UserCreated represents a domain event when a user is created
type UserCreated struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
}

func main() {
	// Command-line flags for PostgreSQL connection and table name
	var pgConnStr = flag.String("postgres-conn", "host=localhost port=5432 user=test password=test dbname=eventstore_test sslmode=disable", "PostgreSQL connection string")
	var tableName = flag.String("table-name", "custom_events", "Table name for storing events")
	flag.Parse()

	fmt.Println("🚀 Go Simple EventStore - Custom Table Name Example")
	fmt.Println("==================================================")
	fmt.Printf("Connecting to PostgreSQL with custom table name '%s'...\n", *tableName)

	// Create PostgreSQL event store with custom configuration
	store, err := postgres.NewPostgresEventStoreWithConfig(postgres.Config{
		ConnectionString: *pgConnStr,
		TableName:        *tableName,
	})
	if err != nil {
		log.Fatalf("Failed to create PostgreSQL event store: %v", err)
	}
	defer store.Close()

	// Initialize the database schema with custom table name
	fmt.Printf("🔧 Initializing database schema with table '%s'...\n", *tableName)
	if err := store.InitSchema(); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}
	fmt.Printf("✅ PostgreSQL event store ready with custom table '%s'\n", *tableName)

	// Create some domain events
	userCreatedData, _ := json.Marshal(UserCreated{
		UserID: "user-456",
		Name:   "Jane Smith",
		Email:  "jane@example.com",
	})

	// Define events
	events := []eventstore.Event{
		{
			Type: "UserCreated",
			Data: userCreatedData,
			Metadata: map[string]string{
				"source":    "user-service",
				"reason":    "registration",
				"table":     *tableName,
			},
		},
	}

	// Append events to a stream
	streamID := "user-456"
	fmt.Printf("\n📝 Appending %d event to stream '%s' in table '%s'...\n", len(events), streamID, *tableName)

	err = store.Append(streamID, events, -1)
	if err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	fmt.Println("✅ Events appended successfully to custom table!")

	// Load events from the stream
	fmt.Printf("\n📖 Loading events from stream '%s' in table '%s'...\n", streamID, *tableName)

	loadedEvents, err := store.Load(streamID, eventstore.LoadOptions{FromVersion: 0, Limit: 10})
	if err != nil {
		log.Fatalf("Failed to load events: %v", err)
	}

	fmt.Printf("✅ Loaded %d events from custom table:\n\n", len(loadedEvents))

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

	// Demonstrate backward compatibility - create store with default constructor
	fmt.Println("\n🔄 Demonstrating backward compatibility...")
	fmt.Println("Creating store with default constructor (uses 'events' table):")

	defaultStore, err := postgres.NewPostgresEventStore(*pgConnStr)
	if err != nil {
		log.Fatalf("Failed to create default PostgreSQL event store: %v", err)
	}
	defer defaultStore.Close()

	if err := defaultStore.InitSchema(); err != nil {
		log.Fatalf("Failed to initialize default schema: %v", err)
	}

	// Add an event to the default table
	defaultEvents := []eventstore.Event{
		{
			Type: "BackwardCompatibilityTest",
			Data: []byte(`{"message": "This goes to the default 'events' table"}`),
			Metadata: map[string]string{
				"source": "compatibility-test",
			},
		},
	}

	defaultStreamID := "compatibility-test"
	err = defaultStore.Append(defaultStreamID, defaultEvents, -1)
	if err != nil {
		log.Fatalf("Failed to append to default store: %v", err)
	}

	fmt.Println("✅ Successfully used both custom table and default table!")
	fmt.Printf("\n🎉 Custom table name example completed successfully!")
	fmt.Printf("\n💡 Events were stored in table '%s' and also in default 'events' table for compatibility demo.\n", *tableName)
}