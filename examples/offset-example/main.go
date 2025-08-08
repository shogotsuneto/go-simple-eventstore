package main

import (
	"fmt"
	"log"
	"time"

	"github.com/shogotsuneto/go-simple-eventstore"
	"github.com/shogotsuneto/go-simple-eventstore/memory"
)

func main() {
	fmt.Println("=== Table-Level Offset Example ===")
	fmt.Println()

	// Create an in-memory event store
	store := memory.NewInMemoryEventStore()

	// Add some events to different streams
	events1 := []eventstore.Event{
		{Type: "UserCreated", Data: []byte(`{"user_id": "123", "name": "Alice"}`), Metadata: map[string]string{"source": "user-service"}},
		{Type: "UserUpdated", Data: []byte(`{"user_id": "123", "name": "Alice Smith"}`), Metadata: map[string]string{"source": "user-service"}},
	}

	events2 := []eventstore.Event{
		{Type: "OrderCreated", Data: []byte(`{"order_id": "456", "user_id": "123"}`), Metadata: map[string]string{"source": "order-service"}},
	}

	events3 := []eventstore.Event{
		{Type: "PaymentProcessed", Data: []byte(`{"payment_id": "789", "order_id": "456"}`), Metadata: map[string]string{"source": "payment-service"}},
	}

	// Append events to different streams
	fmt.Println("1. Appending events to different streams...")
	if err := store.Append("user-123", events1, -1); err != nil {
		log.Fatalf("Failed to append user events: %v", err)
	}

	if err := store.Append("order-456", events2, -1); err != nil {
		log.Fatalf("Failed to append order events: %v", err)
	}

	if err := store.Append("payment-789", events3, -1); err != nil {
		log.Fatalf("Failed to append payment events: %v", err)
	}

	fmt.Println("✓ Events appended successfully")
	fmt.Println()

	// Demonstrate offset-based consumption
	fmt.Println("2. Consuming events by table-level offset...")

	// Retrieve all events using offset 0 (get everything)
	allEvents, err := store.Retrieve(eventstore.ConsumeOptions{
		FromOffset: 0,
		BatchSize:  10,
	})
	if err != nil {
		log.Fatalf("Failed to retrieve events: %v", err)
	}

	fmt.Printf("Retrieved %d events starting from offset 0:\n", len(allEvents))
	for _, event := range allEvents {
		fmt.Printf("  Offset: %d, Type: %s, Timestamp: %s\n", 
			event.Offset, event.Type, event.Timestamp.Format(time.RFC3339))
	}
	fmt.Println()

	// Retrieve events after offset 2
	laterEvents, err := store.Retrieve(eventstore.ConsumeOptions{
		FromOffset: 2,
		BatchSize:  10,
	})
	if err != nil {
		log.Fatalf("Failed to retrieve events: %v", err)
	}

	fmt.Printf("Retrieved %d events after offset 2:\n", len(laterEvents))
	for _, event := range laterEvents {
		fmt.Printf("  Offset: %d, Type: %s, Timestamp: %s\n", 
			event.Offset, event.Type, event.Timestamp.Format(time.RFC3339))
	}
	fmt.Println()

	// Demonstrate subscription with offset
	fmt.Println("3. Creating subscription starting from offset 3...")
	sub, err := store.Subscribe(eventstore.ConsumeOptions{
		FromOffset: 3,
		BatchSize:  5,
	})
	if err != nil {
		log.Fatalf("Failed to create subscription: %v", err)
	}
	defer sub.Close()

	// Listen for events in the background
	go func() {
		for {
			select {
			case event := <-sub.Events():
				fmt.Printf("📨 Subscription received event - Offset: %d, Type: %s\n", 
					event.Offset, event.Type)
			case err := <-sub.Errors():
				fmt.Printf("❌ Subscription error: %v\n", err)
				return
			}
		}
	}()

	// Wait to see if we get the existing event at offset 4
	time.Sleep(100 * time.Millisecond)

	// Add a new event
	fmt.Println("\n4. Adding new event to trigger subscription...")
	newEvents := []eventstore.Event{
		{Type: "NotificationSent", Data: []byte(`{"notification_id": "999", "user_id": "123"}`), Metadata: map[string]string{"source": "notification-service"}},
	}

	if err := store.Append("notification-999", newEvents, -1); err != nil {
		log.Fatalf("Failed to append notification event: %v", err)
	}

	// Wait to see the new event
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\n✓ Example completed successfully!")
	fmt.Println("\nKey benefits of offset-based consumption:")
	fmt.Println("• Deterministic ordering across all streams")
	fmt.Println("• No duplicate events (unlike timestamp-based consumption)")
	fmt.Println("• Atomic counters work reliably across different databases")
	fmt.Println("• Perfect for building reliable projections and read models")
}