package postgres

// This file previously contained unit tests for PostgresEventConsumer functionality,
// but these tests have been removed as they were redundant with comprehensive
// integration tests in integration_test/postgres_consumer_integration_test.go.
//
// The integration tests provide much better coverage by testing with real database
// connections and actual functionality, including:
// - Polling interval configuration and timing verification
// - Subscription lifecycle (create, use, close) with real databases
// - Channel access patterns through actual usage scenarios
// - Constructor behavior through integration test setup
//
// For testing PostgreSQL consumer functionality, see:
// integration_test/postgres_consumer_integration_test.go
