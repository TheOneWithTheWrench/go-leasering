package database

import (
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// TestingT is an interface for testing compatibility.
type TestingT interface {
	Logf(format string, args ...any)
	FailNow()
	Cleanup(func())
}

// SetupTestDatabase creates a test database with isolated schema.
func SetupTestDatabase(t TestingT) *sql.DB {
	var (
		id      = fmt.Sprintf("test_%s", uuid.New().String()[0:8])
		schema  = id
		connURL = "postgres://testuser:testpassword@localhost:5432/leasering_test_db?sslmode=disable"
	)

	// First, connect to create the schema
	conn, err := sql.Open("postgres", connURL)
	if err != nil {
		t.Logf("failed to connect to database. Is your local database running?: %v", err)
		t.FailNow()
	}

	_, err = conn.Exec("CREATE SCHEMA IF NOT EXISTS " + schema)
	if err != nil {
		t.Logf("Failed to create schema %s", schema)
		t.Logf("Error: %s", err)
		t.FailNow()
	}

	// Close the initial connection
	conn.Close()

	// Create a new connection with the schema in the connection string
	var connURLWithSchema = fmt.Sprintf("postgres://testuser:testpassword@localhost:5432/leasering_test_db?sslmode=disable&search_path=%s", schema)
	conn, err = sql.Open("postgres", connURLWithSchema)
	if err != nil {
		t.Logf("failed to connect to database with schema: %v", err)
		t.FailNow()
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}
