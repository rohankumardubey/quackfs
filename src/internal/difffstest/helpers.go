package difffstest

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/vinimdocarmo/difffs/src/internal/logger"
	"github.com/vinimdocarmo/difffs/src/internal/storage"
)

func SetupStorageManager(t *testing.T) (*storage.Manager, func()) {
	connStr := GetTestConnectionString(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	// Create a test log
	log := logger.New(os.Stderr)

	sm := storage.NewManager(db, log)

	cleanup := func() {
		// delete all rows in all tables
		_, err = db.Exec("DELETE FROM chunks")
		if err != nil {
			t.Fatalf("Failed to clean chunks table: %v", err)
		}
		_, err = db.Exec("DELETE FROM snapshot_layers")
		if err != nil {
			t.Fatalf("Failed to clean snapshot_layers table: %v", err)
		}
		_, err = db.Exec("DELETE FROM files")
		if err != nil {
			t.Fatalf("Failed to clean files table: %v", err)
		}
		sm.Close()
	}

	return sm, cleanup
}

// GetTestConnectionString returns the PostgreSQL connection string for tests
func GetTestConnectionString(t *testing.T) string {
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}
	return connStr
}

func SetupDB(t *testing.T) *sql.DB {
	connStr := GetTestConnectionString(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	return db
}
