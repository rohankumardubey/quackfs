package difffstest

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/vinimdocarmo/difffs/src/storage"
)

func SetupStorageManager(t *testing.T) (*storage.Manager, func()) {
	connStr := GetTestConnectionString(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	sm, err := storage.NewManager(db)
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL storage manager: %v", err)
	}

	cleanup := func() {
		// delete all rows in all tables
		_, err = db.Exec("DELETE FROM entries")
		if err != nil {
			t.Fatalf("Failed to clean entries table: %v", err)
		}
		_, err = db.Exec("DELETE FROM layers")
		if err != nil {
			t.Fatalf("Failed to clean layers table: %v", err)
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
