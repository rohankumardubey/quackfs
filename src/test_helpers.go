package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// SetupMetadataStore creates a metadata store for testing with PostgreSQL and returns
// the store along with a cleanup function
func SetupMetadataStore(t *testing.T) (*MetadataStore, func()) {
	connStr := GetTestConnectionString(t)

	ms, err := NewMetadataStore(connStr)
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL metadata store: %v", err)
	}

	// Reset the testing environment - clean up existing test data completely
	// Delete entries first to respect foreign key constraints
	_, err = ms.db.Exec("DELETE FROM entries")
	if err != nil {
		t.Fatalf("Failed to clean entries table: %v", err)
	}

	// Delete layers
	_, err = ms.db.Exec("DELETE FROM layers")
	if err != nil {
		t.Fatalf("Failed to clean layers table: %v", err)
	}

	// Delete files
	_, err = ms.db.Exec("DELETE FROM files")
	if err != nil {
		t.Fatalf("Failed to clean files table: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		_, _ = ms.db.Exec("DELETE FROM entries")
		_, _ = ms.db.Exec("DELETE FROM layers")
		_, _ = ms.db.Exec("DELETE FROM files")
		ms.Close()
	}

	return ms, cleanup
}

// SetupLayerManager creates a new layer manager backed by PostgreSQL for testing
func SetupLayerManager(t *testing.T) (*LayerManager, func()) {
	ms, msCleanup := SetupMetadataStore(t)

	lm, err := NewLayerManager(ms)
	if err != nil {
		msCleanup()
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	cleanup := func() {
		msCleanup()
	}

	return lm, cleanup
}

// SetupFuseMount creates a temporary mount directory and mounts a FUSE filesystem
// It returns the mountpoint, a cleanup function, and an error status channel
func SetupFuseMount(t *testing.T) (string, func(), chan error) {
	// Create a temporary mount directory
	mountDir, err := os.MkdirTemp("", "fusemnt")
	if err != nil {
		t.Fatalf("TempDir error: %v", err)
	}

	// Setup the layer manager
	lm, lmCleanup := SetupLayerManager(t)

	// Override the global layer manager used by the FUSE filesystem
	globalLM = lm

	// Setup error channel to monitor mount process
	errChan := make(chan error, 1)

	// Mount the FUSE filesystem
	conn, err := fuse.Mount(mountDir, fuse.FSName("myfusefs"), fuse.Subtype("myfusefs"))
	if err != nil {
		os.RemoveAll(mountDir)
		lmCleanup()
		t.Fatalf("Failed to mount FUSE: %v", err)
	}

	// Serve the filesystem in a goroutine
	go func() {
		errChan <- fs.Serve(conn, FS{})
	}()

	// Create cleanup function
	cleanup := func() {
		fuse.Unmount(mountDir)
		conn.Close()
		os.RemoveAll(mountDir)
		lmCleanup()
	}

	// Test the mount point by trying to access it
	WaitForMount(mountDir, t)

	return mountDir, cleanup, errChan
}

// WaitForMount attempts to create a file in the mount directory to verify mount is ready
func WaitForMount(mountDir string, t *testing.T) {
	const attempts = 10
	for range attempts {
		testFile := filepath.Join(mountDir, "test_mount_ready")
		f, err := os.Create(testFile)
		if err == nil {
			f.Close()
			os.Remove(testFile)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Mount point %s does not appear to be ready after %d attempts", mountDir, attempts)
}

// GetTestConnectionString returns the PostgreSQL connection string for tests
func GetTestConnectionString(t *testing.T) string {
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}
	return connStr
}
