package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Verify that Manager implements StorageManagerCheckpointer
var _ DBCheckpointer = (*Manager)(nil)

// For testing purposes, we'll use a simple struct that just implements the methods we need
type mockStorageManager struct {
	checkpointFn func(filename, version string) error
}

func (m *mockStorageManager) Checkpoint(filename string, version string) error {
	if m.checkpointFn != nil {
		return m.checkpointFn(filename, version)
	}
	return nil
}

func TestIsWALFile(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     bool
	}{
		{"Valid WAL file", "test.duckdb.wal", true},
		{"Not a WAL file", "test.duckdb", false},
		{"Wrong extension", "test.wal", false},
		{"Empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsWALFile(tt.filename)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetDBFilename(t *testing.T) {
	// Setup
	logger := log.NewWithOptions(os.Stderr, log.Options{Level: log.FatalLevel})
	mockSM := &mockStorageManager{}
	wm := NewWALManager("/tmp", mockSM, logger)

	// Test cases
	tests := []struct {
		name       string
		walFile    string
		wantDBFile string
	}{
		{"Simple WAL file", "test.duckdb.wal", "test.duckdb"},
		{"Already missing .wal", "test.duckdb", "test.duckdb"},
		{"Empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wm.GetDBFilename(tt.walFile)
			assert.Equal(t, tt.wantDBFile, got)
		})
	}
}

func TestWALManagerBasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "walmanager_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Setup
	logger := log.NewWithOptions(os.Stderr, log.Options{Level: log.FatalLevel})
	mockSM := &mockStorageManager{}
	wm := NewWALManager(tmpDir, mockSM, logger)

	testFile := "test.duckdb.wal"
	testFilePath := filepath.Join(tmpDir, testFile)

	// Test Create
	t.Run("Create WAL file", func(t *testing.T) {
		err := wm.Create(testFile)
		require.NoError(t, err)

		// Verify file exists
		_, err = os.Stat(testFilePath)
		assert.NoError(t, err)
	})

	// Test invalid file creation
	t.Run("Create invalid WAL file", func(t *testing.T) {
		err := wm.Create("invalid.txt")
		assert.Error(t, err)
	})

	// Test Exists
	t.Run("Check if file exists", func(t *testing.T) {
		exists, err := wm.Exists(testFile)
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = wm.Exists("nonexistent.duckdb.wal")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	// Test GetFileSize
	t.Run("Get file size", func(t *testing.T) {
		size, err := wm.GetFileSize(testFile)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), size) // Empty file should have size 0
	})

	// Test GetModTime
	t.Run("Get modification time", func(t *testing.T) {
		modTime, err := wm.GetModTime(testFile)
		require.NoError(t, err)

		// ModTime should be recent
		fiveMinutesAgo := time.Now().Add(-5 * time.Minute)
		assert.True(t, modTime.After(fiveMinutesAgo))
	})

	// Test ListWALFiles
	t.Run("List WAL files", func(t *testing.T) {
		// Create another WAL file
		anotherFile := "another.duckdb.wal"
		err := wm.Create(anotherFile)
		require.NoError(t, err)

		// Also create a non-WAL file that should be ignored
		nonWalFile := filepath.Join(tmpDir, "not-a-wal.txt")
		f, err := os.Create(nonWalFile)
		require.NoError(t, err)
		f.Close()

		// List WAL files
		files, err := wm.ListWALFiles()
		require.NoError(t, err)
		assert.Len(t, files, 2)
		assert.Contains(t, files, testFile)
		assert.Contains(t, files, anotherFile)
		assert.NotContains(t, files, "not-a-wal.txt")
	})

	// Test Write and Read
	t.Run("Write and Read data", func(t *testing.T) {
		testData := []byte("Hello, WAL!")

		// Write data
		n, err := wm.Write(testFile, testData, 0)
		require.NoError(t, err)
		assert.Equal(t, len(testData), n)

		// Read data
		readData, err := wm.Read(testFile, 0, uint64(len(testData)))
		require.NoError(t, err)
		assert.Equal(t, testData, readData)

		// Read with offset
		readData, err = wm.Read(testFile, 7, 4)
		require.NoError(t, err)
		assert.Equal(t, []byte("WAL!"), readData)
	})

	// Test Sync
	t.Run("Sync file", func(t *testing.T) {
		err := wm.Sync(testFile)
		assert.NoError(t, err)
	})
}

func TestWALManagerRemove(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "walmanager_remove_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Setup
	logger := log.NewWithOptions(os.Stderr, log.Options{Level: log.FatalLevel})

	// Create a mock that tracks if Checkpoint was called
	checkpointCalled := false
	checkpointError := false
	mockSM := &mockStorageManager{
		checkpointFn: func(filename, version string) error {
			if filename == "test.duckdb" {
				checkpointCalled = true
				return nil
			}
			if filename == "error.duckdb" {
				checkpointError = true
				return assert.AnError
			}
			return nil
		},
	}

	wm := NewWALManager(tmpDir, mockSM, logger)

	testFile := "test.duckdb.wal"
	testFilePath := filepath.Join(tmpDir, testFile)

	// Create a test file
	err = wm.Create(testFile)
	require.NoError(t, err)

	// Test Remove
	t.Run("Remove WAL file", func(t *testing.T) {
		err := wm.Remove(testFile)
		require.NoError(t, err)

		// Verify file was removed
		_, err = os.Stat(testFilePath)
		assert.True(t, os.IsNotExist(err))

		// Verify Checkpoint was called
		assert.True(t, checkpointCalled, "Checkpoint method should have been called")
	})

	// Test removing invalid file
	t.Run("Remove invalid file", func(t *testing.T) {
		err := wm.Remove("invalid.txt")
		assert.Error(t, err)
	})

	// Test Checkpoint error
	t.Run("Checkpoint error", func(t *testing.T) {
		// Create a new file
		newFile := "error.duckdb.wal"
		err = wm.Create(newFile)
		require.NoError(t, err)

		// Try to remove
		err = wm.Remove(newFile)
		assert.Error(t, err)
		assert.True(t, checkpointError, "Checkpoint error handler should have been called")

		// File should still exist
		_, err = os.Stat(filepath.Join(tmpDir, newFile))
		assert.NoError(t, err)
	})
}

func TestWALManagerEdgeCases(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "walmanager_edge_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Setup
	logger := log.NewWithOptions(os.Stderr, log.Options{Level: log.FatalLevel})
	mockSM := &mockStorageManager{}
	wm := NewWALManager(tmpDir, mockSM, logger)

	// Test reading non-existent file
	t.Run("Read non-existent file", func(t *testing.T) {
		data, err := wm.Read("nonexistent.duckdb.wal", 0, 10)
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	// Test writing with invalid filename
	t.Run("Write with invalid filename", func(t *testing.T) {
		_, err := wm.Write("invalid.txt", []byte("test"), 0)
		assert.Error(t, err)
	})

	// Test writing with offset
	t.Run("Write with offset", func(t *testing.T) {
		testFile := "offset.duckdb.wal"

		// Create and write initial data
		err := wm.Create(testFile)
		require.NoError(t, err)

		_, err = wm.Write(testFile, []byte("Hello, "), 0)
		require.NoError(t, err)

		// Write at offset
		_, err = wm.Write(testFile, []byte("World!"), 7)
		require.NoError(t, err)

		// Read the entire content
		data, err := wm.Read(testFile, 0, 13)
		require.NoError(t, err)
		assert.Equal(t, []byte("Hello, World!"), data)
	})

	// Test concurrency by simulating multiple goroutines accessing the WALManager
	t.Run("Concurrent access", func(t *testing.T) {
		testFile := "concurrent.duckdb.wal"
		err := wm.Create(testFile)
		require.NoError(t, err)

		done := make(chan bool)
		iterations := 50

		// Start goroutines for concurrent reads and writes
		for i := 0; i < 5; i++ {
			go func(id int) {
				for j := 0; j < iterations; j++ {
					// Write data
					data := []byte(uuid.New().String())
					_, err := wm.Write(testFile, data, 0)
					require.NoError(t, err)

					// Read data
					_, err = wm.Read(testFile, 0, 10)
					require.NoError(t, err)
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < 5; i++ {
			<-done
		}

		// If we get here without panics or deadlocks, the test passes
		size, err := wm.GetFileSize(testFile)
		require.NoError(t, err)
		assert.Greater(t, size, uint64(0))
	})
}
