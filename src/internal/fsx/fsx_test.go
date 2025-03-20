package fsx

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/charmbracelet/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vinimdocarmo/quackfs/src/internal/logger"
	"github.com/vinimdocarmo/quackfs/src/internal/quackfstest"
	"github.com/vinimdocarmo/quackfs/src/internal/storage"
)

func TestFuseReadWrite(t *testing.T) {
	// Create and mount the FUSE filesystem using the in-process approach
	mountDir, _, cleanup, errChan := setupFuseMount(t)
	defer cleanup()

	// Create the file first to ensure it exists
	filePath := filepath.Join(mountDir, "test_file.duckdb")
	createFile, err := os.Create(filePath)
	require.NoError(t, err, "Failed to create test file")
	require.NoError(t, createFile.Close(), "Failed to close file after creation")

	// Now open the file for read/write.
	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	require.NoError(t, err, "Failed to open file %s", filePath)

	// Write some data.
	dataToWrite := []byte("test data")
	n, err := f.Write(dataToWrite)
	require.NoError(t, err, "Failed to write data")
	assert.Equal(t, len(dataToWrite), n, "Write should write all bytes")

	// Seek back to the beginning.
	_, err = f.Seek(0, 0)
	require.NoError(t, err, "Seek error")

	// Read the data back.
	readBuf := make([]byte, len(dataToWrite))
	n, err = f.Read(readBuf)
	require.NoError(t, err, "Failed to read data")
	assert.Equal(t, len(dataToWrite), n, "Read should return all bytes")
	assert.Equal(t, dataToWrite, readBuf, "Read data should match written data")

	// Close the file handle to ensure it's not busy.
	require.NoError(t, f.Close(), "Failed to close file")

	// Check for any errors from the FUSE server
	select {
	case err := <-errChan:
		require.NoError(t, err, "FUSE server reported an error")
	default:
		// No error, which is good
	}
}

// WaitForMount attempts to create a file in the mount directory to verify mount is ready
func waitForMount(mountDir string, t *testing.T) {
	const attempts = 10
	for range attempts {
		testFile := filepath.Join(mountDir, "test_mount_ready.duckdb")
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

// SetupFuseMount creates a temporary mount directory and mounts a FUSE filesystem
// It returns the mountpoint, a cleanup function, and an error status channel
func setupFuseMount(t *testing.T) (string, *storage.Manager, func(), chan error) {
	// Create a temporary mount directory
	mountDir, err := os.MkdirTemp("", "fusemnt")
	if err != nil {
		t.Fatalf("TempDir error: %v", err)
	}

	sm, smCleanup := quackfstest.SetupStorageManager(t)

	// Create a test log
	log := logger.New(os.Stderr)

	// Setup error channel to monitor mount process
	errChan := make(chan error, 1)

	// Mount the FUSE filesystem
	conn, err := fuse.Mount(mountDir, fuse.FSName("myfusefs"), fuse.Subtype("myfusefs"))
	if err != nil {
		os.RemoveAll(mountDir)
		t.Fatalf("Failed to mount FUSE: %v", err)
	}

	// Serve the filesystem in a goroutine
	go func() {
		errChan <- fs.Serve(conn, NewFS(sm, log, "/tmp"))
	}()

	// Create cleanup function
	cleanup := func() {
		fuse.Unmount(mountDir)
		conn.Close()
		os.RemoveAll(mountDir)
		smCleanup()
	}

	// Test the mount point by trying to access it
	waitForMount(mountDir, t)

	return mountDir, sm, cleanup, errChan
}

// TestWriteBeyondFileSize tests writing to an offset beyond the current file size
func TestWriteBeyondFileSize(t *testing.T) {
	// Set up test environment
	sm, log, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a test file
	filename := "test_write_beyond.duckdb"
	initialContent := []byte("initial")
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err)
	require.NotZero(t, fileID)

	// Write initial content at offset 0
	err = sm.WriteFile(filename, initialContent, 0)
	require.NoError(t, err)

	// Create a FUSE file instance
	file := &File{
		name:     filename,
		created:  time.Now(),
		modified: time.Now(),
		accessed: time.Now(),
		fileSize: uint64(len(initialContent)),
		sm:       sm,
		log:      log,
	}

	// Write data at an offset beyond the current file size
	beyondOffset := int64(20)
	beyondData := []byte("beyond data")

	// Create a write request
	req := &fuse.WriteRequest{
		Data:   beyondData,
		Offset: beyondOffset,
	}
	resp := &fuse.WriteResponse{}

	// Perform the write
	err = file.Write(context.Background(), req, resp)
	require.NoError(t, err)
	require.Equal(t, len(beyondData), resp.Size)

	fileSize, err := sm.SizeOf(filename)
	require.NoError(t, err)

	// Read the entire file to verify the content
	data, err := sm.ReadFile(filename, 0, fileSize)
	require.NoError(t, err)

	// Verify the file size
	expectedSize := uint64(beyondOffset) + uint64(len(beyondData))
	require.Equal(t, expectedSize, uint64(len(data)))

	// Verify initial content is preserved
	require.Equal(t, initialContent, data[:len(initialContent)])

	// Verify the gap is filled with zeroes
	for i := len(initialContent); i < int(beyondOffset); i++ {
		require.Equal(t, byte(0), data[i], "Expected zero at position %d", i)
	}

	// Verify the beyond data is written correctly
	require.Equal(t, beyondData, data[beyondOffset:beyondOffset+int64(len(beyondData))])
}

func TestFileEmptyWriteNonZeroOffset(t *testing.T) {
	// Set up test environment
	sm, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a test file
	filename := "test_empty_write.duckdb"
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err)
	require.NotZero(t, fileID)

	// Write empty data at offset 10
	err = sm.WriteFile(filename, []byte("hello"), 10)
	require.NoError(t, err)

	// Read the file to verify the content
	data, err := sm.ReadFile(filename, 0, 15)
	require.NoError(t, err)

	require.Equal(t, "hello", string(data[10:]))
	require.Equal(t, "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00hello", string(data))
}

// setupTestEnvironment creates a storage manager and logger for testing
func setupTestEnvironment(t *testing.T) (*storage.Manager, *log.Logger, func()) {
	sm, smCleanup := quackfstest.SetupStorageManager(t)
	log := logger.New(os.Stderr)

	cleanup := func() {
		smCleanup()
	}

	return sm, log, cleanup
}

// TestStorageCheckpointOnDuckDBCheckpoint tests removal of .duckdb.wal files with checkpointing
func TestStorageCheckpointOnDuckDBCheckpoint(t *testing.T) {
	// Create and mount the FUSE filesystem
	mountDir, _, cleanup, errChan := setupFuseMount(t)
	defer cleanup()

	// Create a DuckDB database file path in the mounted filesystem
	dbFile := "test_db.duckdb"
	dbFilePath := filepath.Join(mountDir, dbFile)

	// Create a simple table and insert some data to generate WAL file
	createTableCmd := exec.Command("duckdb", dbFilePath, "-c",
		`
		PRAGMA disable_checkpoint_on_shutdown;
		CREATE TABLE test_table(id INTEGER, value VARCHAR);
		INSERT INTO test_table VALUES (1, 'test value 1'), (2, 'test value 2');
		`)
	output, err := createTableCmd.CombinedOutput()
	require.NoError(t, err, "Failed to create table: %s", string(output))

	// The WAL file should now exist
	walFile := dbFile + ".wal"
	walFilePath := filepath.Join(mountDir, walFile)
	_, err = os.Stat(walFilePath)
	require.NoError(t, err, "WAL file should exist after database operations")

	// Now run a CHECKPOINT command that should flush the WAL to the database and delelte it
	checkpointCmd := exec.Command("duckdb", dbFilePath, "-c", "CHECKPOINT;")
	output, err = checkpointCmd.CombinedOutput()
	require.NoError(t, err, "Failed to checkpoint: %s", string(output))

	// WAL file should not exist anymore
	_, err = os.Stat(walFilePath)
	require.Error(t, err, "WAL file should not exist after checkpoint")
	require.True(t, os.IsNotExist(err), "Error should indicate WAL file does not exist")

	// Now try to read from the database to verify it's still functional
	// The data should have been properly saved during checkpoint
	queryCmd := exec.Command("duckdb", dbFilePath, "-c", "SELECT * FROM test_table;")
	output, err = queryCmd.CombinedOutput()
	require.NoError(t, err, "Failed to query database: %s", string(output))

	// Verify the output contains our test value
	require.Contains(t, string(output), "test value 1", "Database should contain our test data")
	require.Contains(t, string(output), "test value 2", "Database should contain our test data")

	// Check for any errors from the FUSE server
	select {
	case err := <-errChan:
		require.NoError(t, err, "FUSE server reported an error")
	default:
		// No error, which is good
	}
}
