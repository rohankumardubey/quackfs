package fsx

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/charmbracelet/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vinimdocarmo/difffs/src/internal/difffstest"
	"github.com/vinimdocarmo/difffs/src/internal/logger"
	"github.com/vinimdocarmo/difffs/src/internal/storage"
)

func TestFuseReadWrite(t *testing.T) {
	// Create and mount the FUSE filesystem using the in-process approach
	mountDir, _, cleanup, errChan := setupFuseMount(t)
	defer cleanup()

	// Create the file first to ensure it exists
	filePath := filepath.Join(mountDir, "test_file.txt")
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

func TestFuseFileRemoval(t *testing.T) {
	// Create and mount the FUSE filesystem
	mountDir, sm, cleanup, errChan := setupFuseMount(t)
	defer cleanup()

	// Create a test file
	testFileName := "test_removal_file.txt"
	filePath := filepath.Join(mountDir, testFileName)

	// Create the file
	f, err := os.Create(filePath)
	require.NoError(t, err, "Failed to create test file")

	// Write some data to the file
	_, err = f.WriteString("This is a test file that will be removed")
	require.NoError(t, err, "Failed to write to test file")
	require.NoError(t, f.Close(), "Failed to close file")

	// Verify the file exists
	_, err = os.Stat(filePath)
	require.NoError(t, err, "File should exist before removal")

	// Remove the file
	err = os.Remove(filePath)
	require.NoError(t, err, "Failed to remove file")

	// Verify the file no longer exists
	_, err = os.Stat(filePath)
	require.Error(t, err, "File should not exist after removal")
	require.True(t, os.IsNotExist(err), "Error should indicate file does not exist")

	// Verify the file was removed from the database
	fileID, err := sm.GetFileIDByName(testFileName)
	require.NoError(t, err, "Error checking file ID")
	require.Equal(t, int64(0), fileID, "File ID should be 0 (not found) after removal")

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

// SetupFuseMount creates a temporary mount directory and mounts a FUSE filesystem
// It returns the mountpoint, a cleanup function, and an error status channel
func setupFuseMount(t *testing.T) (string, *storage.Manager, func(), chan error) {
	// Create a temporary mount directory
	mountDir, err := os.MkdirTemp("", "fusemnt")
	if err != nil {
		t.Fatalf("TempDir error: %v", err)
	}

	sm, smCleanup := difffstest.SetupStorageManager(t)

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
		errChan <- fs.Serve(conn, NewFS(sm, log))
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
	filename := "test_write_beyond.txt"
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

	fileSize, err := sm.FileSize(fileID)
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
	filename := "test_empty_write.txt"
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
	sm, smCleanup := difffstest.SetupStorageManager(t)
	log := logger.New(os.Stderr)

	cleanup := func() {
		smCleanup()
	}

	return sm, log, cleanup
}
