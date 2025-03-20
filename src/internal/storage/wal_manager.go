package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
)

// DBCheckpointer is an interface that defines the methods needed by WALManager
// to checkpoint a database file
type DBCheckpointer interface {
	Checkpoint(filename string, version string) error
}

// WALManager handles operations for DuckDB WAL (Write-Ahead Log) files.
// It provides functionality to read, write, and manage WAL files on the filesystem.
type WALManager struct {
	walPath string         // Path where WAL files are stored
	log     *log.Logger    // Logger for WAL operations
	sm      DBCheckpointer // Reference to the storage manager for checkpointing
	mu      sync.RWMutex   // Mutex to protect concurrent operations
}

// NewWALManager creates a new WAL manager instance.
func NewWALManager(walPath string, sm DBCheckpointer, logger *log.Logger) *WALManager {
	walLog := logger.With()
	walLog.SetPrefix("📝 WALManager")

	return &WALManager{
		walPath: walPath,
		log:     walLog,
		sm:      sm,
	}
}

// IsWALFile checks if a file has the .duckdb.wal extension
func IsWALFile(filename string) bool {
	return strings.HasSuffix(filename, ".duckdb.wal")
}

// GetDBFilename returns the database filename without the .wal extension
func (wm *WALManager) GetDBFilename(walFilename string) string {
	return strings.TrimSuffix(walFilename, ".wal")
}

// GetFilePath returns the full path to the WAL file in the filesystem
func (wm *WALManager) GetFilePath(filename string) string {
	return filepath.Join(wm.walPath, filename)
}

// GetFileSize returns the size of a WAL file in bytes
func (wm *WALManager) GetFileSize(filename string) (uint64, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	filePath := wm.GetFilePath(filename)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	return uint64(fileInfo.Size()), nil
}

// GetModTime returns the modification time of a WAL file
func (wm *WALManager) GetModTime(filename string) (time.Time, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	filePath := wm.GetFilePath(filename)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}

	return fileInfo.ModTime(), nil
}

// Create creates a new WAL file
func (wm *WALManager) Create(filename string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !IsWALFile(filename) {
		return fmt.Errorf("invalid WAL file name: %s", filename)
	}

	filePath := wm.GetFilePath(filename)

	// Create the directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory for WAL file: %w", err)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}
	defer file.Close()

	wm.log.Debug("Created WAL file", "filename", filename)
	return nil
}

// Exists checks if a WAL file exists
func (wm *WALManager) Exists(filename string) (bool, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	filePath := wm.GetFilePath(filename)

	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// ListWALFiles returns a list of all WAL files in the WAL directory
func (wm *WALManager) ListWALFiles() ([]string, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	// Ensure the directory exists
	if err := os.MkdirAll(wm.walPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to ensure WAL directory exists: %w", err)
	}

	entries, err := os.ReadDir(wm.walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var walFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && IsWALFile(entry.Name()) {
			walFiles = append(walFiles, entry.Name())
		}
	}

	return walFiles, nil
}

// Read reads data from a WAL file at the specified offset and size
func (wm *WALManager) Read(filename string, offset uint64, size uint64) ([]byte, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	if !IsWALFile(filename) {
		return nil, fmt.Errorf("invalid WAL file name: %s", filename)
	}

	filePath := wm.GetFilePath(filename)

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// If the file does not exist yet, return empty data
			return []byte{}, nil
		}
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	// Create a buffer to hold the data
	data := make([]byte, size)

	// Seek to the offset
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek in WAL file: %w", err)
	}

	// Read the data
	n, err := file.Read(data)
	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read from WAL file: %w", err)
	}

	wm.log.Debug("Read from WAL file", "filename", filename, "offset", offset, "bytesRead", n)
	return data[:n], nil
}

// Write writes data to a WAL file at the specified offset
func (wm *WALManager) Write(filename string, data []byte, offset uint64) (int, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !IsWALFile(filename) {
		return 0, fmt.Errorf("invalid WAL file name: %s", filename)
	}

	filePath := wm.GetFilePath(filename)

	// Open the file with write permissions, create if it doesn't exist
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open WAL file for writing: %w", err)
	}
	defer file.Close()

	// Seek to the offset
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return 0, fmt.Errorf("failed to seek in WAL file: %w", err)
	}

	// Write the data
	n, err := file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write to WAL file: %w", err)
	}

	wm.log.Debug("Wrote to WAL file", "filename", filename, "offset", offset, "bytesWritten", n)
	return n, nil
}

// Remove removes a WAL file and checkpoints the associated database
func (wm *WALManager) Remove(filename string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !IsWALFile(filename) {
		return fmt.Errorf("invalid WAL file name: %s", filename)
	}

	// Generate a checkpoint ID
	checkpointID := uuid.New().String()

	// Get the database filename without .wal extension
	dbFilename := wm.GetDBFilename(filename)

	// Checkpoint the database file in the storage manager
	wm.log.Info("Removing WAL file, checkpointing database",
		"walFile", filename,
		"dbFile", dbFilename,
		"checkpointID", checkpointID)

	if err := wm.sm.Checkpoint(dbFilename, checkpointID); err != nil {
		return fmt.Errorf("failed to checkpoint database: %w", err)
	}

	// Remove the WAL file
	filePath := wm.GetFilePath(filename)
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to remove WAL file: %w", err)
	}

	wm.log.Info("WAL file removed successfully", "filename", filename)
	return nil
}

// Sync flushes any buffered data to disk
func (wm *WALManager) Sync(filename string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !IsWALFile(filename) {
		return fmt.Errorf("invalid WAL file name: %s", filename)
	}

	filePath := wm.GetFilePath(filename)

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file for syncing: %w", err)
	}
	defer file.Close()

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	wm.log.Debug("Synced WAL file", "filename", filename)
	return nil
}
