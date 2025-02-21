package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

func TestLayerManagerPersistence(t *testing.T) {
	// Create a temporary directory for the SQLite database.
	tempDir := os.TempDir()
	dbPath := filepath.Join(tempDir, "test_metadata.sqlite")
	// Ensure cleanup.
	defer os.Remove(dbPath)

	// Create a new metadata store.
	ms, err := NewMetadataStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create MetadataStore: %v", err)
	}
	// Create a new LayerManager with the metadata store.
	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	// Write some data and seal a layer.
	data1 := []byte("persistent data 1")
	_, offset1 := lm.Write(data1)
	if offset1 != 0 {
		t.Fatalf("Expected first write offset to be 0, got %d", offset1)
	}
	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("Failed to seal layer: %v", err)
	}

	// Write more data in new active layer.
	data2 := []byte("persistent data 2")
	_, offset2 := lm.Write(data2)
	expectedOffset2 := int64(len(data1))
	if offset2 != expectedOffset2 {
		t.Fatalf("Expected second write offset to be %d, got %d", expectedOffset2, offset2)
	}

	// Close the metadata store to simulate a shutdown.
	if err := ms.Close(); err != nil {
		t.Fatalf("Failed to close MetadataStore: %v", err)
	}

	// "Restart" the system by reopening the metadata store.
	ms2, err := NewMetadataStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen MetadataStore: %v", err)
	}
	defer ms2.Close()

	// Reload the layer manager from the metadata store.
	lm2, err := NewLayerManager(ms2)
	if err != nil {
		t.Fatalf("Failed to reload LayerManager: %v", err)
	}

	// Check that we have at least 2 layers.
	if len(lm2.layers) < 2 {
		t.Fatalf("Expected at least 2 layers after reload, got %d", len(lm2.layers))
	}

	// Verify that old data is still present.
	read1, err := lm2.GetDataRange(offset1, len(data1))
	if err != nil {
		t.Fatalf("Error reading data1 after reload: %v", err)
	}
	if !bytes.Equal(read1, data1) {
		t.Fatalf("Expected data1 %q, got %q", data1, read1)
	}

	// Verify that new data is present.
	read2, err := lm2.GetDataRange(offset2, len(data2))
	if err != nil {
		t.Fatalf("Error reading data2 after reload: %v", err)
	}
	if !bytes.Equal(read2, data2) {
		t.Fatalf("Expected data2 %q, got %q", data2, read2)
	}
}

func TestFuseScenario(t *testing.T) {
	// Create a temporary mount directory.
	mountDir, err := os.MkdirTemp("", "fusemnt")
	if err != nil {
		t.Fatalf("TempDir error: %v", err)
	}
	defer os.RemoveAll(mountDir)

	// Create a temporary SQLite database for this test.
	tempDB := filepath.Join(os.TempDir(), "test_metadata_fuse.sqlite")
	// Ensure the temp db does not exist.
	os.Remove(tempDB)
	msTest, err := NewMetadataStore(tempDB)
	if err != nil {
		t.Fatalf("Failed to create test MetadataStore: %v", err)
	}
	lmTest, err := NewLayerManager(msTest)
	if err != nil {
		t.Fatalf("Failed to create test LayerManager: %v", err)
	}
	// Override the global layer manager used by the FUSE filesystem.
	globalLM = lmTest
	// Ensure cleanup.
	defer func() {
		msTest.Close()
		os.Remove(tempDB)
	}()

	// Mount the FUSE filesystem in process.
	conn, err := fuse.Mount(mountDir, fuse.FSName("myfusefs"), fuse.Subtype("myfusefs"))
	if err != nil {
		t.Fatalf("Failed to mount FUSE: %v", err)
	}
	defer fuse.Unmount(mountDir)

	// Assume NewFS returns your FUSE filesystem implementation (which uses globalLM).
	fsImpl := FS{}
	// Serve the filesystem in a separate goroutine.
	go func() {
		err := fs.Serve(conn, fsImpl)
		if err != nil {
			t.Errorf("FUSE serve error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	// The file should be available at mountDir/dummy.txt.
	filePath := filepath.Join(mountDir, "dummy.txt")

	// Open the file in append mode.
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filePath, err)
	}
	defer f.Close()

	// Helper to read file content.
	readContent := func() []byte {
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Failed to read file %s: %v", filePath, err)
		}
		return data
	}

	// Write "hello\n" to the file.
	if _, err := f.WriteString("hello\n"); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	f.Sync()

	expected1 := []byte("hello\n")
	if got := readContent(); !bytes.Equal(got, expected1) {
		t.Fatalf("After first write, expected %q, got %q", expected1, got)
	}

	// Write "hello world\n" to the file.
	if _, err := f.WriteString("hello world\n"); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	f.Sync()
	time.Sleep(500 * time.Millisecond)
	expected2 := []byte("hello\nhello world\n")
	if got := readContent(); !bytes.Equal(got, expected2) {
		t.Fatalf("After second write, expected %q, got %q", expected2, got)
	}

	// Write "hel\n" to the file.
	if _, err := f.WriteString("hel\n"); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	f.Sync()
	time.Sleep(500 * time.Millisecond)
	expectedFinal := []byte("hello\nhello world\nhel\n")
	if got := readContent(); !bytes.Equal(got, expectedFinal) {
		t.Fatalf("After third write, expected %q, got %q", expectedFinal, got)
	}
}
