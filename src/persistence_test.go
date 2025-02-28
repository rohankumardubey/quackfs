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
	// Fail the test if PostgreSQL connection string is not available
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}

	// Use test database connection string from environment variable
	// Format: postgres://username:password@localhost/testdb?sslmode=disable

	// Create a new metadata store
	ms, err := NewMetadataStore(connStr)
	if err != nil {
		t.Fatalf("Failed to create MetadataStore: %v", err)
	}

	// Create a new LayerManager with the metadata store
	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	// Write some data and seal a layer
	data1 := []byte("persistent data 1")
	_, offset1, err := lm.Write(data1, 0)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if offset1 != 0 {
		t.Fatalf("Expected first write offset to be 0, got %d", offset1)
	}
	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("Failed to seal layer: %v", err)
	}

	// Write more data in new active layer
	data2 := []byte("persistent data 2")
	_, offset2, err := lm.Write(data2, offset1+uint64(len(data1)))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	expectedOffset2 := uint64(len(data1))
	if offset2 != expectedOffset2 {
		t.Fatalf("Expected second write offset to be %d, got %d", expectedOffset2, offset2)
	}

	// Close the metadata store to simulate a shutdown
	if err := ms.Close(); err != nil {
		t.Fatalf("Failed to close MetadataStore: %v", err)
	}

	// "Restart" the system by reopening the metadata store
	ms2, err := NewMetadataStore(connStr)
	if err != nil {
		t.Fatalf("Failed to reopen MetadataStore: %v", err)
	}
	defer ms2.Close()

	// Reload the layer manager from the metadata store
	lm2, err := NewLayerManager(ms2)
	if err != nil {
		t.Fatalf("Failed to reload LayerManager: %v", err)
	}

	// Check that we have at least 2 layers
	if len(lm2.layers) < 2 {
		t.Fatalf("Expected at least 2 layers after reload, got %d", len(lm2.layers))
	}

	// Verify that old data is still present
	read1, err := lm2.GetDataRange(offset1, uint64(len(data1)))
	if err != nil {
		t.Fatalf("Error reading data1 after reload: %v", err)
	}
	if !bytes.Equal(read1, data1) {
		t.Fatalf("Expected data1 %q, got %q", data1, read1)
	}

	// Verify that new data is present
	read2, err := lm2.GetDataRange(offset2, uint64(len(data2)))
	if err != nil {
		t.Fatalf("Error reading data2 after reload: %v", err)
	}
	if !bytes.Equal(read2, data2) {
		t.Fatalf("Expected data2 %q, got %q", data2, read2)
	}

	// Cleanup - For PostgreSQL, we should clean up the test tables
	_, _ = ms2.db.Exec("DELETE FROM entries")
	_, _ = ms2.db.Exec("DELETE FROM layers")
}

func TestFuseScenario(t *testing.T) {
	// Fail the test if PostgreSQL connection info is not available
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}

	// Create a temporary mount directory
	mountDir, err := os.MkdirTemp("", "fusemnt")
	if err != nil {
		t.Fatalf("TempDir error: %v", err)
	}
	defer os.RemoveAll(mountDir)

	// Create a PostgreSQL metadata store for this test
	msTest, err := NewMetadataStore(connStr)
	if err != nil {
		t.Fatalf("Failed to create test MetadataStore: %v", err)
	}

	// Ensure tables are clean before the test
	_, _ = msTest.db.Exec("DELETE FROM entries")
	_, _ = msTest.db.Exec("DELETE FROM layers")

	lmTest, err := NewLayerManager(msTest)
	if err != nil {
		t.Fatalf("Failed to create test LayerManager: %v", err)
	}

	// Override the global layer manager used by the FUSE filesystem
	globalLM = lmTest

	// Ensure cleanup
	defer func() {
		_, _ = msTest.db.Exec("DELETE FROM entries")
		_, _ = msTest.db.Exec("DELETE FROM layers")
		msTest.Close()
	}()

	// Mount the FUSE filesystem in process
	conn, err := fuse.Mount(mountDir, fuse.FSName("myfusefs"), fuse.Subtype("myfusefs"))
	if err != nil {
		t.Fatalf("Failed to mount FUSE: %v", err)
	}
	defer fuse.Unmount(mountDir)

	// Assume NewFS returns your FUSE filesystem implementation (which uses globalLM)
	fsImpl := FS{}
	// Serve the filesystem in a separate goroutine
	go func() {
		err := fs.Serve(conn, fsImpl)
		if err != nil {
			t.Errorf("FUSE serve error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	// The file should be available at mountDir/dummy.txt
	filePath := filepath.Join(mountDir, "dummy.txt")

	// Open the file in append mode
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filePath, err)
	}
	defer f.Close()

	// Helper to read file content
	readContent := func() []byte {
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Failed to read file %s: %v", filePath, err)
		}
		return data
	}

	// Write "hello\n" to the file
	if _, err := f.WriteString("hello\n"); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	f.Sync()

	expected1 := []byte("hello\n")
	if got := readContent(); !bytes.Equal(got, expected1) {
		t.Fatalf("After first write, expected %q, got %q", expected1, got)
	}

	// Write "hello world\n" to the file
	if _, err := f.WriteString("hello world\n"); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	f.Sync()
	time.Sleep(500 * time.Millisecond)
	expected2 := []byte("hello\nhello world\n")
	if got := readContent(); !bytes.Equal(got, expected2) {
		t.Fatalf("After second write, expected %q, got %q", expected2, got)
	}

	// Write "hel\n" to the file
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

func TestWritingAtDifferentOffsets(t *testing.T) {
	// Fail the test if PostgreSQL connection string is not available
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}

	// Create a new metadata store
	ms, err := NewMetadataStore(connStr)
	if err != nil {
		t.Fatalf("Failed to create MetadataStore: %v", err)
	}

	// Clean up tables at the start and ensure cleanup when test finishes
	_, _ = ms.db.Exec("DELETE FROM entries")
	_, _ = ms.db.Exec("DELETE FROM layers")
	defer func() {
		_, _ = ms.db.Exec("DELETE FROM entries")
		_, _ = ms.db.Exec("DELETE FROM layers")
		ms.Close()
	}()

	// Create a new LayerManager with the metadata store
	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	// Write a string containing 10 '#' characters and seal the layer
	data := []byte("##########")
	_, offset, err := lm.Write(data, 0)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if offset != 0 {
		t.Fatalf("Expected first write offset to be 0, got %d", offset)
	}
	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("Failed to seal layer: %v", err)
	}

	// Verify base layer content after seal
	baseContent, err := lm.GetDataRange(0, uint64(len(data)))
	if err != nil {
		t.Fatalf("Error reading base layer content: %v", err)
	}
	if !bytes.Equal(baseContent, data) {
		t.Fatalf("Base layer content mismatch, expected %q, got %q", data, baseContent)
	}

	// Now write modifications on a new active layer:
	// Write "@@@" starting at offset 2.
	mod1 := []byte("@@@")
	_, modOffset1, err := lm.Write(mod1, 2)
	if err != nil {
		t.Fatalf("Write error at offset 2: %v", err)
	}
	if modOffset1 != 2 {
		t.Fatalf("Expected write offset for '@@@' to be 2, got %d", modOffset1)
	}

	// Check content after first mod
	interContent, err := lm.GetDataRange(0, 10)
	if err != nil {
		t.Fatalf("Error reading content after first mod: %v", err)
	}
	expectedInter := []byte("##@@@#####")
	if !bytes.Equal(interContent, expectedInter) {
		t.Fatalf("After first mod, expected %q, got %q", expectedInter, interContent)
	}

	// Write "***" starting at offset 7.
	mod2 := []byte("***")
	_, modOffset2, err := lm.Write(mod2, 7)
	if err != nil {
		t.Fatalf("Write error at offset 7: %v", err)
	}
	if modOffset2 != 7 {
		t.Fatalf("Expected write offset for '***' to be 7, got %d", modOffset2)
	}

	// Check content before sealing
	interContent2, err := lm.GetDataRange(0, 10)
	if err != nil {
		t.Fatalf("Error reading content after second mod: %v", err)
	}
	expectedInter2 := []byte("##@@@##***")
	if !bytes.Equal(interContent2, expectedInter2) {
		t.Fatalf("After second mod, expected %q, got %q", expectedInter2, interContent2)
	}

	// Seal the new layer where modifications were made
	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("Failed to seal new layer: %v", err)
	}

	// The virtual file is an overlay:
	// Original content: "##########"
	// After modifications:
	// Offsets 0-1: "##" (unchanged)
	// Offsets 2-4: "@@@" (modified)
	// Offsets 5-6: "##" (unchanged)
	// Offsets 7-9: "***" (modified)
	// Final expected content: "##@@@##***"
	expectedVirtual := []byte("##@@@##***")
	actual, err := lm.GetDataRange(0, uint64(len(expectedVirtual)))
	if err != nil {
		t.Fatalf("Error reading new virtual content: %v", err)
	}
	if !bytes.Equal(actual, expectedVirtual) {
		t.Fatalf("Expected virtual file content %q, got %q", expectedVirtual, actual)
	}

	// Seal the last layer if it isn't already sealed
	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("Failed to seal last layer: %v", err)
	}

	// Write "aa" at virtual file offset 0
	mod3 := []byte("aa")
	_, modOffset3, err := lm.Write(mod3, 0)
	if err != nil {
		t.Fatalf("Write error at offset 0: %v", err)
	}
	if modOffset3 != 0 {
		t.Fatalf("Expected write offset for 'aa' to be 0, got %d", modOffset3)
	}

	// Check final content
	finalContent, err := lm.GetDataRange(0, 10)
	if err != nil {
		t.Fatalf("Error reading final content: %v", err)
	}
	expectedFinal := []byte("aa@@@##***")
	if !bytes.Equal(finalContent, expectedFinal) {
		t.Fatalf("After final write, expected %q, got %q", expectedFinal, finalContent)
	}
}
