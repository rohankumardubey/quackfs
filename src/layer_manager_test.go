package main

import (
	"bytes"
	"os"
	"testing"
)

// createTestMetadataStore creates a metadata store for testing using PostgreSQL
func createTestMetadataStore(t *testing.T) (*MetadataStore, func()) {
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}

	ms, err := NewMetadataStore(connStr)
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL metadata store: %v", err)
	}

	// Clean up existing test data
	_, _ = ms.db.Exec("DELETE FROM entries")
	_, _ = ms.db.Exec("DELETE FROM layers")

	// Return cleanup function
	cleanup := func() {
		_, _ = ms.db.Exec("DELETE FROM entries")
		_, _ = ms.db.Exec("DELETE FROM layers")
		ms.Close()
	}

	return ms, cleanup
}

func TestWriteReadActiveLayer(t *testing.T) {
	ms, cleanup := createTestMetadataStore(t)
	defer cleanup()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input := []byte("hello world")
	layerID, offset, err := lm.Write(input, 0)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if offset != uint64(0) {
		t.Fatalf("Expected initial write offset to be 0, got %d", offset)
	}

	fullContent := lm.GetFullContent()
	if len(fullContent) != len(input) {
		t.Fatalf("Expected full content length %d, got %d", len(input), len(fullContent))
	}

	data, err := lm.GetDataRange(offset, uint64(len(input)))
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if !bytes.Equal(data, input) {
		t.Fatalf("Expected data %q, got %q", input, data)
	}

	if layerID != lm.ActiveLayer().ID {
		t.Fatalf("Write returned layerID %d, but active layer's ID is %d", layerID, lm.ActiveLayer().ID)
	}
}

func TestSealLayerNewActiveLayer(t *testing.T) {
	ms, cleanup := createTestMetadataStore(t)
	defer cleanup()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input1 := []byte("data1")
	layerID1, offset1, err := lm.Write(input1, 0)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if offset1 != uint64(0) {
		t.Fatalf("Expected first write offset in active layer to be 0, got %d", offset1)
	}

	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("SealActiveLayer failed: %v", err)
	}

	active := lm.ActiveLayer()
	if active.ID == layerID1 {
		t.Fatalf("Active layer did not change after sealing")
	}
	expectedBase := uint64(len(input1))
	if active.base != expectedBase {
		t.Fatalf("Expected new active layer base to be %d, got %d", expectedBase, active.base)
	}

	input2 := []byte("data2")
	layerID2, offset2, err := lm.Write(input2, expectedBase)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if layerID2 == layerID1 {
		t.Fatalf("New write went to the sealed layer instead of the new active layer")
	}
	if offset2 != expectedBase {
		t.Fatalf("Expected new layer's first write offset to be %d, got %d", expectedBase, offset2)
	}

	data, err := lm.GetDataRange(offset2, uint64(len(input2)))
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if !bytes.Equal(data, input2) {
		t.Fatalf("Expected data %q in new active layer, got %q", input2, data)
	}
}

func TestReadFromSealedLayer(t *testing.T) {
	ms, cleanup := createTestMetadataStore(t)
	defer cleanup()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input1 := []byte("old data")
	_, offset1, err := lm.Write(input1, 0)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("SealActiveLayer failed: %v", err)
	}

	input2 := []byte("new data")
	expectedOffset2 := uint64(len(input1))
	_, offset2, err := lm.Write(input2, expectedOffset2)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if offset2 != expectedOffset2 {
		t.Fatalf("Expected new write offset to be %d, got %d", expectedOffset2, offset2)
	}

	data, err := lm.GetDataRange(offset1, uint64(len(input1)))
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if !bytes.Equal(data, input1) {
		t.Fatalf("Expected sealed layer data %q, got %q", input1, data)
	}

	data, err = lm.GetDataRange(offset2, uint64(len(input2)))
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if !bytes.Equal(data, input2) {
		t.Fatalf("Expected active layer data %q, got %q", input2, data)
	}
}

func TestPartialRead(t *testing.T) {
	ms, cleanup := createTestMetadataStore(t)
	defer cleanup()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input := []byte("partial read test")
	_, offset, err := lm.Write(input, 0)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	partialSize := uint64(7)
	data, err := lm.GetDataRange(offset, partialSize)
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if len(data) != int(partialSize) {
		t.Fatalf("Expected partial read length %d, got %d", partialSize, len(data))
	}
	if !bytes.Equal(data, input[:partialSize]) {
		t.Fatalf("Expected partial data %q, got %q", input[:partialSize], data)
	}
}
