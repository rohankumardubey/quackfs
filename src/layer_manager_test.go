package main

import (
	"bytes"
	"testing"
)

func TestWriteReadActiveLayer(t *testing.T) {
	ms, err := NewMetadataStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer ms.Close()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input := []byte("hello world")
	layerID, offset := lm.Write(input)

	if offset != 0 {
		t.Fatalf("Expected initial write offset to be 0, got %d", offset)
	}

	fullContent := lm.GetFullContent()
	if len(fullContent) != len(input) {
		t.Fatalf("Expected full content length %d, got %d", len(input), len(fullContent))
	}

	data, err := lm.GetDataRange(offset, len(input))
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
	ms, err := NewMetadataStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer ms.Close()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input1 := []byte("data1")
	layerID1, offset1 := lm.Write(input1)
	if offset1 != 0 {
		t.Fatalf("Expected first write offset in active layer to be 0, got %d", offset1)
	}

	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("SealActiveLayer failed: %v", err)
	}

	active := lm.ActiveLayer()
	if active.ID == layerID1 {
		t.Fatalf("Active layer did not change after sealing")
	}
	expectedBase := int64(len(input1))
	if active.base != expectedBase {
		t.Fatalf("Expected new active layer base to be %d, got %d", expectedBase, active.base)
	}

	input2 := []byte("data2")
	layerID2, offset2 := lm.Write(input2)
	if layerID2 == layerID1 {
		t.Fatalf("New write went to the sealed layer instead of the new active layer")
	}
	if offset2 != expectedBase {
		t.Fatalf("Expected new layer's first write offset to be %d, got %d", expectedBase, offset2)
	}

	data, err := lm.GetDataRange(offset2, len(input2))
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if !bytes.Equal(data, input2) {
		t.Fatalf("Expected data %q in new active layer, got %q", input2, data)
	}
}

func TestReadFromSealedLayer(t *testing.T) {
	ms, err := NewMetadataStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer ms.Close()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input1 := []byte("old data")
	_, offset1 := lm.Write(input1)

	if err := lm.SealActiveLayer(); err != nil {
		t.Fatalf("SealActiveLayer failed: %v", err)
	}

	input2 := []byte("new data")
	_, offset2 := lm.Write(input2)

	expectedOffset2 := int64(len(input1))
	if offset2 != expectedOffset2 {
		t.Fatalf("Expected new write offset to be %d, got %d", expectedOffset2, offset2)
	}

	data, err := lm.GetDataRange(offset1, len(input1))
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if !bytes.Equal(data, input1) {
		t.Fatalf("Expected sealed layer data %q, got %q", input1, data)
	}

	data, err = lm.GetDataRange(offset2, len(input2))
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if !bytes.Equal(data, input2) {
		t.Fatalf("Expected active layer data %q, got %q", input2, data)
	}
}

func TestPartialRead(t *testing.T) {
	ms, err := NewMetadataStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer ms.Close()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	input := []byte("partial read test")
	_, offset := lm.Write(input)

	partialSize := 7
	data, err := lm.GetDataRange(offset, partialSize)
	if err != nil {
		t.Fatalf("GetDataRange error: %v", err)
	}
	if len(data) != partialSize {
		t.Fatalf("Expected partial read length %d, got %d", partialSize, len(data))
	}
	if !bytes.Equal(data, input[:partialSize]) {
		t.Fatalf("Expected partial data %q, got %q", input[:partialSize], data)
	}
}
