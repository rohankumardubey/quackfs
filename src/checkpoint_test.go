package main

import (
	"bytes"
	"testing"
)

func TestExampleWorkflow(t *testing.T) {
	// Use an in-memory SQLite database for testing.
	ms, err := NewMetadataStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer ms.Close()

	lm, err := NewLayerManager(ms)
	if err != nil {
		t.Fatalf("Failed to create LayerManager: %v", err)
	}

	// Write initial data.
	data1 := []byte("Hello, checkpoint!")
	_, offset1 := lm.Write(data1)
	if offset1 != 0 {
		t.Fatalf("Expected first write offset to be 0, got %d", offset1)
	}

	// Simulate a checkpoint using our test instance.
	simulateCheckpoint(lm)

	// Write additional data.
	data2 := []byte("More data after checkpoint.")
	_, offset2 := lm.Write(data2)
	expectedOffset2 := int64(len(data1))
	if offset2 != expectedOffset2 {
		t.Fatalf("Expected second write offset to be %d, got %d", expectedOffset2, offset2)
	}

	// The full file content should be the concatenation of data1 and data2.
	expectedContent := append(data1, data2...)
	fullContent := lm.GetFullContent()
	if !bytes.Equal(fullContent, expectedContent) {
		t.Fatalf("Expected full content %q, got %q", expectedContent, fullContent)
	}
}
