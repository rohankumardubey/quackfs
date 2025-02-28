package main

import (
	"bytes"
	"os"
	"testing"
)

func TestExampleWorkflow(t *testing.T) {
	// Fail if PostgreSQL connection string is not available
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}

	ms, err := NewMetadataStore(connStr)
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}

	// Clean up existing test data
	_, _ = ms.db.Exec("DELETE FROM entries")
	_, _ = ms.db.Exec("DELETE FROM layers")

	defer func() {
		_, _ = ms.db.Exec("DELETE FROM entries")
		_, _ = ms.db.Exec("DELETE FROM layers")
		ms.Close()
	}()

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
	expectedOffset2 := uint64(len(data1))
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
