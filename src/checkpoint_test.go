package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExampleWorkflow(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	// Write initial data.
	data1 := []byte("Hello, checkpoint!")
	_, offset1, err := lm.Write(data1, 0)
	require.NoError(t, err, "Failed to write initial data")
	assert.Equal(t, uint64(0), offset1, "First write offset should be 0")

	// Simulate a checkpoint using our test instance.
	simulateCheckpoint(lm)

	// Write additional data.
	data2 := []byte("More data after checkpoint.")
	expectedOffset2 := uint64(len(data1))
	_, offset2, err := lm.Write(data2, expectedOffset2)
	require.NoError(t, err, "Failed to write additional data")
	assert.Equal(t, expectedOffset2, offset2, "Second write offset should match data1 length")

	// The full file content should be the concatenation of data1 and data2.
	expectedContent := append(data1, data2...)
	fullContent := lm.GetFullContent()
	assert.Equal(t, expectedContent, fullContent, "Full content should be the concatenation of data1 and data2")
}
