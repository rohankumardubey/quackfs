package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExampleWorkflow(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	filename := "testfile_example_workflow"

	// Insert the file, which should create an initial unsealed layer
	fileID, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	// Write initial data.
	data1 := []byte("Hello, checkpoint!")
	_, offset1, err := lm.Write(filename, data1, 0)
	require.NoError(t, err, "Failed to write initial data")
	assert.Equal(t, uint64(0), offset1, "First write offset should be 0")

	// Simulate a checkpoint using our test instance.
	simulateCheckpoint(lm, filename)

	// Write additional data.
	data2 := []byte("More data after checkpoint.")
	expectedOffset2 := uint64(len(data1))
	_, offset2, err := lm.Write(filename, data2, expectedOffset2)
	require.NoError(t, err, "Failed to write additional data")
	assert.Equal(t, expectedOffset2, offset2, "Second write offset should match data1 length")

	// The full file content should be the concatenation of data1 and data2.
	expectedContent := append(data1, data2...)
	fullContent := lm.GetFullContentForFile(fileID)
	assert.Equal(t, expectedContent, fullContent, "Full content should be the concatenation of data1 and data2")
}
