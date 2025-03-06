package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadActiveLayer(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	filename := "testfile_write_read" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	fileID, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	input := []byte("hello world")
	layerID, offset, err := lm.Write(filename, input, 0)
	require.NoError(t, err, "Write error")
	assert.Equal(t, uint64(0), offset, "Initial write offset should be 0")

	fullContent := lm.GetFullContentForFile(fileID)
	assert.Equal(t, len(input), len(fullContent), "Full content length should match input length")

	data, err := lm.GetDataRange(filename, offset, uint64(len(input)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input, data, "Retrieved data should match input")

	activeLayer := lm.ActiveLayer()
	require.NotNil(t, activeLayer, "Active layer should not be nil")
	assert.Equal(t, activeLayer.ID, layerID, "Write should return the active layer's ID")
}

func TestSealLayerNewActiveLayer(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	filename := "testfile_seal_layer" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	fileID, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	input1 := []byte("data1")
	layerID1, offset1, err := lm.Write(filename, input1, 0)
	require.NoError(t, err, "Write error")
	assert.Equal(t, uint64(0), offset1, "First write offset in active layer should be 0")

	err = lm.SealActiveLayer(filename)
	require.NoError(t, err, "SealActiveLayer failed")

	active := lm.ActiveLayer()
	require.NotNil(t, active, "Active layer should not be nil")
	assert.Equal(t, layerID1+1, active.ID, "Active layer should change after sealing")

	input2 := []byte("data2")
	layerID2, offset2, err := lm.Write(filename, input2, uint64(len(input1)))
	require.NoError(t, err, "Write error")
	require.Equal(t, layerID2, active.ID, "Second write should be in new active layer")

	data, err := lm.GetDataRange(filename, offset2, uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input2, data, "Retrieved data should match second input")
}

func TestReadFromSealedLayer(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	filename := "testfile_read_sealed" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	_, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial data
	input1 := []byte("hello")
	_, _, err = lm.Write(filename, input1, 0)
	require.NoError(t, err, "Write error")

	// Seal the layer
	err = lm.SealActiveLayer(filename)
	require.NoError(t, err, "Failed to seal layer")

	// Write more data
	input2 := []byte(" world")
	_, _, err = lm.Write(filename, input2, uint64(len(input1)))
	require.NoError(t, err, "Write error")

	// Read from first layer
	data1, err := lm.GetDataRange(filename, 0, uint64(len(input1)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input1, data1, "Retrieved data should match first input")

	// Read from second layer
	data2, err := lm.GetDataRange(filename, uint64(len(input1)), uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input2, data2, "Retrieved data should match second input")

	// Read across both layers
	combined := append(input1, input2...)
	data3, err := lm.GetDataRange(filename, 0, uint64(len(combined)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, combined, data3, "Retrieved data should match combined input")
}

func TestPartialRead(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	filename := "testfile_partial_read" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	fileID, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	input := []byte("partial read test")
	_, offset, err := lm.Write(filename, input, 0)
	require.NoError(t, err, "Write error")

	partialSize := uint64(7)
	data, err := lm.GetDataRange(filename, offset, partialSize)
	require.NoError(t, err, "GetDataRange error")

	assert.Equal(t, int(partialSize), len(data), "Partial read should return requested length")
	assert.Equal(t, input[:partialSize], data, "Partial read should return correct data slice")
}

func TestInitialLayerCreationOnFileInsert(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	filename := "newfile"

	// Insert the file, which should create an initial unsealed layer
	fileID, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")

	// Verify that one layer exists and it is unsealed
	require.Equal(t, 1, len(layers), "Expected one initial layer")
	assert.False(t, layers[0].Sealed, "Initial layer should be unsealed")
}

// TestGetDataRangeByFileName tests that GetDataRange correctly retrieves data for a specific file
func TestGetDataRangeByFileName(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	// Create two different files with different content
	filename1 := "testfile1_data_range"
	filename2 := "testfile2_data_range"

	// Insert the files
	fileID1, err := lm.metadata.InsertFile(filename1)
	require.NoError(t, err, "Failed to insert file1")

	fileID2, err := lm.metadata.InsertFile(filename2)
	require.NoError(t, err, "Failed to insert file2")

	// Write different content to each file
	content1 := []byte("hello from file 1")
	content2 := []byte("different content in file 2")

	// Write to file 1
	_, _, err = lm.Write(filename1, content1, 0)
	require.NoError(t, err, "Failed to write to file1")

	// Write to file 2
	_, _, err = lm.Write(filename2, content2, 0)
	require.NoError(t, err, "Failed to write to file2")

	// Read from file 1
	data1, err := lm.GetDataRange(filename1, 0, uint64(len(content1)))
	require.NoError(t, err, "GetDataRange error for file1")
	assert.Equal(t, content1, data1, "Retrieved data should match file1 content")

	// Read from file 2
	data2, err := lm.GetDataRange(filename2, 0, uint64(len(content2)))
	require.NoError(t, err, "GetDataRange error for file2")
	assert.Equal(t, content2, data2, "Retrieved data should match file2 content")

	// Test partial reads
	partialData1, err := lm.GetDataRange(filename1, 0, 5)
	require.NoError(t, err, "Partial GetDataRange error for file1")
	assert.Equal(t, content1[:5], partialData1, "Retrieved partial data should match file1 content")

	// Test offset reads
	offsetData2, err := lm.GetDataRange(filename2, 10, 5)
	require.NoError(t, err, "Offset GetDataRange error for file2")
	assert.Equal(t, content2[10:15], offsetData2, "Retrieved offset data should match file2 content")

	// Verify that GetFullContentForFile returns the correct content for each file
	fullContent1 := lm.GetFullContentForFile(fileID1)
	assert.Equal(t, content1, fullContent1, "Full content for file1 should match")

	fullContent2 := lm.GetFullContentForFile(fileID2)
	assert.Equal(t, content2, fullContent2, "Full content for file2 should match")
}
