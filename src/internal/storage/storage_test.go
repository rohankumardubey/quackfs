package storage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vinimdocarmo/difffs/src/internal/difffstest"
)

func TestWriteReadActiveLayer(t *testing.T) {
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_write_read" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := sm.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	input := []byte("hello world")
	layerID, offset, err := sm.Write(filename, input, 0)
	require.NoError(t, err, "Write error")
	assert.Equal(t, uint64(0), offset, "Initial write offset should be 0")

	fullContent := sm.GetFullContentForFile(fileID)
	assert.Equal(t, len(input), len(fullContent), "Full content length should match input length")

	data, err := sm.GetDataRange(filename, offset, uint64(len(input)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input, data, "Retrieved data should match input")

	activeLayer := sm.ActiveLayer()
	require.NotNil(t, activeLayer, "Active layer should not be nil")
	assert.Equal(t, activeLayer.ID, layerID, "Write should return the active layer's ID")
}

func TestSealLayerNewActiveLayer(t *testing.T) {
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_seal_layer" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := sm.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	input1 := []byte("data1")
	layerID1, offset1, err := sm.Write(filename, input1, 0)
	require.NoError(t, err, "Write error")
	assert.Equal(t, uint64(0), offset1, "First write offset in active layer should be 0")

	err = sm.SealActiveLayer(filename)
	require.NoError(t, err, "SealActiveLayer failed")

	active := sm.ActiveLayer()
	require.NotNil(t, active, "Active layer should not be nil")
	assert.Equal(t, layerID1+1, active.ID, "Active layer should change after sealing")

	input2 := []byte("data2")
	layerID2, offset2, err := sm.Write(filename, input2, uint64(len(input1)))
	require.NoError(t, err, "Write error")
	require.Equal(t, layerID2, active.ID, "Second write should be in new active layer")

	data, err := sm.GetDataRange(filename, offset2, uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input2, data, "Retrieved data should match second input")
}

func TestReadFromSealedLayer(t *testing.T) {
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_read_sealed" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial data
	input1 := []byte("hello")
	_, _, err = sm.Write(filename, input1, 0)
	require.NoError(t, err, "Write error")

	// Seal the layer
	err = sm.SealActiveLayer(filename)
	require.NoError(t, err, "Failed to seal layer")

	// Write more data
	input2 := []byte(" world")
	_, _, err = sm.Write(filename, input2, uint64(len(input1)))
	require.NoError(t, err, "Write error")

	// Read from first layer
	data1, err := sm.GetDataRange(filename, 0, uint64(len(input1)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input1, data1, "Retrieved data should match first input")

	// Read from second layer
	data2, err := sm.GetDataRange(filename, uint64(len(input1)), uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input2, data2, "Retrieved data should match second input")

	// Read across both layers
	combined := append(input1, input2...)
	data3, err := sm.GetDataRange(filename, 0, uint64(len(combined)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, combined, data3, "Retrieved data should match combined input")
}

func TestPartialRead(t *testing.T) {
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_partial_read" // Unique file name for testing

	// Insert the file, which should create an initial unsealed layer
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := sm.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	input := []byte("partial read test")
	_, offset, err := sm.Write(filename, input, 0)
	require.NoError(t, err, "Write error")

	partialSize := uint64(7)
	data, err := sm.GetDataRange(filename, offset, partialSize)
	require.NoError(t, err, "GetDataRange error")

	assert.Equal(t, int(partialSize), len(data), "Partial read should return requested length")
	assert.Equal(t, input[:partialSize], data, "Partial read should return correct data slice")
}

func TestInitialLayerCreationOnFileInsert(t *testing.T) {
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	filename := "newfile"

	// Insert the file, which should create an initial unsealed layer
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := sm.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")

	// Verify that one layer exists and it is unsealed
	require.Equal(t, 1, len(layers), "Expected one initial layer")
	assert.False(t, layers[0].Sealed, "Initial layer should be unsealed")
}

func TestGetDataRangeByFileName(t *testing.T) {
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_get_data_range"

	// Insert the file, which should create an initial unsealed layer
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write data to the layer
	data := []byte("test data for GetDataRange by filename")
	_, _, err = sm.Write(filename, data, 0)
	require.NoError(t, err, "Failed to write data")

	// Read the data using GetDataRange with filename
	readData, err := sm.GetDataRange(filename, 0, uint64(len(data)))
	require.NoError(t, err, "Failed to read data by filename")
	assert.Equal(t, data, readData, "Data read by filename should match what was written")

	// Try reading with a non-existent filename
	_, err = sm.GetDataRange("nonexistent_file", 0, 10)
	assert.Error(t, err, "Reading from non-existent file should return an error")
}

func TestStorageManagerPersistence(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_persistence"
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write some data
	data1 := []byte("initial data")
	_, _, err = sm.Write(filename, data1, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Seal the layer to simulate a checkpoint
	err = sm.SealActiveLayer(filename)
	require.NoError(t, err, "Failed to seal layer")

	// Write more data
	data2 := []byte("more data")
	_, _, err = sm.Write(filename, data2, uint64(len(data1)))
	require.NoError(t, err, "Failed to write more data")

	// Verify the data is correct
	fullContent := sm.GetFullContentForFile(fileID)
	expectedContent := append(data1, data2...)
	assert.Equal(t, expectedContent, fullContent, "Full content should match expected")

	// Now create a new storage manager instance to simulate restarting the application
	sm2, cleanup2 := difffstest.SetupStorageManager(t)
	defer cleanup2()

	// Verify the data is still correct
	fullContent2 := sm2.GetFullContentForFile(fileID)
	assert.Equal(t, expectedContent, fullContent2, "Full content should persist across storage manager instances")

	// Verify we can still write to the file
	data3 := []byte("even more data")
	_, _, err = sm2.Write(filename, data3, uint64(len(data1)+len(data2)))
	require.NoError(t, err, "Failed to write additional data")

	// Verify the combined data is correct
	fullContent3 := sm2.GetFullContentForFile(fileID)
	expectedContent3 := append(expectedContent, data3...)
	assert.Equal(t, expectedContent3, fullContent3, "Full content should include all writes")
}

func TestFuseScenario(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_fuse_scenario"
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write some initial data
	initialData := []byte("initial data for FUSE test")
	_, _, err = sm.Write(filename, initialData, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Verify the data
	readData, err := sm.GetDataRange(filename, 0, uint64(len(initialData)))
	require.NoError(t, err, "Failed to read data")
	assert.Equal(t, initialData, readData, "Read data should match written data")

	// Simulate a checkpoint
	err = sm.SealActiveLayer(filename)
	require.NoError(t, err, "Failed to seal layer")

	// Write more data
	additionalData := []byte(" - additional data")
	_, _, err = sm.Write(filename, additionalData, uint64(len(initialData)))
	require.NoError(t, err, "Failed to write additional data")

	// Read the combined data
	combinedData := append(initialData, additionalData...)
	readCombined, err := sm.GetDataRange(filename, 0, uint64(len(combinedData)))
	require.NoError(t, err, "Failed to read combined data")
	assert.Equal(t, combinedData, readCombined, "Combined data should match expected")

	// Verify file size
	size, err := sm.FileSize(fileID)
	require.NoError(t, err, "Failed to get file size")
	assert.Equal(t, uint64(len(combinedData)), size, "File size should match combined data length")

	// Create a new storage manager to simulate restarting
	sm2, cleanup2 := difffstest.SetupStorageManager(t)
	defer cleanup2()

	// Verify data persists
	readAfterRestart, err := sm2.GetDataRange(filename, 0, uint64(len(combinedData)))
	require.NoError(t, err, "Failed to read data after restart")
	assert.Equal(t, combinedData, readAfterRestart, "Data should persist after restart")
}

func TestFailedWriteBeyondFileSize(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_failed_write_beyond_file_size"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write data at different offsets
	_, _, err = sm.Write(filename, []byte("first"), 0)
	require.NoError(t, err, "Failed to write 'first'")

	_, _, err = sm.Write(filename, []byte("second"), 10)
	require.Error(t, err, "Write should fail because it's beyond the file size")
}

func TestCalculateVirtualFileSize(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_virtual_size"
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write data at different offsets to create gaps
	writes := []struct {
		offset uint64
		data   []byte
	}{
		{0, []byte("start")},  // 0-4
		{5, []byte("middle")}, // 5-10
		{10, []byte("end")},   // 10-13
	}

	// Perform the writes
	for _, w := range writes {
		_, _, err := sm.Write(filename, w.data, w.offset)
		require.NoError(t, err, "Failed to write at offset %d", w.offset)
	}

	// Get the file size
	size, err := sm.FileSize(fileID)
	require.NoError(t, err, "Failed to get file size")

	// The size should be the highest offset + length of data at that offset
	expectedSize := uint64(10 + len([]byte("end")))
	assert.Equal(t, expectedSize, size, "File size should be based on highest offset + data length")

	// Seal the layer and write more data at a higher offset
	err = sm.SealActiveLayer(filename)
	require.NoError(t, err, "Failed to seal layer")

	// Write at an even higher offset
	finalData := []byte("final")
	finalOffset := uint64(13)
	_, _, err = sm.Write(filename, finalData, finalOffset)
	require.NoError(t, err, "Failed to write final data")

	// Get the updated file size
	newSize, err := sm.FileSize(fileID)
	require.NoError(t, err, "Failed to get updated file size")

	// The size should now be the new highest offset + length of data at that offset
	expectedNewSize := finalOffset + uint64(len(finalData))
	assert.Equal(t, expectedNewSize, newSize, "Updated file size should reflect the new highest offset + data length")
}

func TestDeleteFile(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	// Create multiple test files
	filenames := []string{
		"testfile_delete_1",
		"testfile_delete_2",
		"testfile_delete_3",
	}

	// Insert all files
	for _, filename := range filenames {
		_, err := sm.InsertFile(filename)
		require.NoError(t, err, "Failed to insert file: %s", filename)

		// Write some data to each file
		_, _, err = sm.Write(filename, []byte("data for "+filename), 0)
		require.NoError(t, err, "Failed to write to file: %s", filename)
	}

	// Get all files and verify count
	files, err := sm.GetAllFiles()
	require.NoError(t, err, "Failed to get all files")
	assert.Equal(t, len(filenames), len(files), "Should have the expected number of files")

	// Delete the second file
	err = sm.DeleteFile(filenames[1])
	require.NoError(t, err, "Failed to delete file")

	// Get all files again and verify count
	filesAfterDelete, err := sm.GetAllFiles()
	require.NoError(t, err, "Failed to get all files after delete")
	assert.Equal(t, len(filenames)-1, len(filesAfterDelete), "Should have one less file after deletion")

	// Verify the deleted file is gone
	var deletedFileFound bool
	for _, file := range filesAfterDelete {
		if file.Name == filenames[1] {
			deletedFileFound = true
			break
		}
	}
	assert.False(t, deletedFileFound, "Deleted file should not be found")

	// Verify the other files still exist
	var file1Found, file3Found bool
	for _, file := range filesAfterDelete {
		if file.Name == filenames[0] {
			file1Found = true
		}
		if file.Name == filenames[2] {
			file3Found = true
		}
	}
	assert.True(t, file1Found, "First file should still exist")
	assert.True(t, file3Found, "Third file should still exist")

	// Try to read from the deleted file
	_, err = sm.GetDataRange(filenames[1], 0, 10)
	assert.Error(t, err, "Reading from deleted file should return an error")

	// Try to write to the deleted file
	_, _, err = sm.Write(filenames[1], []byte("new data"), 0)
	assert.Error(t, err, "Writing to deleted file should return an error")
}

func TestExampleWorkflow(t *testing.T) {
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_example_workflow"

	// Insert the file, which should create an initial unsealed layer
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := sm.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	// Write initial data.
	data1 := []byte("Hello, checkpoint!")
	_, offset1, err := sm.Write(filename, data1, 0)
	require.NoError(t, err, "Failed to write initial data")
	assert.Equal(t, uint64(0), offset1, "First write offset should be 0")

	// Simulate a checkpoint using our test instance.
	err = sm.Checkpoint(filename)
	require.NoError(t, err, "Failed to checkpoint file")

	// Write additional data.
	data2 := []byte("More data after checkpoint.")
	expectedOffset2 := uint64(len(data1))
	_, offset2, err := sm.Write(filename, data2, expectedOffset2)
	require.NoError(t, err, "Failed to write additional data")
	assert.Equal(t, expectedOffset2, offset2, "Second write offset should match data1 length")

	// The full file content should be the concatenation of data1 and data2.
	expectedContent := append(data1, data2...)
	fullContent := sm.GetFullContentForFile(fileID)
	assert.Equal(t, expectedContent, fullContent, "Full content should be the concatenation of data1 and data2")
}

func TestWriteToSameOffsetTwice(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := difffstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_write_same_offset"
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial data
	initialData := []byte("initial data")
	_, _, err = sm.Write(filename, initialData, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Verify the initial data was written correctly
	readData, err := sm.GetDataRange(filename, 0, uint64(len(initialData)))
	require.NoError(t, err, "Failed to read initial data")
	assert.Equal(t, initialData, readData, "Initial data should be read correctly")

	// Write new data to the same offset
	newData := []byte("overwritten!")
	_, _, err = sm.Write(filename, newData, 0)
	require.NoError(t, err, "Failed to write new data to the same offset")

	// Verify the new data overwrote the initial data
	readNewData, err := sm.GetDataRange(filename, 0, uint64(len(newData)))
	require.NoError(t, err, "Failed to read new data")
	assert.Equal(t, newData, readNewData, "New data should overwrite initial data at the same offset")

	// Check the full content of the file
	fullContent := sm.GetFullContentForFile(fileID)
	assert.Equal(t, newData, fullContent, "Full content should match the new data")

	// Write data that partially overlaps with existing data
	partialData := []byte("partial")
	partialOffset := uint64(5) // This will overlap with part of the existing data
	_, _, err = sm.Write(filename, partialData, partialOffset)
	require.NoError(t, err, "Failed to write partially overlapping data")

	// Expected content after partial write
	expectedContent := make([]byte, len(newData))
	copy(expectedContent, newData)
	// Overwrite the portion that should be replaced by partialData
	for i := 0; i < len(partialData); i++ {
		if int(partialOffset)+i < len(expectedContent) {
			expectedContent[partialOffset+uint64(i)] = partialData[i]
		} else {
			expectedContent = append(expectedContent, partialData[i:]...)
			break
		}
	}

	// Verify the full content matches our expectations
	fullContentAfterPartial := sm.GetFullContentForFile(fileID)
	assert.Equal(t, expectedContent, fullContentAfterPartial, "Full content should reflect partial overwrite")
}
