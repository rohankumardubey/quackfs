package storage_test

import (
	"database/sql"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vinimdocarmo/quackfs/src/internal/quackfstest"
	"github.com/vinimdocarmo/quackfs/src/internal/storage"
)

func TestWriteReadActiveLayer(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_write_read" // Unique file name for testing

	// Insert the file, which should create an initial active layer
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	require.Equal(t, sm.GetActiveLayerSize(fileID), uint64(0), "Active layer should be empty")
	require.Nil(t, sm.GetActiveLayerData(fileID), "Active layer should be nil")

	input := []byte("hello world")
	err = sm.WriteFile(filename, input, 0)
	require.NoError(t, err, "Write error")
	require.Equal(t, sm.GetActiveLayerSize(fileID), uint64(len(input)), "Active layer should be the same size as the input")
	require.Equal(t, string(sm.GetActiveLayerData(fileID)), string(input), "Active layer should contain the input data")

	// Get the file size to read the entire content
	fileSize, err := sm.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	assert.Equal(t, uint64(len(input)), fileSize, "File size should match input length")

	fullContent, err := sm.ReadFile(filename, 0, fileSize)
	require.NoError(t, err, "Failed to get full content")
	assert.Equal(t, len(input), len(fullContent), "Full content length should match input length")

	data, err := sm.ReadFile(filename, 0, uint64(len(input)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input, data, "Retrieved data should match input")
}

func TestCheckpointingNewActiveLayer(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_checkpoint_layer" // Unique file name for testing

	// Insert the file, which should create an initial active layer
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	input1 := []byte("data1")
	err = sm.WriteFile(filename, input1, 0)
	require.NoError(t, err, "Write error")

	err = sm.Checkpoint(filename, "v1")
	require.NoError(t, err, "Checkpoint failed")

	db := quackfstest.SetupDB(t)
	defer db.Close()

	input2 := []byte("data2")
	err = sm.WriteFile(filename, input2, uint64(len(input1)))
	require.NoError(t, err, "Write error")

	data, err := sm.ReadFile(filename, uint64(len(input1)), uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, string(input2), string(data), "Retrieved data should match second input")
}

func TestReadFromActiveLayer(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_read_active" // Unique file name for testing

	// Insert the file, which should create an initial active layer
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial data
	input1 := []byte("hello")
	err = sm.WriteFile(filename, input1, 0)
	require.NoError(t, err, "Write error")

	// Seal the layer
	err = sm.Checkpoint(filename, "v1")
	require.NoError(t, err, "Failed to commit layer")

	// Write more data
	input2 := []byte(" world")
	err = sm.WriteFile(filename, input2, uint64(len(input1)))
	require.NoError(t, err, "Write error")

	// Read from first layer
	data1, err := sm.ReadFile(filename, 0, uint64(len(input1)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input1, data1, "Retrieved data should match first input")

	// Read from second layer
	data2, err := sm.ReadFile(filename, uint64(len(input1)), uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input2, data2, "Retrieved data should match second input")

	// Read across both layers
	combined := append(input1, input2...)
	data3, err := sm.ReadFile(filename, 0, uint64(len(combined)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, combined, data3, "Retrieved data should match combined input")
}

func TestPartialRead(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_partial_read" // Unique file name for testing

	// Insert the file, which should create an initial active layer
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	input := []byte("partial read test")
	err = sm.WriteFile(filename, input, 0)
	require.NoError(t, err, "Write error")

	partialSize := uint64(7)
	data, err := sm.ReadFile(filename, 0, partialSize)
	require.NoError(t, err, "GetDataRange error")

	assert.Equal(t, int(partialSize), len(data), "Partial read should return requested length")
	assert.Equal(t, input[:partialSize], data, "Partial read should return correct data slice")
}

func TestGetDataRangeByFileName(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_get_data_range"

	// Insert the file, which should create an initial active layer
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write data to the layer
	data := []byte("test data for GetDataRange by filename")
	err = sm.WriteFile(filename, data, 0)
	require.NoError(t, err, "Failed to write data")

	// Read the data using GetDataRange with filename
	readData, err := sm.ReadFile(filename, 0, uint64(len(data)))
	require.NoError(t, err, "Failed to read data by filename")
	assert.Equal(t, data, readData, "Data read by filename should match what was written")

	// Try reading with a non-existent filename
	_, err = sm.ReadFile("nonexistent_file", 0, 10)
	assert.Error(t, err, "Reading from non-existent file should return an error")
}

func TestStorageManagerPersistence(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_persistence"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write some data
	data1 := []byte("initial data")
	err = sm.WriteFile(filename, data1, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Seal the layer to simulate a checkpoint
	err = sm.Checkpoint(filename, "v1")
	require.NoError(t, err, "Failed to commit layer")

	// Write more data to active layer
	data2 := []byte("more data")
	err = sm.WriteFile(filename, data2, uint64(len(data1)))
	require.NoError(t, err, "Failed to write more data")

	// Checkpoint again to persist the second data
	err = sm.Checkpoint(filename, "v2")
	require.NoError(t, err, "Failed to commit second layer")

	// Verify the data is correct
	fileSize, err := sm.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	fullContent, err := sm.ReadFile(filename, 0, fileSize)
	require.NoError(t, err, "Failed to get full content")
	expectedContent := append(data1, data2...)
	assert.Equal(t, expectedContent, fullContent, "Full content should match expected")

	// Now create a new storage manager instance to simulate restarting the application
	sm2, cleanup2 := quackfstest.SetupStorageManager(t)
	defer cleanup2()

	// Verify the checkpointed data is still correct (should include data1 and data2)
	fileSize2, err := sm2.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	fullContent2, err := sm2.ReadFile(filename, 0, fileSize2)
	require.NoError(t, err, "Failed to get full content")
	assert.Equal(t, expectedContent, fullContent2, "Checkpointed content should persist across storage manager instances")

	// Verify we can still write to the file
	data3 := []byte("even more data")
	err = sm2.WriteFile(filename, data3, uint64(len(data1)+len(data2)))
	require.NoError(t, err, "Failed to write additional data")

	// Verify the combined data is correct in memory
	fileSize3, err := sm2.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	fullContent3, err := sm2.ReadFile(filename, 0, fileSize3)
	require.NoError(t, err, "Failed to get full content")
	expectedContent3 := append(expectedContent, data3...)
	assert.Equal(t, expectedContent3, fullContent3, "Full content should include all writes")

	// Checkpoint again to persist the third data
	err = sm2.Checkpoint(filename, "v3")
	require.NoError(t, err, "Failed to commit third layer")

	// Create yet another storage manager to verify all three checkpoints persist
	sm3, cleanup3 := quackfstest.SetupStorageManager(t)
	defer cleanup3()

	// Verify all checkpointed data persists (should include data1, data2, and data3)
	fileSize4, err := sm3.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	fullContent4, err := sm3.ReadFile(filename, 0, fileSize4)
	require.NoError(t, err, "Failed to get full content")
	assert.Equal(t, expectedContent3, fullContent4, "All checkpointed content should persist")
}

func TestFuseScenario(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_fuse_scenario"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write some initial data
	initialData := []byte("initial data for FUSE test")
	err = sm.WriteFile(filename, initialData, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Verify the data
	readData, err := sm.ReadFile(filename, 0, uint64(len(initialData)))
	require.NoError(t, err, "Failed to read data")
	assert.Equal(t, initialData, readData, "Read data should match written data")

	// Simulate a checkpoint
	err = sm.Checkpoint(filename, "v1")
	require.NoError(t, err, "Failed to commit layer")

	// Write more data
	additionalData := []byte(" - additional data")
	err = sm.WriteFile(filename, additionalData, uint64(len(initialData)))
	require.NoError(t, err, "Failed to write additional data")

	// Read the combined data
	combinedData := append(initialData, additionalData...)
	readCombined, err := sm.ReadFile(filename, 0, uint64(len(combinedData)))
	require.NoError(t, err, "Failed to read combined data")
	assert.Equal(t, combinedData, readCombined, "Combined data should match expected")

	// Verify file size
	size, err := sm.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	assert.Equal(t, uint64(len(combinedData)), size, "File size should match combined data length")

	// Create checkpoint for additional data
	err = sm.Checkpoint(filename, "v2")
	require.NoError(t, err, "Failed to commit second layer")

	// Create a new storage manager to simulate restarting
	sm2, cleanup2 := quackfstest.SetupStorageManager(t)
	defer cleanup2()

	// Verify checkpointed data persists
	readAfterRestart, err := sm2.ReadFile(filename, 0, uint64(len(combinedData)))
	require.NoError(t, err, "Failed to read data after restart")
	assert.Equal(t, combinedData, readAfterRestart, "Checkpointed data should persist after restart")

	// Write more data after restart
	finalData := []byte(" (final segment)")
	err = sm2.WriteFile(filename, finalData, uint64(len(combinedData)))
	require.NoError(t, err, "Failed to write final data")

	// Read the final combined data
	finalCombinedData := append(combinedData, finalData...)
	readFinalData, err := sm2.ReadFile(filename, 0, uint64(len(finalCombinedData)))
	require.NoError(t, err, "Failed to read final combined data")
	assert.Equal(t, finalCombinedData, readFinalData, "Final combined data should match expected")

	// Create a third storage manager without checkpointing
	sm3, cleanup3 := quackfstest.SetupStorageManager(t)
	defer cleanup3()

	// Verify only the checkpointed data persists (without the final segment)
	persistedData, err := sm3.ReadFile(filename, 0, uint64(len(finalCombinedData)))
	require.NoError(t, err, "Failed to read persisted data")
	assert.Equal(t, combinedData, persistedData, "Only checkpointed data should persist across instances")
}

func TestWriteBeyondFileSize(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_failed_write_beyond_file_size"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write data at different offsets
	err = sm.WriteFile(filename, []byte("first"), 0)
	require.NoError(t, err, "Failed to write 'first'")

	err = sm.WriteFile(filename, []byte("second"), 10)
	require.NoError(t, err, "Write should fail because it's beyond the file size")

	// check file content
	content, err := sm.ReadFile(filename, 0, 16)
	require.NoError(t, err, "Failed to read file content")
	assert.Equal(t, []byte("first\x00\x00\x00\x00\x00second"), content, "File content should match 'firstsecond'")
}

func TestCalculateVirtualFileSize(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_virtual_size"
	_, err := sm.InsertFile(filename)
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
		err = sm.WriteFile(filename, w.data, w.offset)
		require.NoError(t, err, "Failed to write at offset %d", w.offset)
	}

	// Get the file size
	size, err := sm.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")

	// The size should be the highest offset + length of data at that offset
	expectedSize := uint64(10 + len([]byte("end")))
	assert.Equal(t, expectedSize, size, "File size should be based on highest offset + data length")

	// Seal the layer and write more data at a higher offset
	err = sm.Checkpoint(filename, "v1")
	require.NoError(t, err, "Failed to commit layer")

	// Write at an even higher offset
	finalData := []byte("final")
	finalOffset := uint64(13)
	err = sm.WriteFile(filename, finalData, finalOffset)
	require.NoError(t, err, "Failed to write final data")

	// Get the updated file size
	newSize, err := sm.SizeOf(filename)
	require.NoError(t, err, "Failed to get updated file size")

	// The size should now be the new highest offset + length of data at that offset
	expectedNewSize := finalOffset + uint64(len(finalData))
	assert.Equal(t, expectedNewSize, newSize, "Updated file size should reflect the new highest offset + data length")
}

func TestExampleWorkflow(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_example_workflow"

	// Insert the file, which should create an initial active layer
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial data.
	data1 := []byte("Hello, checkpoint!")
	err = sm.WriteFile(filename, data1, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Simulate a checkpoint using our test instance.
	err = sm.Checkpoint(filename, "v1")
	require.NoError(t, err, "Failed to commit layer")

	// Write additional data.
	data2 := []byte("More data after checkpoint.")
	expectedOffset2 := uint64(len(data1))
	err = sm.WriteFile(filename, data2, expectedOffset2)
	require.NoError(t, err, "Failed to write additional data")

	// The full file content should be the concatenation of data1 and data2.
	expectedContent := append(data1, data2...)
	fileSize, err := sm.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	fullContent, err := sm.ReadFile(filename, 0, fileSize)
	require.NoError(t, err, "Failed to get full content")
	assert.Equal(t, expectedContent, fullContent, "Full content should be the concatenation of data1 and data2")
}

func TestWriteToSameOffsetTwice(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_write_same_offset"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial data
	initialData := []byte("initial data")
	err = sm.WriteFile(filename, initialData, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Verify the initial data was written correctly
	readData, err := sm.ReadFile(filename, 0, uint64(len(initialData)))
	require.NoError(t, err, "Failed to read initial data")
	assert.Equal(t, initialData, readData, "Initial data should be read correctly")

	// Write new data to the same offset
	newData := []byte("overwritten!")
	err = sm.WriteFile(filename, newData, 0)
	require.NoError(t, err, "Failed to write new data to the same offset")

	// Verify the new data overwrote the initial data
	readNewData, err := sm.ReadFile(filename, 0, uint64(len(newData)))
	require.NoError(t, err, "Failed to read new data")
	assert.Equal(t, newData, readNewData, "New data should overwrite initial data at the same offset")

	// Check the full content of the file
	fileSize, err := sm.SizeOf(filename)
	require.NoError(t, err, "Failed to get file size")
	fullContent, err := sm.ReadFile(filename, 0, fileSize)
	require.NoError(t, err, "Failed to get full content")
	assert.Equal(t, newData, fullContent, "Full content should match the new data")

	// Write data that partially overlaps with existing data
	partialData := []byte("partial")
	partialOffset := uint64(5) // This will overlap with part of the existing data
	err = sm.WriteFile(filename, partialData, partialOffset)
	require.NoError(t, err, "Failed to write partially overlapping data")

	// Expected content after partial write
	expectedContent := make([]byte, len(newData))
	copy(expectedContent, newData)
	// Overwrite the portion that should be replaced by partialData
	for i := range len(partialData) {
		if int(partialOffset)+i < len(expectedContent) {
			expectedContent[partialOffset+uint64(i)] = partialData[i]
		} else {
			expectedContent = append(expectedContent, partialData[i:]...)
			break
		}
	}

	// Verify the full content matches our expectations
	fullContentAfterPartial, err := sm.ReadFile(filename, 0, uint64(len(expectedContent)))
	require.NoError(t, err, "Failed to get full content")
	assert.Equal(t, expectedContent, fullContentAfterPartial, "Full content should reflect partial overwrite")
}

func TestVersionedLayers(t *testing.T) {
	// Setup a storage manager
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_versioned"
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write some initial data
	initialData := []byte("initial data")
	err = sm.WriteFile(filename, initialData, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Checkpoint with version tag "v1"
	versionTag1 := "v1"
	err = sm.Checkpoint(filename, versionTag1)
	require.NoError(t, err, "Failed to checkpoint file with version tag")

	// Write more data
	additionalData := []byte(" - additional data")
	err = sm.WriteFile(filename, additionalData, uint64(len(initialData)))
	require.NoError(t, err, "Failed to write additional data")

	// Checkpoint with version tag "v2"
	versionTag2 := "v2"
	err = sm.Checkpoint(filename, versionTag2)
	require.NoError(t, err, "Failed to checkpoint file with version tag")

	// Load all layers for the file
	layers, err := sm.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 2, len(layers), "Expected two layers (v1, v2)")

	db := quackfstest.SetupDB(t)
	defer db.Close()

	// Get version IDs
	versionID1 := getVersionIDByTag(t, db, versionTag1)
	versionID2 := getVersionIDByTag(t, db, versionTag2)

	// Print actual values for debugging
	t.Logf("Layer 0 version ID: %d", layers[0].VersionID)
	t.Logf("Layer 1 version ID: %d", layers[1].VersionID)
	t.Logf("Version ID for tag v1: %d", versionID1)
	t.Logf("Version ID for tag v2: %d", versionID2)

	// Check layer versions based on the actual values
	// First layer has version v1
	assert.Equal(t, versionTag1, layers[0].Tag, "First layer should have version v1")

	// Second layer has version v2
	assert.Equal(t, versionTag2, layers[1].Tag, "Second layer should have version v2")

	// Verify that we can retrieve version tags by ID
	tag1 := getVersionTagByID(t, db, versionID1)
	assert.Equal(t, versionTag1, tag1, "Retrieved tag should match original tag")

	tag2 := getVersionTagByID(t, db, versionID2)
	assert.Equal(t, versionTag2, tag2, "Retrieved tag should match original tag")
}

func TestGetDataRangeWithVersion(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	// Create a test file
	filename := "testfile_versioned_read"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial content
	initialContent := []byte("***************")
	err = sm.WriteFile(filename, initialContent, 0)
	require.NoError(t, err, "Failed to write initial content")

	// Create version v1
	v1Tag := "v1"
	err = sm.Checkpoint(filename, v1Tag)
	require.NoError(t, err, "Failed to checkpoint with version v1")

	// Write more content
	updatedContent := []byte("---------------")
	err = sm.WriteFile(filename, updatedContent, 0)
	require.NoError(t, err, "Failed to write updated content")

	// Create version v2
	v2Tag := "v2"
	err = sm.Checkpoint(filename, v2Tag)
	require.NoError(t, err, "Failed to checkpoint with version v2")

	// Write final content
	finalContent := []byte("@@@@@@@@@@@@@@@")
	err = sm.WriteFile(filename, finalContent, 0)
	require.NoError(t, err, "Failed to write final content")

	// Test reading with version v1
	v1Content, err := sm.ReadFile(filename, 0, 100, storage.WithVersion(v1Tag))
	require.NoError(t, err, "Failed to read content with version v1")
	assert.Equal(t, string(v1Content), string(initialContent), "Expected content for version v1 to be %q, got %q", initialContent, v1Content)

	// Test reading with version v2
	v2Content, err := sm.ReadFile(filename, 0, 100, storage.WithVersion(v2Tag))
	require.NoError(t, err, "Failed to read content with version v2")
	assert.Equal(t, string(v2Content), string(updatedContent), "Expected content for version v2 to be %q, got %q", updatedContent, v2Content)

	// Test reading latest content (no version specified)
	latestContent, err := sm.ReadFile(filename, 0, 100)
	require.NoError(t, err, "Failed to read latest content")

	assert.Equal(t, string(latestContent), string(finalContent), "Expected latest content to be %q, got %q", finalContent, latestContent)

	// Test reading with non-existent version
	_, err = sm.ReadFile(filename, 0, 100, storage.WithVersion("non_existent_version"))
	assert.Error(t, err, "Expected error when reading with non-existent version")
	assert.Contains(t, err.Error(), "version tag not found", "Error should indicate version tag not found")
}

func TestWithinAndOverlappingWrites(t *testing.T) {
	/**
	[2000, 4000):   	----------
	[1024, 2048):     @@@@@
	[3000, 6000):              %%%%%%%%%
	[0, 4096):   	***************
	*/

	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_within_and_overlapping_writes"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	first := make([]byte, 4096)
	// fill with *
	for i := range first {
		first[i] = '*'
	}

	err = sm.WriteFile(filename, first, 0)
	require.NoError(t, err, "Failed to write first data")

	{
		// check content
		content, err := sm.ReadFile(filename, 0, 4096)
		require.NoError(t, err, "Failed to read first data")
		assert.Equal(t, first, content, "Content should match")
	}

	second := make([]byte, 3000)
	// fill with %
	for i := range second {
		second[i] = '%'
	}

	err = sm.WriteFile(filename, second, 3000)
	require.NoError(t, err, "Failed to write second data")

	{
		// check content
		content, err := sm.ReadFile(filename, 0, 6000)
		require.NoError(t, err, "Failed to read first data")
		require.Equal(t, len(content), 6000, "Content should be 6000 bytes long")
		assert.Equal(t, string(content[:1024]), string(first[:1024]), "Bytes 0-1024 should match")
	}

	third := make([]byte, 1024)
	// fill with @
	for i := range third {
		third[i] = '@'
	}

	err = sm.WriteFile(filename, third, 1024)
	require.NoError(t, err, "Failed to write third data")

	{
		// check content
		content, err := sm.ReadFile(filename, 0, 6000)
		require.NoError(t, err, "Failed to read first data")
		require.Equal(t, len(content), 6000, "Content should be 6000 bytes long")
		assert.Equal(t, string(content[:1024]), string(first[:1024]), "Bytes 0-1024 should match")
		assert.Equal(t, string(content[1024:2048]), string(third), "Bytes 1024-2048 should match")
		assert.Equal(t, string(content[2048:3000]), string(first[:952]), "Bytes 2048-3000 should match")
		assert.Equal(t, string(content[3000:6000]), string(second), "Bytes 3000-6000 should match")
	}

	fourth := make([]byte, 2000)
	// fill with -
	for i := range fourth {
		fourth[i] = '-'
	}

	err = sm.WriteFile(filename, fourth, 2000)
	require.NoError(t, err, "Failed to write fourth data")

	{
		// check content
		content, err := sm.ReadFile(filename, 0, 6000)
		require.NoError(t, err, "Failed to read first data")
		require.Equal(t, len(content), 6000, "Content should be 6000 bytes long")

		// the final expected content should be:
		// [0, 1024): ****...
		// [1024, 2000): @@@@@...
		// [2000, 4000): ----------
		// [4000, 6000): %%%%...

		assert.Equal(t, string(content[:1024]), string(first[:1024]), "Bytes 0-1024 should match")
		assert.Equal(t, string(content[1024:2000]), string(third[:976]), "Bytes 1024-2000 should match")
		assert.Equal(t, string(content[2000:4000]), string(fourth[:2000]), "Bytes 2000-4000 should match")
		assert.Equal(t, string(content[4000:6000]), string(second[:2000]), "Bytes 4000-6000 should match")
	}
}

func TestReadFileStartingMidChunk(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_read_with_offset"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write initial data - this will be our first chunk
	initialData := []byte("initial data for the file")
	err = sm.WriteFile(filename, initialData, 0)
	require.NoError(t, err, "Failed to write initial data")

	// Create a checkpoint to seal this layer
	err = sm.Checkpoint(filename, "v1")
	require.NoError(t, err, "Failed to checkpoint")

	// Write more data at a later position - this will be our second chunk
	secondData := []byte("second chunk data")
	secondOffset := uint64(50) // Well beyond the first chunk
	err = sm.WriteFile(filename, secondData, secondOffset)
	require.NoError(t, err, "Failed to write second data")

	// Now try to read starting from an offset that's in the middle of the first chunk
	// This should trigger the potential integer underflow in the bufferPos calculation
	readOffset := uint64(10) // After the start of the first chunk
	readSize := uint64(100)  // Long enough to include the second chunk

	// This read would potentially cause the out-of-bounds error if not handled correctly
	data, err := sm.ReadFile(filename, readOffset, readSize)
	require.NoError(t, err, "ReadFile should not fail with offset in the middle of a chunk")

	// Verify we got the expected data (partial first chunk + second chunk)
	expectedFirstPart := initialData[readOffset:]
	expectedData := make([]byte, secondOffset+uint64(len(secondData))-readOffset)

	// Copy the expected partial first chunk
	copy(expectedData, expectedFirstPart)

	// The second chunk should start at offset 50 relative to the file start
	// But in our expectedData slice, it starts at (50 - readOffset)
	secondChunkStartInSlice := secondOffset - readOffset
	copy(expectedData[secondChunkStartInSlice:], secondData)

	// Verify we got what we expected
	assert.Equal(t, expectedData, data, "Retrieved data should match expected partial chunks")
}

func TestConcurrentWrites(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_concurrent_writes"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	wg := sync.WaitGroup{}
	wg.Add(2)

	barrier := sync.WaitGroup{}
	barrier.Add(1)

	go func() {
		defer wg.Done()
		barrier.Wait()
		sm.WriteFile(filename, []byte("hello"), 0)
	}()

	go func() {
		defer wg.Done()
		barrier.Wait()
		sm.WriteFile(filename, []byte("world"), 0)
	}()

	barrier.Done()
	wg.Wait()

	// Check the final content
	content, err := sm.ReadFile(filename, 0, 5)
	require.NoError(t, err, "Failed to read file")

	if string(content) != "hello" && string(content) != "world" {
		t.Fatalf("Content should be 'hello' or 'world', got %s", string(content))
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_concurrent_read_write"
	_, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	wg := sync.WaitGroup{}
	wg.Add(3)

	barrier := sync.WaitGroup{}
	barrier.Add(1)

	var content []byte

	go func() {
		defer wg.Done()
		barrier.Wait()
		sm.WriteFile(filename, []byte("hello"), 0)
	}()

	go func() {
		defer wg.Done()
		barrier.Wait()
		sm.WriteFile(filename, []byte("world"), 0)
	}()

	go func() {
		defer wg.Done()
		barrier.Wait()
		content, _ = sm.ReadFile(filename, 0, 5)
	}()

	barrier.Done()
	wg.Wait()

	if string(content) != "hello" && string(content) != "world" && string(content) != "" {
		t.Fatalf("Content should be 'hello' or 'world' or '', got %s", string(content))
	}
}

func TestConcurrentCheckpoint(t *testing.T) {
	sm, cleanup := quackfstest.SetupStorageManager(t)
	defer cleanup()

	filename := "testfile_concurrent_checkpoint"
	fileID, err := sm.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Write some data to the file
	err = sm.WriteFile(filename, []byte("test data"), 0)
	require.NoError(t, err, "Failed to write data")

	wg := sync.WaitGroup{}
	wg.Add(2)

	var barrier sync.WaitGroup
	barrier.Add(1)

	go func() {
		defer wg.Done()
		barrier.Wait()
		assert.NoError(t, sm.Checkpoint(filename, "v1"))
	}()

	go func() {
		defer wg.Done()
		barrier.Wait()
		assert.NoError(t, sm.Checkpoint(filename, "v2"))
	}()

	barrier.Done()
	wg.Wait()

	// either v1 or v2 should be the current version
	db := quackfstest.SetupDB(t)
	defer db.Close()

	layers, err := sm.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers")
	assert.Equal(t, 1, len(layers), "Should have 1 layer")

	tag := layers[0].Tag
	if tag != "v1" && tag != "v2" {
		t.Fatalf("Tag should be 'v1' or 'v2', got %s", tag)
	}
}

func getVersionIDByTag(t *testing.T, db *sql.DB, tag string) int64 {
	query := `SELECT id FROM versions WHERE tag = $1;`
	var versionID int64
	err := db.QueryRow(query, tag).Scan(&versionID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0
		}
		t.Fatalf("Failed to get version ID for tag %s: %v", tag, err)
	}

	return versionID
}

func getVersionTagByID(t *testing.T, db *sql.DB, id int64) string {
	query := `SELECT tag FROM versions WHERE id = $1;`
	var tag string
	err := db.QueryRow(query, id).Scan(&tag)
	if err != nil {
		if err == sql.ErrNoRows {
			return ""
		}
		t.Fatalf("Failed to get version tag for ID %d: %v", id, err)
	}

	return tag
}
