package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLayerManagerPersistence(t *testing.T) {
	// Setup a metadata store and layer manager
	ms, cleanup := SetupMetadataStore(t)
	defer cleanup()

	lm, err := NewLayerManager(ms)
	require.NoError(t, err, "Failed to create LayerManager")

	// Create a test file
	filename := "test_persistence_file"

	// Insert the file explicitly
	fileID, err := ms.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")
	t.Logf("Inserted file with ID: %d", fileID)

	// Write some data and seal a layer
	data1 := []byte("hello world")
	layerID, offset1, err := lm.Write(filename, data1, 0)
	require.NoError(t, err, "Failed to write data1")
	t.Logf("Wrote data1 to layer ID: %d", layerID)

	// Verify data was written correctly
	readData, err := lm.GetDataRange(filename, offset1, uint64(len(data1)))
	require.NoError(t, err, "Failed to read data1 after initial write")
	assert.Equal(t, data1, readData, "Data1 should be readable immediately after write")

	err = lm.SealActiveLayer(filename)
	require.NoError(t, err, "Failed to seal layer")

	// Write more data in new active layer
	data2 := []byte("more data")
	layerID2, offset2, err := lm.Write(filename, data2, uint64(len(data1)))
	require.NoError(t, err, "Failed to write data2")
	t.Logf("Wrote data2 to layer ID: %d", layerID2)

	expectedOffset2 := uint64(len(data1))
	assert.Equal(t, expectedOffset2, offset2, "Second write offset should match first data length")

	// Verify data2 was written correctly
	readData2, err := lm.GetDataRange(filename, offset2, uint64(len(data2)))
	require.NoError(t, err, "Failed to read data2 after initial write")
	assert.Equal(t, data2, readData2, "Data2 should be readable immediately after write")

	// Close the metadata store to simulate a shutdown
	err = ms.Close()
	require.NoError(t, err, "Failed to close metadata store")

	// "Restart" the system by reopening the metadata store
	ms2, err := NewMetadataStore(GetTestConnectionString(t))
	require.NoError(t, err, "Failed to reopen MetadataStore")
	defer ms2.Close()

	// Reload the layer manager from the metadata store
	lm2, err := NewLayerManager(ms2)
	require.NoError(t, err, "Failed to reload LayerManager")

	// Check that we have at least 2 layers by querying the database
	layers, err := ms2.LoadLayers()
	require.NoError(t, err, "Failed to load layers from metadata store")
	assert.GreaterOrEqual(t, len(layers), 2, "Expected at least 2 layers after reload")

	// Debug: print all layers
	t.Logf("Found %d layers after reload", len(layers))
	for i, layer := range layers {
		t.Logf("Layer %d: ID=%d, FileID=%d, Sealed=%v", i, layer.ID, layer.FileID, layer.Sealed)
	}

	// Debug: check if the file exists
	fileID2, err := ms2.GetFileIDByName(filename)
	if err != nil {
		t.Logf("Error getting file ID: %v", err)
	} else {
		t.Logf("Found file with ID: %d", fileID2)
	}

	// Verify that old data is still present
	read1, err := lm2.GetDataRange(filename, offset1, uint64(len(data1)))
	require.NoError(t, err, "Error reading data1 after reload")
	t.Logf("Read1 data: %v", read1)
	assert.Equal(t, data1, read1, "Data from first layer should persist after reload")

	// Verify that new data is present
	read2, err := lm2.GetDataRange(filename, offset2, uint64(len(data2)))
	require.NoError(t, err, "Error reading data2 after reload")
	assert.Equal(t, data2, read2, "Data from second layer should persist after reload")
}

func TestFuseScenario(t *testing.T) {
	// Create and mount the FUSE filesystem
	mountDir, cleanup, errChan := SetupFuseMount(t)
	defer cleanup()

	// Explicitly wait for the mount to be ready
	WaitForMount(mountDir, t)

	// Create a test file name
	filename := "test_fuse_file"

	// The file should be available at mountDir/test_fuse_file
	filePath := filepath.Join(mountDir, filename)

	// Open the file in write mode (not append) to have more control over writes
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err, "Failed to open file %s", filePath)
	defer f.Close()

	// First, write a known sequence and verify it
	firstWrite := "hello "
	n, err := f.WriteString(firstWrite)
	require.NoError(t, err, "First write error")
	require.Equal(t, len(firstWrite), n, "First write length mismatch")

	// Ensure the data was written correctly by reading directly from layer manager
	expected1 := []byte(firstWrite)
	got, err := globalLM.GetDataRange(filename, 0, uint64(len(expected1)))
	require.NoError(t, err, "Error getting content after first write")
	assert.Equal(t, expected1, got, "After first write, content doesn't match expected")

	// Write a second sequence
	secondWrite := "hello world\n"
	n, err = f.WriteString(secondWrite)
	require.NoError(t, err, "Second write error")
	require.Equal(t, len(secondWrite), n, "Second write length mismatch")

	// Get the combined content after both writes
	expectedLen := len(firstWrite) + len(secondWrite)
	got, err = globalLM.GetDataRange(filename, 0, uint64(expectedLen))
	require.NoError(t, err, "Error getting content after second write")

	// The expected content is the concatenation of both writes
	expected2 := append([]byte(firstWrite), []byte(secondWrite)...)
	assert.Equal(t, expected2, got, "After second write, content doesn't match expected")

	// Write a third sequence
	thirdWrite := "hel\n"
	n, err = f.WriteString(thirdWrite)
	require.NoError(t, err, "Third write error")
	require.Equal(t, len(thirdWrite), n, "Third write length mismatch")

	// Get the combined content after all three writes
	expectedFinalLen := len(firstWrite) + len(secondWrite) + len(thirdWrite)
	got, err = globalLM.GetDataRange(filename, 0, uint64(expectedFinalLen))
	require.NoError(t, err, "Error getting content after third write")

	// The expected content is the concatenation of all three writes
	expected3 := append(expected2, []byte(thirdWrite)...)
	assert.Equal(t, expected3, got, "After third write, content doesn't match expected")

	// Check for FUSE errors that might have occurred during the test
	select {
	case err := <-errChan:
		assert.NoError(t, err, "FUSE error during test")
	default:
		// No errors
	}
}

// TestWritingAtDifferentOffsets refactored to use table-driven testing
func TestWritingAtDifferentOffsets(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	// Create a test file name
	filename := "test_offsets_file"

	// Insert the file, which should create an initial unsealed layer
	fileID, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")

	// Load layers for the file
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	require.NoError(t, err, "Failed to load layers for file")
	require.Equal(t, 1, len(layers), "Expected one initial layer")

	// Write a string containing 10 '#' characters and seal the layer
	initialData := []byte("##########")
	_, _, err = lm.Write(filename, initialData, 0)
	require.NoError(t, err, "Initial write error")

	err = lm.SealActiveLayer(filename)
	require.NoError(t, err, "Failed to seal layer")

	// Table-driven test cases for modifications
	tests := []struct {
		name           string
		data           []byte
		offset         uint64
		expectedOffset uint64
		fullContent    []byte // Expected content after this modification
		sealAfter      bool   // Whether to seal the layer after this modification
	}{
		{
			name:           "Write @@@s at offset 2",
			data:           []byte("@@@"),
			offset:         2,
			expectedOffset: 2,
			fullContent:    []byte("##@@@#####"),
			sealAfter:      false,
		},
		{
			name:           "Write ***s at offset 7",
			data:           []byte("***"),
			offset:         7,
			expectedOffset: 7,
			fullContent:    []byte("##@@@##***"),
			sealAfter:      true,
		},
		{
			name:           "Write aas at offset 0",
			data:           []byte("aa"),
			offset:         0,
			expectedOffset: 0,
			fullContent:    []byte("aa@@@##***"),
			sealAfter:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, offset, err := lm.Write(filename, tc.data, tc.offset)
			require.NoError(t, err, "Write error")
			assert.Equal(t, tc.expectedOffset, offset, "Write offset doesn't match expected value")

			// Check content after modification
			content, err := lm.GetDataRange(filename, 0, uint64(len(tc.fullContent)))
			require.NoError(t, err, "Error reading content")
			assert.Equal(t, tc.fullContent, content, "Content doesn't match expected after modification")

			// Seal the layer if specified
			if tc.sealAfter {
				err := lm.SealActiveLayer(filename)
				require.NoError(t, err, "Failed to seal layer")
			}
		})
	}
}

func TestCalculateVirtualFileSize(t *testing.T) {
	ms, cleanup := SetupMetadataStore(t)
	defer cleanup()

	lm, err := NewLayerManager(ms)
	require.NoError(t, err, "Failed to create layer manager")

	// Create a test file name
	filename := "test_filesize_file"

	// Insert the file
	fileID, err := lm.metadata.InsertFile(filename)
	require.NoError(t, err, "Failed to insert file")
	t.Logf("Inserted file with ID: %d", fileID)

	// Write data at different offsets
	data1 := []byte("data at offset 0")
	layerID1, _, err := lm.Write(filename, data1, 0)
	require.NoError(t, err, "Failed to write at offset 0")
	t.Logf("Wrote data1 to layer ID: %d", layerID1)

	data2 := []byte("data at offset 20")
	layerID2, _, err := lm.Write(filename, data2, 20)
	require.NoError(t, err, "Failed to write at offset 20")
	t.Logf("Wrote data2 to layer ID: %d", layerID2)

	// Calculate virtual file size
	size, err := lm.metadata.CalculateVirtualFileSize(fileID)
	require.NoError(t, err, "Failed to calculate virtual file size")
	t.Logf("Calculated virtual file size: %d", size)

	// The expected size is the end of the last write: offset 20 + length of "data at offset 20"
	expectedSize := uint64(20 + len(data2))
	t.Logf("Expected virtual file size: %d", expectedSize)
	assert.Equal(t, expectedSize, size, "Unexpected virtual file size")
}

func TestDeleteFile(t *testing.T) {
	ms, cleanup := SetupMetadataStore(t)
	defer cleanup()

	// Insert a test file
	fileName := "testfile"
	fileID, err := ms.InsertFile(fileName)
	if err != nil {
		t.Fatalf("Failed to insert test file: %v", err)
	}

	// Create a layer for the file
	layer := NewLayer(fileID)
	layerID, err := ms.RecordNewLayer(layer)
	if err != nil {
		t.Fatalf("Failed to create layer for test file: %v", err)
	}

	// Insert an entry for the layer
	entryData := []byte("test data")
	entryOffset := uint64(0)
	entryLayerRange := [2]uint64{0, uint64(len(entryData))}
	entryFileRange := [2]uint64{0, uint64(len(entryData))}
	err = ms.RecordEntry(layerID, entryOffset, entryData, entryLayerRange, entryFileRange)
	if err != nil {
		t.Fatalf("Failed to insert entry for test file: %v", err)
	}

	// Verify the file exists
	retrievedFileID, err := ms.GetFileIDByName(fileName)
	if err != nil || retrievedFileID != fileID {
		t.Fatalf("Failed to retrieve test file ID: %v", err)
	}

	// Delete the file
	err = ms.DeleteFile(fileName)
	if err != nil {
		t.Fatalf("Failed to delete test file: %v", err)
	}

	// Verify the file is deleted
	retrievedFileID, err = ms.GetFileIDByName(fileName)
	if err != nil || retrievedFileID != 0 {
		t.Fatalf("Test file was not deleted: %v", err)
	}

	// Verify no layers exist for the file
	layers, err := ms.LoadLayersByFileID(fileID)
	if err != nil || len(layers) != 0 {
		t.Fatalf("Layers were not deleted for test file: %v", err)
	}

	// Verify no entries exist for the layers
	entries, err := ms.LoadEntries()
	if err != nil || len(entries[layerID]) != 0 {
		t.Fatalf("Entries were not deleted for test file: %v", err)
	}
}
