package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLayerManagerPersistence(t *testing.T) {
	// Create a new metadata store
	ms, cleanup := SetupMetadataStore(t)
	defer cleanup()

	// Create a new LayerManager with the metadata store
	lm, err := NewLayerManager(ms)
	require.NoError(t, err, "Failed to create LayerManager")

	// Write some data and seal a layer
	data1 := []byte("persistent data 1")
	_, offset1, err := lm.Write(data1, 0)
	require.NoError(t, err, "Write error")
	assert.Equal(t, uint64(0), offset1, "Expected first write offset to be 0")

	require.NoError(t, lm.SealActiveLayer(), "Failed to seal layer")

	// Write more data in new active layer
	data2 := []byte("persistent data 2")
	_, offset2, err := lm.Write(data2, offset1+uint64(len(data1)))
	require.NoError(t, err, "Write error")

	expectedOffset2 := uint64(len(data1))
	assert.Equal(t, expectedOffset2, offset2, "Expected second write offset to match first data length")

	// Close the metadata store to simulate a shutdown
	require.NoError(t, ms.Close(), "Failed to close MetadataStore")

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

	// Verify that old data is still present
	read1, err := lm2.GetDataRange(offset1, uint64(len(data1)))
	require.NoError(t, err, "Error reading data1 after reload")
	assert.Equal(t, data1, read1, "Data from first layer should persist after reload")

	// Verify that new data is present
	read2, err := lm2.GetDataRange(offset2, uint64(len(data2)))
	require.NoError(t, err, "Error reading data2 after reload")
	assert.Equal(t, data2, read2, "Data from second layer should persist after reload")
}

func TestFuseScenario(t *testing.T) {
	// Create and mount the FUSE filesystem
	mountDir, cleanup, errChan := SetupFuseMount(t)
	defer cleanup()

	// Explicitly wait for the mount to be ready
	WaitForMount(mountDir, t)

	// The file should be available at mountDir/db.duckdb
	filePath := filepath.Join(mountDir, "db.duckdb")

	// Open the file in write mode (not append) to have more control over writes
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err, "Failed to open file %s", filePath)
	defer f.Close()

	// First, write a known sequence and verify it
	firstWrite := "hello\n"
	n, err := f.WriteString(firstWrite)
	require.NoError(t, err, "First write error")
	require.Equal(t, len(firstWrite), n, "First write should write all bytes")

	// Ensure the data was written correctly by reading directly from layer manager
	expected1 := []byte(firstWrite)
	got, err := globalLM.GetDataRange(0, uint64(len(expected1)))
	require.NoError(t, err, "Error getting content after first write")
	assert.Equal(t, expected1, got, "After first write, content doesn't match expected")

	// Second write - write at the end of the current content
	offset, err := f.Seek(int64(len(firstWrite)), 0)
	require.NoError(t, err, "Seek error before second write")
	require.Equal(t, int64(len(firstWrite)), offset, "Seek position should match first write length")

	secondWrite := "hello world\n"
	n, err = f.WriteString(secondWrite)
	require.NoError(t, err, "Second write error")
	require.Equal(t, len(secondWrite), n, "Second write should write all bytes")

	// Get the combined content after both writes
	expectedLen := len(firstWrite) + len(secondWrite)
	got, err = globalLM.GetDataRange(0, uint64(expectedLen))
	require.NoError(t, err, "Error getting content after second write")

	// Expect the concatenation of both writes
	expected2 := []byte(firstWrite + secondWrite)
	assert.Equal(t, expected2, got, "After second write, content doesn't match expected")

	// Third write - write at the end of the current content
	offset, err = f.Seek(int64(len(firstWrite)+len(secondWrite)), 0)
	require.NoError(t, err, "Seek error before third write")
	require.Equal(t, int64(len(firstWrite)+len(secondWrite)), offset, "Seek position should match combined length")

	thirdWrite := "hel\n"
	n, err = f.WriteString(thirdWrite)
	require.NoError(t, err, "Third write error")
	require.Equal(t, len(thirdWrite), n, "Third write should write all bytes")

	// Get the combined content after all three writes
	expectedFinalLen := len(firstWrite) + len(secondWrite) + len(thirdWrite)
	got, err = globalLM.GetDataRange(0, uint64(expectedFinalLen))
	require.NoError(t, err, "Error getting content after third write")

	// Expect the concatenation of all three writes
	expectedFinal := []byte(firstWrite + secondWrite + thirdWrite)
	assert.Equal(t, expectedFinal, got, "After third write, content doesn't match expected")

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

	// Write a string containing 10 '#' characters and seal the layer
	initialData := []byte("##########")
	_, _, err := lm.Write(initialData, 0)
	require.NoError(t, err, "Initial write error")

	err = lm.SealActiveLayer()
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
			_, offset, err := lm.Write(tc.data, tc.offset)
			require.NoError(t, err, "Write error")
			assert.Equal(t, tc.expectedOffset, offset, "Write offset doesn't match expected value")

			// Check content after modification
			content, err := lm.GetDataRange(0, uint64(len(tc.fullContent)))
			require.NoError(t, err, "Error reading content")
			assert.Equal(t, tc.fullContent, content, "Content doesn't match expected after modification")

			// Seal the layer if specified
			if tc.sealAfter {
				err := lm.SealActiveLayer()
				require.NoError(t, err, "Failed to seal layer")
			}
		})
	}
}
