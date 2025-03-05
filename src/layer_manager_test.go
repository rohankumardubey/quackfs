package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadActiveLayer(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	input := []byte("hello world")
	layerID, offset, err := lm.Write(input, 0)
	require.NoError(t, err, "Write error")
	assert.Equal(t, uint64(0), offset, "Initial write offset should be 0")

	fullContent := lm.GetFullContent()
	assert.Equal(t, len(input), len(fullContent), "Full content length should match input length")

	data, err := lm.GetDataRange(offset, uint64(len(input)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input, data, "Retrieved data should match input")

	activeLayer := lm.ActiveLayer()
	require.NotNil(t, activeLayer, "Active layer should not be nil")
	assert.Equal(t, activeLayer.ID, layerID, "Write should return the active layer's ID")
}

func TestSealLayerNewActiveLayer(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	input1 := []byte("data1")
	layerID1, offset1, err := lm.Write(input1, 0)
	require.NoError(t, err, "Write error")
	assert.Equal(t, uint64(0), offset1, "First write offset in active layer should be 0")

	err = lm.SealActiveLayer()
	require.NoError(t, err, "SealActiveLayer failed")

	active := lm.ActiveLayer()
	require.NotNil(t, active, "Active layer should not be nil")
	assert.Equal(t, layerID1+1, active.ID, "Active layer should change after sealing")

	input2 := []byte("data2")
	layerID2, offset2, err := lm.Write(input2, uint64(len(input1)))
	require.NoError(t, err, "Write error")
	require.Equal(t, layerID2, active.ID, "Second write should be in new active layer")

	data, err := lm.GetDataRange(offset2, uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error")
	assert.Equal(t, input2, data, "Retrieved data should match second input")
}

func TestReadFromSealedLayer(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	input1 := []byte("old data")
	_, offset1, err := lm.Write(input1, 0)
	require.NoError(t, err, "Write error")

	err = lm.SealActiveLayer()
	require.NoError(t, err, "SealActiveLayer failed")

	input2 := []byte("new data")
	expectedOffset2 := uint64(len(input1))
	_, offset2, err := lm.Write(input2, expectedOffset2)
	require.NoError(t, err, "Write error")
	assert.Equal(t, expectedOffset2, offset2, "Second write offset should match first data length")

	// Check data from sealed layer
	data, err := lm.GetDataRange(offset1, uint64(len(input1)))
	require.NoError(t, err, "GetDataRange error for sealed layer data")
	assert.Equal(t, input1, data, "Data from sealed layer should be retrievable")

	// Check data from active layer
	data, err = lm.GetDataRange(offset2, uint64(len(input2)))
	require.NoError(t, err, "GetDataRange error for active layer data")
	assert.Equal(t, input2, data, "Data from active layer should be retrievable")
}

func TestPartialRead(t *testing.T) {
	lm, cleanup := SetupLayerManager(t)
	defer cleanup()

	input := []byte("partial read test")
	_, offset, err := lm.Write(input, 0)
	require.NoError(t, err, "Write error")

	partialSize := uint64(7)
	data, err := lm.GetDataRange(offset, partialSize)
	require.NoError(t, err, "GetDataRange error")

	assert.Equal(t, int(partialSize), len(data), "Partial read should return requested length")
	assert.Equal(t, input[:partialSize], data, "Partial read should return correct data slice")
}
