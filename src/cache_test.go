package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLayerManagerCaching(t *testing.T) {
	// Create metadata store and layer manager
	metaStore, err := NewMetadataStore(GetTestConnectionString(t))
	require.NoError(t, err)
	defer metaStore.Close()

	lm, err := NewLayerManager(metaStore)
	require.NoError(t, err)

	// Test 1: Initial state - cache should be invalid
	_, cacheValid := lm.GetCacheState()
	assert.False(t, cacheValid, "Cache should be invalid initially")

	// Test 2: Get content should generate cache
	content := lm.GetFullContent()
	assert.NotNil(t, content, "Should get content")

	cache, cacheValid := lm.GetCacheState()
	assert.True(t, cacheValid, "Cache should be valid after GetFullContent()")
	assert.Equal(t, content, cache, "Cache should match returned content")

	// Test 3: Writing data should invalidate cache
	lm.Write([]byte("test data"), 0)
	_, cacheValid = lm.GetCacheState()
	assert.False(t, cacheValid, "Cache should be invalid after Write()")

	// Test 4: GetDataRange should regenerate cache
	dataRange, err := lm.GetDataRange(0, 9)
	require.NoError(t, err)
	assert.Equal(t, []byte("test data"), dataRange, "Should get correct data range")

	_, cacheValid = lm.GetCacheState()
	assert.True(t, cacheValid, "Cache should be valid after GetDataRange()")

	// Test 5: Multiple reads should reuse cache without regeneration
	// Store the cache for comparison
	beforeCache, _ := lm.GetCacheState()

	// Access content multiple times
	lm.GetFullContent()
	lm.GetDataRange(0, 4)

	// Get the cache again to see if it changed
	afterCache, _ := lm.GetCacheState()

	// Verify same cache instance is used (pointer comparison would be better
	// but we're working with copies due to reflection)
	assert.Equal(t, beforeCache, afterCache, "Cache should be reused for multiple reads")

	// Test 6: Sealing should invalidate cache
	err = lm.SealActiveLayer()
	require.NoError(t, err)

	_, cacheValid = lm.GetCacheState()
	assert.False(t, cacheValid, "Cache should be invalid after SealActiveLayer()")
}
