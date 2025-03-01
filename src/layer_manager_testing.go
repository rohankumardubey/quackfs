//go:build testing
// +build testing

package main

// GetCacheState returns a copy of the current cache and its validity.
// This function is only included in builds with the "testing" tag.
func (lm *LayerManager) GetCacheState() ([]byte, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	cacheCopy := make([]byte, len(lm.contentCache))
	copy(cacheCopy, lm.contentCache)
	return cacheCopy, lm.cacheValid
}
