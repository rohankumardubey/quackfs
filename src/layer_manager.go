package main

import (
	"fmt"
	"slices"
	"sync"
)

// Layer represents an in-memory layer storing appended data.
type Layer struct {
	ID      int
	base    uint64            // Global offset where this layer starts.
	entries map[uint64][]byte // Maps global offsets to data.
}

// NewLayer creates a new layer with the given unique ID and base offset.
func NewLayer(id int, base uint64) *Layer {
	Logger.Debug("Creating new layer", "id", id, "baseOffset", base)
	return &Layer{
		ID:      id,
		base:    base,
		entries: make(map[uint64][]byte),
	}
}

// AddEntry adds data at the specified offset within the layer
func (l *Layer) AddEntry(offset uint64, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	l.entries[offset] = dataCopy
}

// GetEntry returns the data at the specified offset, or nil if none exists
func (l *Layer) GetEntry(offset uint64) []byte {
	return l.entries[offset]
}

// GetSortedOffsets returns all offsets in this layer in ascending order
func (l *Layer) GetSortedOffsets() []uint64 {
	offsets := make([]uint64, 0, len(l.entries))
	for offset := range l.entries {
		offsets = append(offsets, offset)
	}
	slices.Sort(offsets)
	return offsets
}

// EntryCount returns the number of entries in this layer
func (l *Layer) EntryCount() int {
	return len(l.entries)
}

// LayerManager manages multiple layers and a global offset.
// It uses a MetadataStore to persist layer metadata and entry data.
type LayerManager struct {
	mu           sync.RWMutex   // Primary mutex for protecting all shared state
	layers       []*Layer       // All layers managed by this instance
	nextID       int            // Next ID to assign to a new layer
	globalOffset uint64         // Current global offset
	metadata     *MetadataStore // Metadata store for persistence

	// Cache-related fields
	contentCache []byte // Content cache protected by mu
	cacheValid   bool   // Whether the cache is valid, protected by mu
}

// NewLayerManager creates (or reloads) a LayerManager using the provided MetadataStore.
func NewLayerManager(store *MetadataStore) (*LayerManager, error) {
	Logger.Debug("Creating/reloading layer manager from metadata store")
	layers, nextID, baseGlobalOffset, err := store.LoadLayers()
	if err != nil {
		Logger.Error("Failed to load layers from metadata store", "error", err)
		return nil, fmt.Errorf("failed to load layers: %w", err)
	}

	Logger.Debug("Loaded layer information", "layerCount", len(layers), "nextID", nextID, "baseGlobalOffset", baseGlobalOffset)

	lm := &LayerManager{
		layers:       layers,
		nextID:       nextID,
		globalOffset: baseGlobalOffset,
		metadata:     store,
		cacheValid:   false,
	}

	// If no layers exist, create an initial layer.
	if len(layers) == 0 {
		if err := lm.createInitialLayer(); err != nil {
			return nil, err
		}
	}

	// Load entries and rehydrate layers.
	if err := lm.loadEntries(); err != nil {
		return nil, err
	}

	Logger.Debug("Layer manager initialization complete", "totalLayers", len(lm.layers), "globalOffset", lm.globalOffset)
	return lm, nil
}

// createInitialLayer creates the very first layer when no layers exist
func (lm *LayerManager) createInitialLayer() error {
	Logger.Debug("No existing layers found, creating initial layer")
	initial := NewLayer(1, 0)
	lm.layers = append(lm.layers, initial)
	lm.nextID = 2
	lm.globalOffset = 0
	if err := lm.metadata.RecordNewLayer(initial); err != nil {
		Logger.Error("Failed to record initial layer", "error", err)
		return fmt.Errorf("failed to record initial layer: %w", err)
	}
	Logger.Debug("Initial layer created and recorded", "layerID", initial.ID)
	return nil
}

// loadEntries loads all entries from the metadata store and populates the layers
func (lm *LayerManager) loadEntries() error {
	Logger.Debug("Loading entries from metadata store")
	entries, err := lm.metadata.LoadEntries()
	if err != nil {
		Logger.Error("Failed to load entries from metadata store", "error", err)
		return fmt.Errorf("failed to load entries: %w", err)
	}

	entriesLoaded := 0
	for _, layer := range lm.layers {
		if entryList, ok := entries[layer.ID]; ok {
			Logger.Debug("Populating layer with entries", "layerID", layer.ID, "entryCount", len(entryList))
			for _, entry := range entryList {
				layer.AddEntry(entry.Offset, entry.Data)
				candidate := entry.Offset + uint64(len(entry.Data))
				if candidate > lm.globalOffset {
					lm.globalOffset = candidate
					Logger.Debug("Updated global offset", "layerID", layer.ID, "newOffset", lm.globalOffset)
				}
				entriesLoaded++
			}
		} else {
			Logger.Debug("No entries found for layer", "layerID", layer.ID)
		}
	}

	Logger.Debug("Entries loaded successfully", "totalEntries", entriesLoaded)
	return nil
}

// ActiveLayer returns the current active (last) layer.
func (lm *LayerManager) ActiveLayer() *Layer {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	active := lm.layers[len(lm.layers)-1]
	Logger.Debug("Getting active layer", "layerID", active.ID, "baseOffset", active.base, "entryCount", active.EntryCount())
	return active
}

// SealActiveLayer seals the current active layer and creates a new active layer.
// It updates the metadata store accordingly.
func (lm *LayerManager) SealActiveLayer() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Invalidate cache
	lm.cacheValid = false

	current := lm.layers[len(lm.layers)-1]
	Logger.Debug("Sealing active layer", "layerID", current.ID, "entryCount", current.EntryCount())

	if err := lm.metadata.SealLayer(current.ID); err != nil {
		Logger.Error("Failed to seal layer in metadata store", "layerID", current.ID, "error", err)
		return fmt.Errorf("failed to seal layer %d: %w", current.ID, err)
	}

	newLayer := NewLayer(lm.nextID, lm.globalOffset)
	lm.layers = append(lm.layers, newLayer)
	lm.nextID++

	Logger.Debug("Created new active layer after sealing", "newLayerID", newLayer.ID, "baseOffset", newLayer.base, "totalLayers", len(lm.layers))

	if err := lm.metadata.RecordNewLayer(newLayer); err != nil {
		Logger.Error("Failed to record new layer", "layerID", newLayer.ID, "error", err)
		return fmt.Errorf("failed to record new layer %d: %w", newLayer.ID, err)
	}

	return nil
}

// Write writes data to the active layer at the specified global offset.
// It returns the active layer's ID and the offset where the data was written.
func (lm *LayerManager) Write(data []byte, offset uint64) (layerID int, writtenOffset uint64, err error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Invalidate cache
	lm.cacheValid = false

	Logger.Debug("Writing data", "size", len(data), "requestedOffset", offset)
	active := lm.layers[len(lm.layers)-1]

	// Use the requested offset
	writtenOffset = offset

	// Add the entry to the active layer
	active.AddEntry(writtenOffset, data)
	Logger.Debug("Added entry to active layer", "layerID", active.ID, "offset", writtenOffset, "dataSize", len(data))

	// Calculate ranges and record entry in metadata store
	layerRange, fileRange := lm.calculateRanges(active, writtenOffset, len(data))

	if err = lm.metadata.RecordEntry(active.ID, writtenOffset, data, layerRange, fileRange); err != nil {
		Logger.Error("Failed to record entry", "layerID", active.ID, "offset", writtenOffset, "error", err)
		return 0, 0, fmt.Errorf("failed to record entry: %w", err)
	}

	// Update global offset if needed
	oldOffset := lm.globalOffset
	lm.globalOffset = max(lm.globalOffset, writtenOffset+uint64(len(data)))
	if oldOffset != lm.globalOffset {
		Logger.Debug("Global offset updated", "oldOffset", oldOffset, "newOffset", lm.globalOffset)
	}

	Logger.Debug("Data written successfully", "layerID", active.ID, "offset", writtenOffset, "size", len(data))
	return active.ID, writtenOffset, nil
}

// calculateRanges computes the layer-relative and file-absolute ranges for an entry
func (lm *LayerManager) calculateRanges(layer *Layer, offset uint64, dataSize int) ([2]uint64, [2]uint64) {
	dataLength := uint64(dataSize)

	// Layer range is relative to the layer's base offset
	var layerStart uint64
	if offset >= layer.base {
		layerStart = offset - layer.base
	} else {
		// This shouldn't happen with proper layer management
		// but protect against potential underflow
		layerStart = 0
		Logger.Warn("Write offset is before layer base", "offset", offset, "layerBase", layer.base, "layerID", layer.ID)
	}
	layerEnd := layerStart + dataLength
	layerRange := [2]uint64{layerStart, layerEnd}

	// File range is the global offset range
	fileStart := offset
	fileEnd := offset + dataLength
	fileRange := [2]uint64{fileStart, fileEnd}

	Logger.Debug("Calculated ranges for entry",
		"layerID", layer.ID,
		"layerRange", layerRange,
		"fileRange", fileRange)

	return layerRange, fileRange
}

// updateCache generates a new content buffer under the protection of mu lock
func (lm *LayerManager) updateCache() {
	// Note: Caller must hold lm.mu.Lock (write or read lock)
	Logger.Debug("Regenerating content cache", "globalOffset", lm.globalOffset, "layerCount", len(lm.layers))

	buf := make([]byte, lm.globalOffset)
	totalEntries := 0

	for _, layer := range lm.layers {
		offsets := layer.GetSortedOffsets()
		Logger.Debug("Merging layer into content cache", "layerID", layer.ID, "entryCount", len(offsets))

		for _, off := range offsets {
			data := layer.GetEntry(off)
			// Ensure we don't go out of bounds
			if off+uint64(len(data)) <= uint64(len(buf)) {
				copy(buf[off:off+uint64(len(data))], data)
				totalEntries++
			} else {
				Logger.Warn("Skipping entry that would exceed buffer bounds",
					"offset", off, "dataSize", len(data), "bufSize", len(buf))
			}
		}
	}

	// Store the generated buffer in the cache
	lm.contentCache = buf
	lm.cacheValid = true

	Logger.Debug("Content cache regenerated", "size", len(buf), "totalEntriesProcessed", totalEntries)
}

// GetFullContent merges all layers with overlay semantics.
func (lm *LayerManager) GetFullContent() []byte {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// If cache is not valid, update it
	if !lm.cacheValid {
		lm.updateCache()
	}

	// Create a defensive copy to prevent external modifications
	result := make([]byte, len(lm.contentCache))
	copy(result, lm.contentCache)

	return result
}

// GetDataRange returns a slice of data from the given offset up to size bytes.
func (lm *LayerManager) GetDataRange(offset uint64, size uint64) ([]byte, error) {
	Logger.Debug("Getting data range", "offset", offset, "requestedSize", size)

	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// If cache is not valid, update it
	if !lm.cacheValid {
		lm.updateCache()
	}

	// Get the content from cache (already protected by the RLock)
	if offset >= uint64(len(lm.contentCache)) {
		Logger.Debug("Requested offset beyond content size", "offset", offset, "contentSize", len(lm.contentCache))
		return []byte{}, nil
	}

	end := min(offset+size, uint64(len(lm.contentCache)))

	// Create a copy of the slice to prevent race conditions
	result := make([]byte, end-offset)
	copy(result, lm.contentCache[offset:end])

	Logger.Debug("Returning data range", "offset", offset, "end", end, "returnedSize", end-offset)
	return result, nil
}

// GetCacheState returns a copy of the current cache and its validity.
// This method is intended for testing purposes.
func (lm *LayerManager) GetCacheState() ([]byte, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	cacheCopy := make([]byte, len(lm.contentCache))
	copy(cacheCopy, lm.contentCache)
	return cacheCopy, lm.cacheValid
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
