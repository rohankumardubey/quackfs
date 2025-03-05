package main

import (
	"fmt"
	"slices"
	"sync"
)

// Layer represents an in-memory layer storing appended data.
type Layer struct {
	ID      int
	entries map[uint64][]byte // Maps global offsets to data.
}

// NewLayer creates a new layer with the given unique ID
func NewLayer() *Layer {
	Logger.Debug("Creating new layer")
	return &Layer{
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
	mu       sync.RWMutex   // Primary mutex for protecting all shared state
	metadata *MetadataStore // Metadata store for persistence
}

// NewLayerManager creates (or reloads) a LayerManager using the provided MetadataStore.
func NewLayerManager(store *MetadataStore) (*LayerManager, error) {
	Logger.Debug("Creating/reloading layer manager from metadata store")

	lm := &LayerManager{
		metadata: store,
	}

	// Check if any layers exist, if not create an initial layer
	layers, err := store.LoadLayers()
	if err != nil {
		Logger.Error("Failed to check for existing layers", "error", err)
		return nil, fmt.Errorf("failed to check for existing layers: %w", err)
	}

	// If no layers exist, create an initial layer.
	if len(layers) == 0 {
		if err := lm.createInitialLayer(); err != nil {
			return nil, err
		}
	}

	Logger.Debug("Layer manager initialization complete")
	return lm, nil
}

// createInitialLayer creates the very first layer when no layers exist
func (lm *LayerManager) createInitialLayer() error {
	Logger.Debug("No existing layers found, creating initial layer")
	initial := NewLayer()
	id, err := lm.metadata.RecordNewLayer(initial)
	if err != nil {
		Logger.Error("Failed to record initial layer", "error", err)
		return fmt.Errorf("failed to record initial layer: %w", err)
	}
	Logger.Debug("Initial layer created and recorded", "layerID", id)
	return nil
}

// ActiveLayer returns the current active (last) layer.
func (lm *LayerManager) ActiveLayer() *Layer {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Use the new method to load only the active layer
	active, err := lm.metadata.LoadActiveLayer()
	if err != nil {
		Logger.Error("Failed to load active layer from metadata store", "error", err)
		return nil
	}

	if active == nil {
		Logger.Error("No active layer found in metadata store")
		return nil
	}

	Logger.Debug("Got active layer", "layerID", active.ID, "entryCount", active.EntryCount())
	return active
}

// SealActiveLayer seals the current active layer and creates a new active layer.
// It updates the metadata store accordingly.
func (lm *LayerManager) SealActiveLayer() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Get the current active layer ID
	layers, err := lm.metadata.LoadLayers()
	if err != nil {
		Logger.Error("Failed to load layers from metadata store", "error", err)
		return fmt.Errorf("failed to load layers: %w", err)
	}

	if len(layers) == 0 {
		return fmt.Errorf("no layers found to seal")
	}

	current := layers[len(layers)-1]
	Logger.Debug("Sealing active layer", "layerID", current.ID, "entryCount", current.EntryCount())

	if err := lm.metadata.SealLayer(current.ID); err != nil {
		Logger.Error("Failed to seal layer in metadata store", "layerID", current.ID, "error", err)
		return fmt.Errorf("failed to seal layer %d: %w", current.ID, err)
	}

	newLayer := NewLayer()
	id, err := lm.metadata.RecordNewLayer(newLayer)

	if err != nil {
		Logger.Error("Failed to record new layer", "error", err)
		return fmt.Errorf("failed to record new layer: %w", err)
	}

	Logger.Debug("Created new active layer after sealing", "newLayerID", id)

	return nil
}

// Write writes data to the active layer at the specified global offset.
// It returns the active layer's ID and the offset where the data was written.
func (lm *LayerManager) Write(data []byte, offset uint64) (layerID int, writtenOffset uint64, err error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	Logger.Debug("Writing data", "size", len(data), "requestedOffset", offset)

	// Get the active layer
	layers, err := lm.metadata.LoadLayers()
	if err != nil {
		Logger.Error("Failed to load layers from metadata store", "error", err)
		return 0, 0, fmt.Errorf("failed to load layers: %w", err)
	}

	if len(layers) == 0 {
		return 0, 0, fmt.Errorf("no active layer found")
	}

	active := layers[len(layers)-1]

	// Use the requested offset
	writtenOffset = offset

	// Add the entry to the active layer
	active.AddEntry(writtenOffset, data)
	Logger.Debug("Added entry to active layer", "layerID", active.ID, "offset", writtenOffset, "dataSize", len(data))

	// Calculate ranges and record entry in metadata store
	layerRange, fileRange, err := lm.calculateRanges(active, writtenOffset, len(data))

	if err != nil {
		Logger.Errorf("Failed to calculate ranges: %v", err)
		return 0, 0, fmt.Errorf("failed to calculate ranges: %w", err)
	}

	if err = lm.metadata.RecordEntry(active.ID, writtenOffset, data, layerRange, fileRange); err != nil {
		Logger.Error("Failed to record entry", "layerID", active.ID, "offset", writtenOffset, "error", err)
		return 0, 0, fmt.Errorf("failed to record entry: %w", err)
	}

	Logger.Debug("Data written successfully", "layerID", active.ID, "offset", writtenOffset, "size", len(data))
	return active.ID, writtenOffset, nil
}

// calculateRanges computes the layer-relative and file-absolute ranges for an entry
func (lm *LayerManager) calculateRanges(layer *Layer, offset uint64, dataSize int) ([2]uint64, [2]uint64, error) {
	dataLength := uint64(dataSize)

	// Retrieve the layer base from the metadata store.
	baseOffset, err := lm.metadata.GetLayerBase(layer.ID)
	if err != nil {
		Logger.Error("Failed to retrieve layer base", "layerID", layer.ID, "error", err)
		return [2]uint64{}, [2]uint64{}, fmt.Errorf("failed to retrieve layer base: %w", err)
	}

	var layerStart uint64
	if offset >= baseOffset {
		layerStart = offset - baseOffset
	} else {
		layerStart = 0
		Logger.Warn("Write offset is before layer base", "offset", offset, "layerBase", baseOffset, "layerID", layer.ID)
	}
	layerEnd := layerStart + dataLength
	layerRange := [2]uint64{layerStart, layerEnd}

	// File range remains the global offset range.
	fileStart := offset
	fileEnd := offset + dataLength
	fileRange := [2]uint64{fileStart, fileEnd}

	Logger.Debug("Calculated ranges for entry",
		"layerID", layer.ID,
		"layerRange", layerRange,
		"fileRange", fileRange)

	return layerRange, fileRange, nil
}

func (lm *LayerManager) FileSize() (uint64, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	return lm.metadata.CalculateVirtualFileSize()
}

// GetFullContent merges all layers with overlay semantics.
func (lm *LayerManager) GetFullContent() []byte {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	Logger.Debug("Getting full content from database")

	// Load all layers
	layers, err := lm.metadata.LoadLayers()
	if err != nil {
		Logger.Error("Failed to load layers from metadata store", "error", err)
		return []byte{}
	}

	// Load all entries
	entriesMap, err := lm.metadata.LoadEntries()
	if err != nil {
		Logger.Error("Failed to load entries from metadata store", "error", err)
		return []byte{}
	}

	// Calculate maximum size by finding the highest offset + data length
	var maxSize uint64 = 0

	for _, layer := range layers {
		if entries, ok := entriesMap[layer.ID]; ok {
			for _, entry := range entries {
				endOffset := entry.Offset + uint64(len(entry.Data))
				if endOffset > maxSize {
					maxSize = endOffset
				}
			}
		}
	}

	// Create buffer of appropriate size
	buf := make([]byte, maxSize)

	// Merge layers in order (later layers override earlier ones)
	for _, layer := range layers {
		if entries, ok := entriesMap[layer.ID]; ok {
			// Sort entries by offset for consistent application
			for _, entry := range entries {
				if entry.Offset+uint64(len(entry.Data)) <= uint64(len(buf)) {
					copy(buf[entry.Offset:entry.Offset+uint64(len(entry.Data))], entry.Data)
				}
			}
		}
	}

	Logger.Debug("Full content retrieved", "size", len(buf))
	return buf
}

// GetDataRange returns a slice of data from the given offset up to size bytes.
func (lm *LayerManager) GetDataRange(offset uint64, size uint64) ([]byte, error) {
	Logger.Debug("Getting data range from database", "offset", offset, "requestedSize", size)

	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Get full content (this could be optimized to only get the needed range)
	fullContent := lm.GetFullContent()

	if offset >= uint64(len(fullContent)) {
		Logger.Debug("Requested offset beyond content size", "offset", offset, "contentSize", len(fullContent))
		return []byte{}, nil
	}

	end := min(offset+size, uint64(len(fullContent)))

	// Create a copy of the slice to prevent race conditions
	result := make([]byte, end-offset)
	copy(result, fullContent[offset:end])

	Logger.Debug("Returning data range", "offset", offset, "end", end, "returnedSize", end-offset)
	return result, nil
}
