package main

import (
	"fmt"
	"slices"
	"sync"
)

// Layer represents an in-memory layer storing appended data.
type Layer struct {
	ID      int
	FileID  int               // New attribute to associate layer with a specific file
	Sealed  bool              // New field to track if the layer is sealed
	entries map[uint64][]byte // Maps global offsets to data.
}

// NewLayer creates a new layer with the given unique ID and file ID
func NewLayer(fileID int) *Layer {
	Logger.Debug("Creating new layer")
	return &Layer{
		FileID:  fileID,
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

	// If no layers exist, create an initial layer with a default fileID.
	if len(layers) == 0 {
		initialFileID := 0 // Use a default fileID for the initial layer
		if err := lm.createInitialLayer(initialFileID); err != nil {
			return nil, err
		}
	}

	Logger.Debug("Layer manager initialization complete")
	return lm, nil
}

// createInitialLayer creates the very first layer when no layers exist
func (lm *LayerManager) createInitialLayer(fileID int) error {
	Logger.Debug("No existing layers found, creating initial layer")
	initial := NewLayer(fileID)
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
func (lm *LayerManager) SealActiveLayer(fileName string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Get the file ID from the file name
	fileID, err := lm.metadata.GetFileIDByName(fileName)
	if err != nil {
		Logger.Error("Failed to get file ID", "fileName", fileName, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

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

	newLayer := NewLayer(fileID)
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
func (lm *LayerManager) Write(fileName string, data []byte, offset uint64) (layerID int, writtenOffset uint64, err error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	Logger.Debug("Writing data", "fileName", fileName, "size", len(data), "requestedOffset", offset)

	// Get the file ID from the file name
	fileID, err := lm.metadata.GetFileIDByName(fileName)
	if err != nil {
		Logger.Error("Failed to get file ID", "fileName", fileName, "error", err)
		return 0, 0, fmt.Errorf("failed to get file ID: %w", err)
	}

	// FIX: file should have been inserted by now. Fix this
	if fileID == 0 {
		Logger.Warn("File not found, inserting new file", "fileName", fileName)
		_, err = lm.metadata.InsertFile(fileName)
		if err != nil {
			Logger.Error("Failed to insert new file", "fileName", fileName, "error", err)
			return 0, 0, fmt.Errorf("failed to insert new file: %w", err)
		}
	}

	// Get the active layer
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	if err != nil {
		Logger.Error("Failed to load layers for file from metadata store", "fileID", fileID, "error", err)
		return 0, 0, fmt.Errorf("failed to load layers for file: %w", err)
	}

	if len(layers) == 0 {
		Logger.Error("No active layer found for file", "fileID", fileID)
		return 0, 0, fmt.Errorf("no active layer found for file")
	} else {
		active := layers[len(layers)-1]
		writtenOffset = offset
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

func (lm *LayerManager) FileSize(fileID int) (uint64, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	return lm.metadata.CalculateVirtualFileSize(fileID)
}

// GetDataRange returns a slice of data from the given offset up to size bytes.
func (lm *LayerManager) GetDataRange(fileName string, offset uint64, size uint64) ([]byte, error) {
	Logger.Debug("Getting data range from database", "fileName", fileName, "offset", offset, "requestedSize", size)

	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Get the file ID from the file name
	fileID, err := lm.metadata.GetFileIDByName(fileName)
	if err != nil {
		Logger.Error("Failed to get file ID", "fileName", fileName, "error", err)
		return nil, fmt.Errorf("failed to get file ID: %w", err)
	}

	// Get full content for this specific file
	fullContent := lm.GetFullContentForFile(fileID)

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

// GetFullContentForFile returns the full content of a specific file by ID
func (lm *LayerManager) GetFullContentForFile(fileID int) []byte {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	Logger.Debug("Getting full content for file", "fileID", fileID)

	// Load layers for this specific file
	layers, err := lm.metadata.LoadLayersByFileID(fileID)
	if err != nil {
		Logger.Error("Failed to load layers for file", "fileID", fileID, "error", err)
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
			// Entries are already sorted by offset from the database
			for _, entry := range entries {
				if entry.Offset+uint64(len(entry.Data)) <= uint64(len(buf)) {
					copy(buf[entry.Offset:entry.Offset+uint64(len(entry.Data))], entry.Data)
				} else {
					// Handle case where entry extends beyond current buffer
					newSize := entry.Offset + uint64(len(entry.Data))
					newBuf := make([]byte, newSize)
					copy(newBuf, buf)
					copy(newBuf[entry.Offset:], entry.Data)
					buf = newBuf
				}
			}
		}
	}

	Logger.Debug("Full content retrieved for file", "fileID", fileID, "size", len(buf))
	return buf
}
