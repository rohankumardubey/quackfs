package main

import (
	"slices"
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

// LayerManager manages multiple layers and a global offset.
// It uses a MetadataStore to persist layer metadata and entry data.
type LayerManager struct {
	layers       []*Layer
	nextID       int
	globalOffset uint64
	metadata     *MetadataStore
}

// NewLayerManager creates (or reloads) a LayerManager using the provided MetadataStore.
func NewLayerManager(store *MetadataStore) (*LayerManager, error) {
	Logger.Debug("Creating/reloading layer manager from metadata store")
	layers, nextID, baseGlobalOffset, err := store.LoadLayers()
	if err != nil {
		Logger.Error("Failed to load layers from metadata store", "error", err)
		return nil, err
	}

	Logger.Debug("Loaded layer information", "layerCount", len(layers), "nextID", nextID, "baseGlobalOffset", baseGlobalOffset)

	lm := &LayerManager{
		layers:       layers,
		nextID:       nextID,
		globalOffset: baseGlobalOffset, // temporary; will update after loading entries
		metadata:     store,
	}

	// If no layers exist, create an initial layer.
	if len(layers) == 0 {
		Logger.Debug("No existing layers found, creating initial layer")
		initial := NewLayer(1, 0)
		lm.layers = append(lm.layers, initial)
		lm.nextID = 2
		lm.globalOffset = 0
		if err := store.RecordNewLayer(initial); err != nil {
			Logger.Error("Failed to record initial layer", "error", err)
			return nil, err
		}
		Logger.Debug("Initial layer created and recorded", "layerID", initial.ID)
	}

	// Load entries and rehydrate layers.
	Logger.Debug("Loading entries from metadata store")
	entries, err := store.LoadEntries()
	if err != nil {
		Logger.Error("Failed to load entries from metadata store", "error", err)
		return nil, err
	}

	entriesLoaded := 0
	for _, layer := range lm.layers {
		if entryList, ok := entries[layer.ID]; ok {
			Logger.Debug("Populating layer with entries", "layerID", layer.ID, "entryCount", len(entryList))
			for _, entry := range entryList {
				layer.entries[entry.Offset] = entry.Data
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

	Logger.Debug("Layer manager initialization complete", "totalLayers", len(lm.layers), "totalEntries", entriesLoaded, "globalOffset", lm.globalOffset)
	return lm, nil
}

// ActiveLayer returns the current active (last) layer.
func (lm *LayerManager) ActiveLayer() *Layer {
	active := lm.layers[len(lm.layers)-1]
	Logger.Debug("Getting active layer", "layerID", active.ID, "baseOffset", active.base, "entryCount", len(active.entries))
	return active
}

// SealActiveLayer seals the current active layer and creates a new active layer.
// It updates the metadata store accordingly.
func (lm *LayerManager) SealActiveLayer() error {
	current := lm.ActiveLayer()
	Logger.Debug("Sealing active layer", "layerID", current.ID, "entryCount", len(current.entries))

	if err := lm.metadata.SealLayer(current.ID); err != nil {
		Logger.Error("Failed to seal layer in metadata store", "layerID", current.ID, "error", err)
		return err
	}

	newLayer := NewLayer(lm.nextID, lm.globalOffset)
	lm.layers = append(lm.layers, newLayer)
	lm.nextID++

	Logger.Debug("Created new active layer after sealing", "newLayerID", newLayer.ID, "baseOffset", newLayer.base, "totalLayers", len(lm.layers))
	return lm.metadata.RecordNewLayer(newLayer)
}

// Write writes data to the active layer at the specified global offset.
// It returns the active layer's ID and the offset where the data was written.
func (lm *LayerManager) Write(data []byte, offset uint64) (layerID int, writtenOffset uint64, err error) {
	Logger.Debug("Writing data", "size", len(data), "requestedOffset", offset)
	active := lm.ActiveLayer()

	// Use the requested offset
	writtenOffset = offset
	active.entries[writtenOffset] = data
	Logger.Debug("Added entry to active layer", "layerID", active.ID, "offset", writtenOffset, "dataSize", len(data))

	// Calculate the ranges for this data entry
	dataLength := uint64(len(data))

	// Layer range is relative to the layer's base offset
	// Ensure we don't have underflow when calculating layer-relative offset
	var layerStart uint64
	if writtenOffset >= active.base {
		layerStart = writtenOffset - active.base
	} else {
		// This shouldn't happen with proper layer management
		// but protect against potential underflow
		layerStart = 0
		Logger.Warn("Write offset is before layer base", "offset", writtenOffset, "layerBase", active.base, "layerID", active.ID)
	}
	layerEnd := layerStart + dataLength
	layerRange := [2]uint64{layerStart, layerEnd}

	// File range is the global offset range
	fileStart := writtenOffset
	fileEnd := writtenOffset + dataLength
	fileRange := [2]uint64{fileStart, fileEnd}

	Logger.Debug("Calculated ranges for entry",
		"layerID", active.ID,
		"layerRange", layerRange,
		"fileRange", fileRange)

	// Persist the entry with range information
	if err = lm.metadata.RecordEntry(active.ID, writtenOffset, data, layerRange, fileRange); err != nil {
		Logger.Error("Failed to record entry", "layerID", active.ID, "offset", writtenOffset, "error", err)
		return 0, 0, err
	}

	oldOffset := lm.globalOffset
	lm.globalOffset = max(lm.globalOffset, writtenOffset+dataLength)
	if oldOffset != lm.globalOffset {
		Logger.Debug("Global offset updated", "oldOffset", oldOffset, "newOffset", lm.globalOffset)
	}

	Logger.Debug("Data written successfully", "layerID", active.ID, "offset", writtenOffset, "size", len(data))
	return active.ID, writtenOffset, nil
}

// GetFullContent merges all layers with overlay semantics.
func (lm *LayerManager) GetFullContent() []byte {
	Logger.Debug("Getting full content", "globalOffset", lm.globalOffset, "layerCount", len(lm.layers))

	buf := make([]byte, lm.globalOffset)
	totalEntries := 0

	for _, layer := range lm.layers {
		var offsets []uint64
		for off := range layer.entries {
			offsets = append(offsets, off)
		}

		slices.Sort(offsets)

		Logger.Debug("Merging layer into full content", "layerID", layer.ID, "entryCount", len(offsets))

		for _, off := range offsets {
			data := layer.entries[off]
			copy(buf[off:off+uint64(len(data))], data)
			totalEntries++
		}
	}

	Logger.Debug("Full content generated", "size", len(buf), "totalEntriesProcessed", totalEntries)
	return buf
}

// GetDataRange returns a slice of data from the given offset up to size bytes.
func (lm *LayerManager) GetDataRange(offset uint64, size uint64) ([]byte, error) {
	Logger.Debug("Getting data range", "offset", offset, "requestedSize", size)

	full := lm.GetFullContent()

	if offset >= uint64(len(full)) {
		Logger.Debug("Requested offset beyond content size", "offset", offset, "contentSize", len(full))
		return []byte{}, nil
	}

	end := min(offset+uint64(size), uint64(len(full)))

	Logger.Debug("Returning data range", "offset", offset, "end", end, "returnedSize", end-offset)
	return full[offset:end], nil
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
