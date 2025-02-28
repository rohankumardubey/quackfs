package main

import (
	"slices"

	log "github.com/charmbracelet/log"
)

// Layer represents an in-memory layer storing appended data.
type Layer struct {
	ID      int
	base    uint64            // Global offset where this layer starts.
	entries map[uint64][]byte // Maps global offsets to data.
}

// NewLayer creates a new layer with the given unique ID and base offset.
func NewLayer(id int, base uint64) *Layer {
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
	layers, nextID, baseGlobalOffset, err := store.LoadLayers()
	if err != nil {
		return nil, err
	}
	lm := &LayerManager{
		layers:       layers,
		nextID:       nextID,
		globalOffset: baseGlobalOffset, // temporary; will update after loading entries
		metadata:     store,
	}
	// If no layers exist, create an initial layer.
	if len(layers) == 0 {
		initial := NewLayer(1, 0)
		lm.layers = append(lm.layers, initial)
		lm.nextID = 2
		lm.globalOffset = 0
		if err := store.RecordNewLayer(initial); err != nil {
			return nil, err
		}
	}
	// Load entries and rehydrate layers.
	entries, err := store.LoadEntries()
	if err != nil {
		return nil, err
	}
	for _, layer := range lm.layers {
		if entryList, ok := entries[layer.ID]; ok {
			for _, entry := range entryList {
				layer.entries[entry.Offset] = entry.Data
				candidate := entry.Offset + uint64(len(entry.Data))
				if candidate > lm.globalOffset {
					lm.globalOffset = candidate
				}
			}
		}
	}
	return lm, nil
}

// ActiveLayer returns the current active (last) layer.
func (lm *LayerManager) ActiveLayer() *Layer {
	return lm.layers[len(lm.layers)-1]
}

// SealActiveLayer seals the current active layer and creates a new active layer.
// It updates the metadata store accordingly.
func (lm *LayerManager) SealActiveLayer() error {
	current := lm.ActiveLayer()
	if err := lm.metadata.SealLayer(current.ID); err != nil {
		return err
	}
	newLayer := NewLayer(lm.nextID, lm.globalOffset)
	lm.layers = append(lm.layers, newLayer)
	lm.nextID++
	return lm.metadata.RecordNewLayer(newLayer)
}

// Write writes data to the active layer at the specified global offset.
// It returns the active layer's ID and the offset where the data was written.
func (lm *LayerManager) Write(data []byte, offset uint64) (layerID int, writtenOffset uint64, err error) {
	log.Debugf("Writing data (size %d bytes) at offset %d", len(data), offset)
	active := lm.ActiveLayer()

	// Use the requested offset
	writtenOffset = offset
	active.entries[writtenOffset] = data

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
	}

	layerEnd := layerStart + dataLength
	layerRange := [2]uint64{layerStart, layerEnd}

	// File range is the global offset range
	fileStart := writtenOffset
	fileEnd := writtenOffset + dataLength
	fileRange := [2]uint64{fileStart, fileEnd}

	// Persist the entry with range information
	if err = lm.metadata.RecordEntry(active.ID, writtenOffset, data, layerRange, fileRange); err != nil {
		log.Errorf("Failed to record entry: %v", err)
		return 0, 0, err
	}
	lm.globalOffset = max(lm.globalOffset, writtenOffset+dataLength)
	log.Debugf("Data written successfully at offset %d", writtenOffset)
	return active.ID, writtenOffset, nil
}

// Updated GetFullContent to merge layers with overlay semantics.
func (lm *LayerManager) GetFullContent() []byte {
	buf := make([]byte, lm.globalOffset)
	for _, layer := range lm.layers {
		var offsets []uint64
		for off := range layer.entries {
			offsets = append(offsets, off)
		}
		slices.Sort(offsets)
		for _, off := range offsets {
			data := layer.entries[off]
			copy(buf[off:off+uint64(len(data))], data)
		}
	}
	return buf
}

// GetDataRange returns a slice of data from the given offset up to size bytes.
func (lm *LayerManager) GetDataRange(offset uint64, size uint64) ([]byte, error) {
	full := lm.GetFullContent()
	if offset >= uint64(len(full)) {
		return []byte{}, nil
	}
	end := min(offset+uint64(size), uint64(len(full)))
	return full[offset:end], nil
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
