package main

import "sort"

// Layer represents an in-memory layer storing appended data.
type Layer struct {
	ID      int
	base    int64            // Global offset where this layer starts.
	entries map[int64][]byte // Maps global offsets to data.
}

// NewLayer creates a new layer with the given unique ID and base offset.
func NewLayer(id int, base int64) *Layer {
	return &Layer{
		ID:      id,
		base:    base,
		entries: make(map[int64][]byte),
	}
}

// LayerManager manages multiple layers and a global offset.
// It uses a MetadataStore to persist layer metadata and entry data.
type LayerManager struct {
	layers       []*Layer
	nextID       int
	globalOffset int64
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
				candidate := entry.Offset + int64(len(entry.Data))
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

// Write writes data to the active layer at the current global offset.
// It returns the active layer's ID and the offset where the data was written.
func (lm *LayerManager) Write(data []byte) (layerID int, offset int64) {
	active := lm.ActiveLayer()
	offset = lm.globalOffset
	active.entries[offset] = data
	// Persist the entry.
	if err := lm.metadata.RecordEntry(active.ID, offset, data); err != nil {
		// In a real system, handle the error.
	}
	lm.globalOffset += int64(len(data))
	return active.ID, offset
}

// GetFullContent merges all writes in order of layers and, within each layer, by increasing offset.
// This simply concatenates the data from each write without performing offset-based copy.
func (lm *LayerManager) GetFullContent() []byte {
	var content []byte
	for _, layer := range lm.layers {
		// Collect and sort the offsets for this layer.
		var offsets []int64
		for off := range layer.entries {
			offsets = append(offsets, off)
		}
		// Sort offsets in increasing order.
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		for _, off := range offsets {
			content = append(content, layer.entries[off]...)
		}
	}
	return content
}

// GetDataRange returns a slice of data from the given offset up to size bytes.
func (lm *LayerManager) GetDataRange(offset int64, size int) ([]byte, error) {
	full := lm.GetFullContent()
	if offset >= int64(len(full)) {
		return []byte{}, nil
	}
	end := offset + int64(size)
	if end > int64(len(full)) {
		end = int64(len(full))
	}
	return full[offset:end], nil
}
