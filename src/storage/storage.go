package storage

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vinimdocarmo/difffs/src/logger"
)

type Layer struct {
	ID      int
	FileID  int
	Sealed  bool
	entries map[uint64][]byte
}

func newLayer(fileID int) *Layer {
	return &Layer{
		FileID:  fileID,
		Sealed:  false,
		entries: make(map[uint64][]byte),
	}
}

// AddEntry adds data at the specified offset within the layer
func (l *Layer) addEntry(offset uint64, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	l.entries[offset] = dataCopy
}

// EntryCount returns the number of entries in this layer
func (l *Layer) entryCount() int {
	return len(l.entries)
}

// Manager manages multiple layers and a global offset.
// It uses a metadataStore to persist layer metadata and entry data.
type Manager struct {
	mu sync.RWMutex // Primary mutex for protecting all shared state
	db *sql.DB
}

// NewManager creates (or reloads) a StorageManager using the provided metadataStore.
func NewManager(db *sql.DB) (*Manager, error) {
	logger.Log.Debug("Creating/reloading layer manager from metadata store")

	sm := &Manager{
		db: db,
	}

	// Check if any layers exist, if not create an initial layer
	layers, err := sm.loadLayers()
	if err != nil {
		logger.Log.Error("Failed to check for existing layers", "error", err)
		return nil, fmt.Errorf("failed to check for existing layers: %w", err)
	}

	// If no layers exist, create an initial layer with a default fileID.
	if len(layers) == 0 {
		initialFileID := 0 // Use a default fileID for the initial layer
		if err := sm.createInitialLayer(initialFileID); err != nil {
			return nil, err
		}
	}

	logger.Log.Debug("Layer manager initialization complete")
	return sm, nil
}

// createInitialLayer creates the very first layer when no layers exist
func (sm *Manager) createInitialLayer(fileID int) error {
	logger.Log.Debug("No existing layers found, creating initial layer")
	initial := newLayer(fileID)
	id, err := sm.recordNewLayer(initial)
	if err != nil {
		logger.Log.Error("Failed to record initial layer", "error", err)
		return fmt.Errorf("failed to record initial layer: %w", err)
	}
	logger.Log.Debug("Initial layer created and recorded", "layerID", id)
	return nil
}

// ActiveLayer returns the current active (last) layer.
func (sm *Manager) ActiveLayer() *Layer {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Use the new method to load only the active layer
	active, err := sm.loadActiveLayer()
	if err != nil {
		logger.Log.Error("Failed to load active layer from metadata store", "error", err)
		return nil
	}

	if active == nil {
		logger.Log.Error("No active layer found in metadata store")
		return nil
	}

	logger.Log.Debug("Got active layer", "layerID", active.ID, "entryCount", active.entryCount())
	return active
}

// SealActiveLayer seals the current active layer and creates a new active layer.
// It updates the metadata store accordingly.
func (sm *Manager) SealActiveLayer(fileName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Get the file ID from the file name
	fileID, err := sm.GetFileIDByName(fileName)
	if err != nil {
		logger.Log.Error("Failed to get file ID", "fileName", fileName, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	// Get the current active layer ID
	layers, err := sm.loadLayers()
	if err != nil {
		logger.Log.Error("Failed to load layers from metadata store", "error", err)
		return fmt.Errorf("failed to load layers: %w", err)
	}

	if len(layers) == 0 {
		return fmt.Errorf("no layers found to seal")
	}

	current := layers[len(layers)-1]
	logger.Log.Debug("Sealing active layer", "layerID", current.ID, "entryCount", current.entryCount())

	if err := sm.sealLayer(current.ID); err != nil {
		logger.Log.Error("Failed to seal layer in metadata store", "layerID", current.ID, "error", err)
		return fmt.Errorf("failed to seal layer %d: %w", current.ID, err)
	}

	newLayer := newLayer(fileID)
	id, err := sm.recordNewLayer(newLayer)

	if err != nil {
		logger.Log.Error("Failed to record new layer", "error", err)
		return fmt.Errorf("failed to record new layer: %w", err)
	}

	logger.Log.Debug("Created new active layer after sealing", "newLayerID", id)

	return nil
}

// Write writes data to the active layer at the specified global offset.
// It returns the active layer's ID and the offset where the data was written.
func (sm *Manager) Write(fileName string, data []byte, offset uint64) (layerID int, writtenOffset uint64, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	logger.Log.Debug("Writing data", "fileName", fileName, "size", len(data), "offset", offset)

	// Get the file ID from the file name
	fileID, err := sm.GetFileIDByName(fileName)
	if err != nil {
		logger.Log.Error("Failed to get file ID", "fileName", fileName, "error", err)
		return 0, 0, fmt.Errorf("failed to get file ID: %w", err)
	}
	if fileID == 0 {
		logger.Log.Error("File not found", "fileName", fileName)
		return 0, 0, fmt.Errorf("file not found")
	}

	// Get the active layer
	layers, err := sm.LoadLayersByFileID(fileID)
	if err != nil {
		logger.Log.Error("Failed to load layers for file from metadata store", "fileID", fileID, "error", err)
		return 0, 0, fmt.Errorf("failed to load layers for file: %w", err)
	}

	if len(layers) == 0 {
		logger.Log.Error("No active layer found for file", "fileID", fileID)
		return 0, 0, fmt.Errorf("no active layer found for file")
	} else {
		fileSize, err := sm.calculateVirtualFileSize(fileID)
		if err != nil {
			logger.Log.Error("Failed to calculate file size", "fileID", fileID, "error", err)
			return 0, 0, fmt.Errorf("failed to calculate file size: %w", err)
		}

		if offset > fileSize {
			logger.Log.Error("Write offset is beyond file size", "offset", offset, "fileSize", fileSize)
			return 0, 0, fmt.Errorf("write offset is beyond file size")
		}

		active := layers[len(layers)-1]
		writtenOffset = offset
		active.addEntry(writtenOffset, data)
		logger.Log.Debug("Added entry to active layer", "layerID", active.ID, "offset", writtenOffset, "dataSize", len(data))

		// Calculate ranges and record entry in metadata store
		layerRange, fileRange, err := sm.calculateRanges(active, writtenOffset, len(data))

		if err != nil {
			logger.Log.Errorf("Failed to calculate ranges: %v", err)
			return 0, 0, fmt.Errorf("failed to calculate ranges: %w", err)
		}

		if err = sm.recordEntry(active.ID, writtenOffset, data, layerRange, fileRange); err != nil {
			logger.Log.Error("Failed to record entry", "layerID", active.ID, "offset", writtenOffset, "error", err)
			return 0, 0, fmt.Errorf("failed to record entry: %w", err)
		}

		logger.Log.Debug("Data written successfully", "layerID", active.ID, "offset", writtenOffset, "size", len(data))
		return active.ID, writtenOffset, nil
	}
}

// calculateRanges computes the layer-relative and file-absolute ranges for an entry
func (sm *Manager) calculateRanges(layer *Layer, offset uint64, dataSize int) ([2]uint64, [2]uint64, error) {
	dataLength := uint64(dataSize)

	// Retrieve the layer base from the metadata store.
	baseOffset, err := sm.GetLayerBase(layer.ID)
	if err != nil {
		logger.Log.Error("Failed to retrieve layer base", "layerID", layer.ID, "error", err)
		return [2]uint64{}, [2]uint64{}, fmt.Errorf("failed to retrieve layer base: %w", err)
	}

	var layerStart uint64
	if offset >= baseOffset {
		layerStart = offset - baseOffset
	} else {
		layerStart = 0
		logger.Log.Warn("Write offset is before layer base", "offset", offset, "layerBase", baseOffset, "layerID", layer.ID)
	}
	layerEnd := layerStart + dataLength
	layerRange := [2]uint64{layerStart, layerEnd}

	// File range remains the global offset range.
	fileStart := offset
	fileEnd := offset + dataLength
	fileRange := [2]uint64{fileStart, fileEnd}

	logger.Log.Debug("Calculated ranges for entry",
		"layerID", layer.ID,
		"layerRange", layerRange,
		"fileRange", fileRange)

	return layerRange, fileRange, nil
}

func (sm *Manager) FileSize(id int) (uint64, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.calculateVirtualFileSize(id)
}

// GetDataRange returns a slice of data from the given offset up to size bytes.
func (sm *Manager) GetDataRange(fileName string, offset uint64, size uint64) ([]byte, error) {
	logger.Log.Debug("Getting data range from database", "fileName", fileName, "offset", offset, "requestedSize", size)

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Get the file ID from the file name
	fileID, err := sm.GetFileIDByName(fileName)
	if fileID == 0 {
		logger.Log.Warn("File not found", "fileName", fileName)
		return nil, fmt.Errorf("file not found")
	}
	if err != nil {
		logger.Log.Error("Failed to get file ID", "fileName", fileName, "error", err)
		return nil, fmt.Errorf("failed to get file ID: %w", err)
	}

	// Get full content for this specific file
	fullContent := sm.GetFullContentForFile(fileID)

	if offset >= uint64(len(fullContent)) {
		logger.Log.Debug("Requested offset beyond content size", "offset", offset, "contentSize", len(fullContent))
		return []byte{}, nil
	}

	end := min(offset+size, uint64(len(fullContent)))

	// Create a copy of the slice to prevent race conditions
	result := make([]byte, end-offset)
	copy(result, fullContent[offset:end])

	logger.Log.Debug("Returning data range", "offset", offset, "end", end, "returnedSize", end-offset)
	return result, nil
}

// GetFullContentForFile returns the full content of a specific file by ID
func (sm *Manager) GetFullContentForFile(id int) []byte {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	logger.Log.Debug("Getting full content for file", "fileID", id)

	// Load layers for this specific file
	layers, err := sm.LoadLayersByFileID(id)
	if err != nil {
		logger.Log.Error("Failed to load layers for file", "fileID", id, "error", err)
		return []byte{}
	}

	// Load all entries
	entriesMap, err := sm.loadEntries()
	if err != nil {
		logger.Log.Error("Failed to load entries from metadata store", "error", err)
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

	logger.Log.Debug("Full content retrieved for file", "fileID", id, "size", len(buf))
	return buf
}

func (sm *Manager) GetLayerBase(layerID int) (uint64, error) {
	query := `
		SELECT lower(file_range)::bigint
		FROM entries
		WHERE layer_id = $1
		ORDER BY lower(file_range) ASC
		LIMIT 1;
	`

	var base sql.NullInt64
	err := sm.db.QueryRow(query, layerID).Scan(&base)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // base will be 0
		}
		return 0, err
	}
	if !base.Valid {
		return 0, fmt.Errorf("invalid base value for layer %d", layerID)
	}
	return uint64(base.Int64), nil
}

// LayerMetadata holds metadata for a layer.
type LayerMetadata struct {
	ID        int
	Base      int64
	CreatedAt time.Time
	Sealed    bool
}

// EntryRecord holds data for a write entry.
type EntryRecord struct {
	LayerID    int
	Offset     uint64
	Data       []byte
	LayerRange [2]uint64 // Range within a layer as an array of two integers
	FileRange  [2]uint64 // Range within the virtual file as an array of two integers
}

// InsertFile inserts a new file into the files table and returns its ID.
func (sm *Manager) InsertFile(name string) (int, error) {
	logger.Log.Debug("Inserting new file into metadata store", "name", name)

	query := `INSERT INTO files (name) VALUES ($1) RETURNING id;`
	var fileID int
	err := sm.db.QueryRow(query, name).Scan(&fileID)
	if err != nil {
		logger.Log.Error("Failed to insert new file", "name", name, "error", err)
		return 0, err
	}

	// After inserting the file, create an initial unsealed layer for it
	layer := newLayer(fileID)
	_, err = sm.recordNewLayer(layer)
	if err != nil {
		logger.Log.Error("Failed to create initial layer for new file", "fileID", fileID, "error", err)
		return 0, fmt.Errorf("failed to create initial layer for new file: %w", err)
	}

	logger.Log.Debug("File inserted successfully", "name", name, "fileID", fileID)
	return fileID, nil
}

// GetFileIDByName retrieves the file ID for a given file name.
func (sm *Manager) GetFileIDByName(name string) (int, error) {
	query := `SELECT id FROM files WHERE name = $1;`
	var fileID int
	err := sm.db.QueryRow(query, name).Scan(&fileID)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Log.Warn("File not found", "name", name)
			return 0, nil
		}
		logger.Log.Error("Failed to retrieve file ID", "name", name, "error", err)
		return 0, err
	}

	return fileID, nil
}

// recordNewLayer inserts a new layer record.
func (sm *Manager) recordNewLayer(layer *Layer) (int, error) {
	logger.Log.Debug("Recording new layer in metadata store", "layerID", layer.ID)

	query := `INSERT INTO layers (file_id, sealed) VALUES ($1, 0) RETURNING id;`
	var newID int
	err := sm.db.QueryRow(query, layer.FileID).Scan(&newID)
	if err != nil {
		logger.Log.Error("Failed to record new layer", "layerID", layer.ID, "error", err)
		return 0, err
	}

	layer.ID = newID

	logger.Log.Debug("Layer recorded successfully", "layerID", newID)
	return newID, nil
}

// sealLayer marks the layer with the given id as sealed.
func (sm *Manager) sealLayer(layerID int) error {
	logger.Log.Debug("Sealing layer in metadata store", "layerID", layerID)

	query := `UPDATE layers SET sealed = 1 WHERE id = $1;`
	result, err := sm.db.Exec(query, layerID)
	if err != nil {
		logger.Log.Error("Failed to seal layer", "layerID", layerID, "error", err)
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	logger.Log.Debug("Layer sealed successfully", "layerID", layerID, "rowsAffected", rowsAffected)
	return nil
}

// recordEntry inserts a new entry record.
// Now includes layer_range and file_range parameters
func (sm *Manager) recordEntry(layerID int, offset uint64, data []byte, layerRange [2]uint64, fileRange [2]uint64) error {
	logger.Log.Debug("Recording entry in metadata store",
		"layerID", layerID,
		"offset", offset,
		"dataSize", len(data),
		"layerRange", layerRange,
		"fileRange", fileRange)

	layerRangeStr := fmt.Sprintf("[%d,%d)", layerRange[0], layerRange[1])
	fileRangeStr := fmt.Sprintf("[%d,%d)", fileRange[0], fileRange[1])

	query := `INSERT INTO entries (layer_id, offset_value, data, layer_range, file_range) 
	         VALUES ($1, $2, $3, $4, $5);`
	_, err := sm.db.Exec(query, layerID, offset, data, layerRangeStr, fileRangeStr)
	if err != nil {
		logger.Log.Error("Failed to record entry", "layerID", layerID, "offset", offset, "error", err)
		return err
	}

	logger.Log.Debug("Entry recorded successfully", "layerID", layerID, "offset", offset, "dataSize", len(data))
	return nil
}

// loadLayers loads all layer metadata from the database sorted by id.
// It returns a slice of layers (with only the metadata), the next available layer id,
// and the highest base value among layers.
func (sm *Manager) loadLayers() ([]*Layer, error) {
	logger.Log.Debug("Loading layers from metadata store")

	query := `SELECT id, file_id, sealed FROM layers ORDER BY id ASC;`
	rows, err := sm.db.Query(query)
	if err != nil {
		logger.Log.Error("Failed to query layers", "error", err)
		return nil, err
	}
	defer rows.Close()

	var layers []*Layer
	layerCount := 0

	for rows.Next() {
		var id, fileID, sealedInt int
		if err := rows.Scan(&id, &fileID, &sealedInt); err != nil {
			logger.Log.Error("Error scanning layer row", "error", err)
			return nil, err
		}

		layer := newLayer(fileID)
		layer.ID = id
		layers = append(layers, layer)

		sealed := sealedInt != 0
		logger.Log.Debug("Loaded layer", "layerID", id, "sealed", sealed)
		layerCount++
	}

	logger.Log.Debug("Layers loaded successfully", "count", layerCount)
	return layers, nil
}

// loadEntries loads all entry records from the database and groups them by layer_id.
func (sm *Manager) loadEntries() (map[int][]EntryRecord, error) {
	logger.Log.Debug("Loading entries from metadata store")

	query := `SELECT layer_id, offset_value, data, layer_range, file_range FROM entries ORDER BY layer_id, offset_value ASC;`
	rows, err := sm.db.Query(query)
	if err != nil {
		logger.Log.Error("Failed to query entries", "error", err)
		return nil, err
	}
	defer rows.Close()

	result := make(map[int][]EntryRecord)
	entriesCount := 0
	totalDataSize := 0

	for rows.Next() {
		var layerID int
		var offset uint64
		var data []byte
		var layerRangeStr, fileRangeStr sql.NullString

		if err := rows.Scan(&layerID, &offset, &data, &layerRangeStr, &fileRangeStr); err != nil {
			logger.Log.Error("Error scanning entry row", "error", err)
			return nil, err
		}

		layerRange := [2]uint64{0, 0}
		if layerRangeStr.Valid {
			parts := strings.Split(strings.Trim(layerRangeStr.String, "[)"), ",")
			if len(parts) == 2 {
				start, err := strconv.ParseUint(parts[0], 10, 8)
				if err != nil {
					logger.Log.Error("Error parsing layer range start", "value", parts[0], "error", err)
					return nil, err
				}
				end, err := strconv.ParseUint(parts[1], 10, 8)
				if err != nil {
					logger.Log.Error("Error parsing layer range end", "value", parts[1], "error", err)
					return nil, err
				}
				layerRange[0] = start
				layerRange[1] = end
			}
		}

		fileRange := [2]uint64{0, 0}
		if fileRangeStr.Valid {
			parts := strings.Split(strings.Trim(fileRangeStr.String, "[)"), ",")
			if len(parts) == 2 {
				start, err := strconv.ParseUint(parts[0], 10, 64)
				if err != nil {
					logger.Log.Error("Error parsing file range start", "value", parts[0], "error", err)
					return nil, err
				}
				end, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					logger.Log.Error("Error parsing file range end", "value", parts[1], "error", err)
					return nil, err
				}
				fileRange[0] = start
				fileRange[1] = end
			}
		}

		result[layerID] = append(result[layerID], EntryRecord{
			LayerID:    layerID,
			Offset:     offset,
			Data:       data,
			LayerRange: layerRange,
			FileRange:  fileRange,
		})

		entriesCount++
		totalDataSize += len(data)
		logger.Log.Debug("Loaded entry", "layerID", layerID, "offset", offset, "dataSize", len(data))
	}

	layerCount := len(result)
	logger.Log.Debug("Entries loaded successfully",
		"totalEntries", entriesCount,
		"layerCount", layerCount,
		"totalDataSize", totalDataSize)

	return result, nil
}

// loadActiveLayer loads only the active (unsealed) layer and its entries.
// Returns the active layer or nil if no active layer is found.
func (sm *Manager) loadActiveLayer() (*Layer, error) {
	logger.Log.Debug("Loading active (unsealed) layer from metadata store")

	// Use a single query with JOIN to get the active layer and its entries
	query := `
		SELECT l.id, l.file_id, e.offset_value, e.data, e.layer_range, e.file_range 
		FROM layers l
		LEFT JOIN entries e ON l.id = e.layer_id
		WHERE l.sealed = 0
		ORDER BY l.id DESC
		LIMIT 1;
	`

	rows, err := sm.db.Query(query)
	if err != nil {
		logger.Log.Error("Failed to query active layer and entries", "error", err)
		return nil, err
	}
	defer rows.Close()

	// Check if we found any rows
	if !rows.Next() {
		logger.Log.Debug("No active layer found")
		return nil, nil
	}

	// Create the layer
	var fileID int
	rows.Scan(&fileID)
	layer := newLayer(fileID)

	// Track statistics
	var layerID int
	entriesCount := 0
	totalDataSize := 0

	// Process all rows
	for {
		var id, fileID int
		var offset sql.NullInt64
		var data []byte
		var layerRangeStr, fileRangeStr sql.NullString

		// Scan the current row
		if err := rows.Scan(&id, &fileID, &offset, &data, &layerRangeStr, &fileRangeStr); err != nil {
			logger.Log.Error("Error scanning row", "error", err)
			return nil, err
		}

		// Set or verify the layer ID
		if layerID == 0 {
			layerID = id
			layer.ID = id
			logger.Log.Debug("Found active layer", "layerID", id)
		} else if id != layerID {
			// This shouldn't happen with our query, but check anyway
			logger.Log.Warn("Unexpected layer ID in results", "expected", layerID, "got", id)
		}

		// Add entry data if present
		if offset.Valid && data != nil {
			offsetValue := uint64(offset.Int64)
			layer.addEntry(offsetValue, data)
			entriesCount++
			totalDataSize += len(data)
			logger.Log.Debug("Loaded entry", "layerID", id, "offset", offsetValue, "dataSize", len(data))
		}

		// Move to next row or exit loop if done
		if !rows.Next() {
			break
		}
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		logger.Log.Error("Error iterating over rows", "error", err)
		return nil, err
	}

	logger.Log.Debug("Active layer loaded successfully",
		"layerID", layerID,
		"entriesCount", entriesCount,
		"totalDataSize", totalDataSize)

	return layer, nil
}

// close closes the database.
func (sm *Manager) Close() error {
	logger.Log.Debug("Closing metadata store database connection")
	err := sm.db.Close()
	if err != nil {
		logger.Log.Error("Error closing database connection", "error", err)
	} else {
		logger.Log.Debug("Database connection closed successfully")
	}
	return err
}

// calculateVirtualFileSize calculates the total byte size of the virtual file from all layers and their entries, respecting layer creation order and handling overlapping file ranges.
func (sm *Manager) calculateVirtualFileSize(fileID int) (uint64, error) {
	logger.Log.Debug("Calculating virtual file size from metadata store", "fileID", fileID)

	query := `
		SELECT e.file_range
		FROM entries e
		JOIN layers l ON e.layer_id = l.id
		WHERE l.file_id = $1
		ORDER BY l.created_at ASC, lower(e.file_range) ASC;
	`
	rows, err := sm.db.Query(query, fileID)
	if err != nil {
		logger.Log.Error("Failed to query file ranges", "error", err, "fileID", fileID)
		return 0, err
	}
	defer rows.Close()

	type Range struct {
		start uint64
		end   uint64
	}

	var ranges []Range

	for rows.Next() {
		var fileRangeStr sql.NullString

		if err := rows.Scan(&fileRangeStr); err != nil {
			logger.Log.Error("Error scanning file range row", "error", err)
			return 0, err
		}

		if fileRangeStr.Valid {
			parts := strings.Split(strings.Trim(fileRangeStr.String, "[)"), ",")
			if len(parts) == 2 {
				start, err := strconv.ParseUint(parts[0], 10, 64)
				if err != nil {
					logger.Log.Error("Error parsing file range start", "value", parts[0], "error", err)
					return 0, err
				}
				end, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					logger.Log.Error("Error parsing file range end", "value", parts[1], "error", err)
					return 0, err
				}
				ranges = append(ranges, Range{start: start, end: end})
			}
		}
	}

	if err = rows.Err(); err != nil {
		logger.Log.Error("Error iterating over file range rows", "error", err)
		return 0, err
	}

	// Merge overlapping ranges
	var totalSize uint64
	if len(ranges) > 0 {
		// Ranges are already sorted by the SQL query (ORDER BY l.created_at ASC, lower(e.file_range) ASC)
		// Find the maximum end position, which represents the virtual file size
		var maxEnd uint64
		for _, r := range ranges {
			if r.end > maxEnd {
				maxEnd = r.end
			}
		}

		totalSize = maxEnd
	}

	logger.Log.Debug("Virtual file size calculated successfully", "totalSize", totalSize)
	return totalSize, nil
}

// LoadLayersByFileID loads all layers associated with a specific file ID from the database.
func (sm *Manager) LoadLayersByFileID(fileID int) ([]*Layer, error) {
	logger.Log.Debug("Loading layers for file from metadata store", "fileID", fileID)

	query := `SELECT id, file_id, sealed FROM layers WHERE file_id = $1 ORDER BY id ASC;`
	rows, err := sm.db.Query(query, fileID)
	if err != nil {
		logger.Log.Error("Failed to query layers for file", "fileID", fileID, "error", err)
		return nil, err
	}
	defer rows.Close()

	var layers []*Layer
	layerCount := 0

	for rows.Next() {
		var id, sealedInt int
		if err := rows.Scan(&id, &fileID, &sealedInt); err != nil {
			logger.Log.Error("Error scanning layer row", "error", err)
			return nil, err
		}

		layer := newLayer(fileID)
		layer.ID = id
		layers = append(layers, layer)

		sealed := sealedInt != 0
		logger.Log.Debug("Loaded layer for file", "layerID", id, "sealed", sealed)
		layerCount++
	}

	logger.Log.Debug("Layers for file loaded successfully", "fileID", fileID, "count", layerCount)
	return layers, nil
}

// deleteFile removes a file and its associated layers and entries from the database within a transaction.
func (sm *Manager) DeleteFile(name string) error {
	logger.Log.Debug("Deleting file and its associated data from metadata store", "name", name)

	// Begin a transaction
	tx, err := sm.db.Begin()
	if err != nil {
		logger.Log.Error("Failed to begin transaction", "error", err)
		return err
	}

	// Ensure the transaction is rolled back in case of an error
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			logger.Log.Error("Transaction failed, rolling back", "error", err)
			tx.Rollback()
		} else {
			err = tx.Commit()
			if err != nil {
				logger.Log.Error("Failed to commit transaction", "error", err)
			}
		}
	}()

	// Retrieve the file ID
	fileID, err := sm.GetFileIDByName(name)
	if err != nil {
		logger.Log.Error("Failed to retrieve file ID", "name", name, "error", err)
		return err
	}

	if fileID == 0 {
		logger.Log.Warn("File not found, nothing to delete", "name", name)
		return nil
	}

	// Delete all entries associated with the file's layers
	deleteEntriesQuery := `DELETE FROM entries WHERE layer_id IN (SELECT id FROM layers WHERE file_id = $1);`
	_, err = tx.Exec(deleteEntriesQuery, fileID)
	if err != nil {
		logger.Log.Error("Failed to delete entries for file", "name", name, "error", err)
		return err
	}

	// Delete all layers associated with the file
	deleteLayersQuery := `DELETE FROM layers WHERE file_id = $1;`
	_, err = tx.Exec(deleteLayersQuery, fileID)
	if err != nil {
		logger.Log.Error("Failed to delete layers for file", "name", name, "error", err)
		return err
	}

	// Delete the file itself
	deleteFileQuery := `DELETE FROM files WHERE id = $1;`
	_, err = tx.Exec(deleteFileQuery, fileID)
	if err != nil {
		logger.Log.Error("Failed to delete file", "name", name, "error", err)
		return err
	}

	logger.Log.Info("File and its associated data deleted successfully", "name", name)
	return nil
}

func (sm *Manager) Checkpoint(filename string) error {
	return sm.SealActiveLayer(filename)
}

// fileInfo represents basic file information
type fileInfo struct {
	ID   int
	Name string
}

// GetAllFiles returns a list of all files in the database
func (sm *Manager) GetAllFiles() ([]fileInfo, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	query := `SELECT id, name FROM files;`
	rows, err := sm.db.Query(query)
	if err != nil {
		logger.Log.Error("Failed to query files", "error", err)
		return nil, err
	}
	defer rows.Close()

	var files []fileInfo
	for rows.Next() {
		var file fileInfo
		if err := rows.Scan(&file.ID, &file.Name); err != nil {
			logger.Log.Error("Failed to scan file row", "error", err)
			return nil, err
		}
		files = append(files, file)
	}

	if err := rows.Err(); err != nil {
		logger.Log.Error("Error iterating file rows", "error", err)
		return nil, err
	}

	return files, nil
}
