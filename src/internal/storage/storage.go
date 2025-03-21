package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/charmbracelet/log"
)

// layer represents a snapshot layer
// When Active is true it means that the layer is currently in memory
// and not yet persisted to storage. The active layer is the layer where
// data written is stored.
//
// On DuckDB CHECKPOINT, the WAL file is removed which will trigger a
// Manager.Checkpoint() call. The active layer, its data and its chunks
// are persisted to storage. The active layer is then reset to a new empty
// layer and the cycle continues.
//
// data is always written in a append-only fashion. The writes are their offsets
// are stored in the chunks metadata. Which write will be represented by a
// chunkMetadata.
type layer struct {
	id        int64
	FileID    int64
	Active    bool // whether of not it is the current active layer (memory resident)
	VersionID int64
	Tag       string
	chunks    []metadata
	size      uint64
	data      []byte
}

// metadata holds information about where data was written in the layer data
type metadata struct {
	layerID    int64     // 0 if flushed is false
	flushed    bool      // whether the metadata has been persisted to the database
	layerRange [2]uint64 // Range within a layer as an array of two integers
	fileRange  [2]uint64 // Range within the virtual file as an array of two integers
}

var ErrNotFound = errors.New("not found")

type ObjectStore interface {
	// PutObject uploads data to the object store.
	PutObject(ctx context.Context, key string, data []byte) error
	// GetObject returns a slice of data from the given offset up to size bytes.
	// Range is inclusive of the start and the end (i.e. [start, end])
	GetObject(ctx context.Context, key string, dataRange [2]uint64) ([]byte, error)
}

type Manager struct {
	db          *sql.DB
	log         *log.Logger
	mu          sync.RWMutex     // Add a mutex to protect activeLayers
	memtable    map[int64]*layer // Stores a mapping of file ids to their active layer
	objectStore ObjectStore
}

// NewManager creates (or reloads) a StorageManager using the provided metadataStore.
func NewManager(db *sql.DB, objectStore ObjectStore, bucketName string, log *log.Logger) *Manager {
	managerLog := log.With()
	managerLog.SetPrefix("💽 storage")

	sm := &Manager{
		db:          db,
		log:         managerLog,
		memtable:    make(map[int64]*layer),
		objectStore: objectStore,
	}

	return sm
}

// WriteFile writes data to the active layer at the specified offset.
func (sm *Manager) WriteFile(ctx context.Context, filename string, data []byte, offset uint64) error {
	sm.mu.Lock()         // Lock before accessing activeLayers
	defer sm.mu.Unlock() // Ensure unlock when function returns

	sm.log.Debug("Writing data", "filename", filename, "size", len(data), "offset", offset)

	// Get the file ID from the file name
	fileID, err := sm.GetFileIDByName(ctx, filename)
	if err != nil {
		sm.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	activeLayer, exists := sm.memtable[fileID]
	if !exists {
		activeLayer = &layer{
			FileID: fileID,
			chunks: []metadata{},
			data:   []byte{},
			Active: true,
		}
		sm.memtable[fileID] = activeLayer
	}

	fileSize, err := sm.calcSizeOf(ctx, fileID)
	if err != nil {
		sm.log.Error("Failed to calculate size of file", "error", err)
		return fmt.Errorf("failed to calculate size of file: %w", err)
	}

	if offset > fileSize {
		// Calculate how many zero bytes to add
		bytesToAdd := offset - fileSize

		// Create a buffer of zero bytes
		zeroes := make([]byte, bytesToAdd)

		var layerSize uint64 = 0
		if len(activeLayer.chunks) > 0 {
			layerSize = activeLayer.chunks[len(activeLayer.chunks)-1].layerRange[1]
		}

		layerRange := [2]uint64{layerSize, layerSize + bytesToAdd}
		fileRange := [2]uint64{fileSize, fileSize + bytesToAdd}

		activeLayer.data = append(activeLayer.data, zeroes...)
		activeLayer.chunks = append(activeLayer.chunks, metadata{
			layerRange: layerRange,
			fileRange:  fileRange,
			flushed:    false, // since we're writing to the active layer, it's not flushed yet
		})
		activeLayer.size = layerRange[1]
	}

	var layerSize uint64 = 0
	if len(activeLayer.chunks) > 0 {
		layerSize = activeLayer.chunks[len(activeLayer.chunks)-1].layerRange[1]
	}

	layerRange := [2]uint64{layerSize, layerSize + uint64(len(data))}
	fileRange := [2]uint64{offset, offset + uint64(len(data))}

	activeLayer.data = append(activeLayer.data, data...)
	activeLayer.chunks = append(activeLayer.chunks, metadata{
		layerRange: layerRange,
		fileRange:  fileRange,
		flushed:    false, // since we're writing to the active layer, it's not flushed yet
	})
	activeLayer.size = layerRange[1]

	return nil
}

func (sm *Manager) GetActiveLayerSize(ctx context.Context, fileID int64) uint64 {
	sm.mu.RLock() // Read lock is sufficient for reading
	defer sm.mu.RUnlock()

	activeLayer, exists := sm.memtable[fileID]
	if !exists {
		return 0
	}
	return activeLayer.size
}

func (sm *Manager) GetActiveLayerData(ctx context.Context, fileID int64) []byte {
	sm.mu.RLock() // Read lock is sufficient for reading
	defer sm.mu.RUnlock()

	l, exists := sm.memtable[fileID]
	if !exists {
		return nil
	}

	return l.data
}

func (sm *Manager) SizeOf(ctx context.Context, filename string) (uint64, error) {
	fileID, err := sm.GetFileIDByName(ctx, filename)
	if err != nil {
		return 0, err
	}

	return sm.calcSizeOf(ctx, fileID)
}

// readFileOpt defines functional options for GetDataRange
type readFileOpt func(*readFileOpts)

// readFileOpts holds all options for GetDataRange
type readFileOpts struct {
	version string
}

// WithVersion specifies a version tag to retrieve data up to
func WithVersion(v string) readFileOpt {
	return func(opts *readFileOpts) {
		opts.version = v
	}
}

// ReadFile returns a slice of data from the given offset up to size bytes.
// Optional version tag can be specified to retrieve data up to a specific version.
func (sm *Manager) ReadFile(ctx context.Context, filename string, offset uint64, size uint64, opts ...readFileOpt) ([]byte, error) {
	sm.mu.RLock() // Read lock is sufficient for reading
	defer sm.mu.RUnlock()

	options := readFileOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	hasVersion := options.version != ""
	var versionedLayerId int64

	if hasVersion {
		sm.log.Debug("reading file",
			"filename", filename,
			"offset", offset,
			"size", size,
			"version", options.version)
	} else {
		sm.log.Debug("reading file",
			"filename", filename,
			"offset", offset,
			"size", size)
	}

	tx, err := sm.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		sm.log.Error("Failed to begin transaction", "error", err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Setup deferred rollback in case of error or panic
	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				sm.log.Error("Failed to rollback transaction after panic", "error", rbErr)
			}
			// Re-panic after rollback
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				sm.log.Error("Failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	fileID, err := sm.GetFileIDByName(ctx, filename, withTx(tx))
	if fileID == 0 {
		sm.log.Error("File not found", "filename", filename)
		return nil, fmt.Errorf("file not found")
	}
	if err != nil {
		sm.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return nil, fmt.Errorf("failed to get file ID: %w", err)
	}

	// check if there's a layer for this file with the given version tag
	if hasVersion {
		err = tx.QueryRowContext(ctx, `
			SELECT snapshot_layers.id
			FROM snapshot_layers
			INNER JOIN versions on versions.id = snapshot_layers.version_id
			WHERE snapshot_layers.file_id = $1 and versions.tag = $2
		`, fileID, options.version).Scan(&versionedLayerId)
		if err != nil {
			if err == sql.ErrNoRows {
				sm.log.Error("version tag not found", "version", options.version, "filename", filename)
				return nil, fmt.Errorf("version tag not found")
			}
			sm.log.Error("failed to check if layer exists", "id", fileID, "version", options.version, "error", err)
			return nil, fmt.Errorf("failed to check if layer exists: %w", err)
		}
	}

	var query string
	var rows *sql.Rows

	// Load chunks that could match the requested range with proper ordering
	// If version tag is specified, filter layers by version
	if hasVersion {
		query = `
			SELECT c.snapshot_layer_id, c.layer_range, c.file_range
			FROM chunks c
			INNER JOIN snapshot_layers l ON c.snapshot_layer_id = l.id
			WHERE
				l.id <= $1 AND l.file_id = $2 AND
				c.file_range && int8range($3, $4)
			ORDER BY l.id ASC, c.id ASC;
		`
		rows, err = tx.QueryContext(ctx, query, versionedLayerId, fileID, offset, offset+size)
	} else {
		query = `
			SELECT c.snapshot_layer_id, c.layer_range, c.file_range
			FROM chunks c
			INNER JOIN snapshot_layers l ON c.snapshot_layer_id = l.id
			WHERE
				l.file_id = $1 AND c.file_range && int8range($2, $3)
			ORDER BY l.id ASC, c.id ASC;
		`
		rows, err = tx.QueryContext(ctx, query, fileID, offset, offset+size)
	}
	if err != nil {
		sm.log.Error("Failed to query chunks", "error", err)
		return nil, err
	}
	defer rows.Close()

	// Map to store chunks by layer ID
	chunks := make([]metadata, 0)

	var maxEndOffset uint64

	// Process chunks that could match the requested range
	for rows.Next() {
		var layerID int64
		var layerRangeStr, fileRangeStr sql.NullString

		if err := rows.Scan(&layerID, &layerRangeStr, &fileRangeStr); err != nil {
			sm.log.Error("Error scanning chunk row", "error", err)
			return nil, err
		}

		// Parse layer range
		layerRange := [2]uint64{0, 0}
		if layerRangeStr.Valid {
			start, end, err := parseRange(layerRangeStr.String)
			if err != nil {
				return nil, err
			}

			layerRange[0] = start
			layerRange[1] = end
		} else {
			return nil, fmt.Errorf("invalid layer range")
		}

		// Parse file range
		fileRange := [2]uint64{0, 0}
		if fileRangeStr.Valid {
			start, end, err := parseRange(fileRangeStr.String)
			if err != nil {
				return nil, err
			}
			fileRange[0] = start
			fileRange[1] = end
		} else {
			return nil, fmt.Errorf("invalid layer range")
		}

		// Append chunk to the results
		chunks = append(chunks, metadata{
			layerID:    layerID,
			flushed:    true, // since we're reading from the database, it's already flushed
			layerRange: layerRange,
			fileRange:  fileRange,
		})

		if fileRange[1] > maxEndOffset {
			maxEndOffset = fileRange[1]
		}

	}

	activeLayer, exists := sm.memtable[fileID]
	// If there's an active layer for this file and no version tag is specified, include chunks that are in memory (not persisted in the database)
	if exists && !hasVersion {
		for _, chunk := range activeLayer.chunks {
			// Only include chunk if it overlaps with the requested range
			if rangesOverlap(chunk.fileRange, [2]uint64{offset, offset + size}) {
				chunks = append(chunks, chunk)
				if chunk.fileRange[1] > maxEndOffset {
					maxEndOffset = chunk.fileRange[1]
				}
			}
		}
	}

	// Create buffer of appropriate size
	buf := make([]byte, maxEndOffset-offset)

	// Apply chunks in the correct order
	for _, chunk := range chunks {
		// Calculate the position in the buffer (relative to offset)
		var bufferPos uint64
		var chunkStartPos uint64
		var dataSize uint64
		var data []byte

		// The layer for this chunk hasn't been flushed to storage yet. It's in the active layer.
		if !chunk.flushed {
			data = activeLayer.data[chunk.layerRange[0]:chunk.layerRange[1]]
		} else {
			data, err = getChunkData(ctx, sm.db, sm.objectStore, chunk)
			if err != nil {
				sm.log.Error("Failed to get chunk data", "error", err)
				return nil, fmt.Errorf("failed to get chunk data: %w", err)
			}
		}

		// Handle case where chunk starts before the requested offset
		if chunk.fileRange[0] < offset {
			// Chunk starts before the requested offset
			// We only want to copy the portion starting from the requested offset
			chunkStartPos = offset - chunk.fileRange[0]
			bufferPos = 0 // Will start at the beginning of our buffer

			// Calculate how much data we're actually copying
			dataSize = uint64(len(data)) - chunkStartPos
		} else {
			// Chunk starts at or after the requested offset
			bufferPos = chunk.fileRange[0] - offset
			chunkStartPos = 0
			dataSize = uint64(len(data))
		}

		// Calculate the end position in the buffer
		endPos := bufferPos + dataSize

		if endPos <= uint64(len(buf)) {
			copy(buf[bufferPos:endPos], data[chunkStartPos:chunkStartPos+dataSize])
		} else {
			// Handle case where chunk extends beyond current buffer
			newBuf := make([]byte, endPos)
			copy(newBuf, buf)
			copy(newBuf[bufferPos:endPos], data[chunkStartPos:chunkStartPos+dataSize])
			buf = newBuf
		}
	}

	// Limit the returned data to the requested size
	if uint64(len(buf)) > size {
		buf = buf[:size]
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		sm.log.Error("Failed to commit transaction", "error", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if hasVersion {
		sm.log.Debug("Returning data range with version",
			"offset", offset,
			"size", len(buf),
			"version", options.version)
	} else {
		sm.log.Debug("Returning data range",
			"offset", offset,
			"size", len(buf))
	}

	return buf, nil
}

// InsertFile inserts a new file into the files table and returns its ID.
func (sm *Manager) InsertFile(ctx context.Context, name string) (int64, error) {
	sm.log.Debug("Inserting new file into metadata store", "name", name)

	query := `INSERT INTO files (name) VALUES ($1) RETURNING id;`
	var fileID int64
	err := sm.db.QueryRowContext(ctx, query, name).Scan(&fileID)
	if err != nil {
		sm.log.Error("Failed to insert new file", "name", name, "error", err)
		return 0, err
	}

	sm.log.Debug("File inserted successfully", "name", name, "fileID", fileID)
	return fileID, nil
}

type queryOpt func(*queryOpts)

type queryOpts struct {
	tx *sql.Tx
}

func withTx(tx *sql.Tx) queryOpt {
	return func(opts *queryOpts) {
		opts.tx = tx
	}
}

func (sm *Manager) GetFileIDByName(ctx context.Context, name string, opts ...queryOpt) (int64, error) {
	query := `SELECT id FROM files WHERE name = $1;`
	var fileID int64

	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error

	if options.tx != nil {
		err = options.tx.QueryRowContext(ctx, query, name).Scan(&fileID)
	} else {
		err = sm.db.QueryRowContext(ctx, query, name).Scan(&fileID)
	}

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, ErrNotFound
		}
		sm.log.Error("Failed to retrieve file ID", "name", name, "error", err)
		return 0, err
	}

	return fileID, nil
}

func (sm *Manager) insertChunk(ctx context.Context, layerID int64, c metadata, opts ...queryOpt) error {
	sm.log.Debug("Inserting chunk in the database",
		"layerID", layerID,
		"layerRange", c.layerRange,
		"fileRange", c.fileRange)

	layerRangeStr := fmt.Sprintf("[%d,%d)", c.layerRange[0], c.layerRange[1])
	fileRangeStr := fmt.Sprintf("[%d,%d)", c.fileRange[0], c.fileRange[1])

	query := `INSERT INTO chunks (snapshot_layer_id, layer_range, file_range) 
	         VALUES ($1, $2, $3);`

	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	if options.tx != nil {
		_, err = options.tx.ExecContext(ctx, query, layerID, layerRangeStr, fileRangeStr)
	} else {
		_, err = sm.db.ExecContext(ctx, query, layerID, layerRangeStr, fileRangeStr)
	}

	if err != nil {
		sm.log.Error("Failed to insert chunk", "layerID", layerID, "error", err)
		return fmt.Errorf("failed to insert chunk: %w", err)
	}

	sm.log.Debug("Chunk inserted successfully", "layerID", layerID)
	return nil
}

// close closes the database.
func (sm *Manager) Close() error {
	sm.log.Debug("Closing metadata store database connection")
	err := sm.db.Close()
	if err != nil {
		sm.log.Error("Error closing database connection", "error", err)
	} else {
		sm.log.Debug("Database connection closed successfully")
	}
	return err
}

// calcSizeOf calculates the total byte size of the virtual file from all layers and their chunks, respecting layer creation order and handling overlapping file ranges.
//
// File offset →    0    5    10   15   20   25   30   35   40
// Layer 3 (newest) ···╔═════╗···╔═══╗··························
// Layer 2          ········╔══════════╗·······╔═══════════════╗
// Layer 1 (oldest) ╔═══════════════════════════╗···············
//
//									                           ↑
//		      							                       |
//	              							         File size = 44
//
// File size is determined by the highest end offset across all chunks
func (sm *Manager) calcSizeOf(ctx context.Context, fileID int64, opts ...queryOpt) (uint64, error) {
	activeLayer, exists := sm.memtable[fileID]
	if exists && activeLayer != nil && len(activeLayer.chunks) > 0 {
		maxEndOffset := uint64(0)
		for _, chunk := range activeLayer.chunks {
			if chunk.fileRange[1] > maxEndOffset {
				maxEndOffset = chunk.fileRange[1]
			}
		}
		return maxEndOffset, nil
	}

	query := `
		SELECT upper(e.file_range)
		FROM chunks e
			INNER JOIN snapshot_layers l ON e.snapshot_layer_id = l.id
		WHERE l.file_id = $1
		ORDER BY upper(e.file_range) DESC
		LIMIT 1;
	`
	var highestOffCommited uint64
	var highestOffInActiveLayer uint64

	if exists && activeLayer != nil {
		for _, chunk := range activeLayer.chunks {
			if chunk.fileRange[1] > highestOffInActiveLayer {
				highestOffInActiveLayer = chunk.fileRange[1]
			}
		}
	}

	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	if options.tx != nil {
		err = options.tx.QueryRowContext(ctx, query, fileID).Scan(&highestOffCommited)
	} else {
		err = sm.db.QueryRowContext(ctx, query, fileID).Scan(&highestOffCommited)
	}

	if err != nil {
		// If the file has no chunks, its size is 0
		if err == sql.ErrNoRows {
			return highestOffInActiveLayer, nil
		}
		sm.log.Error("Failed to query file ranges", "error", err, "fileID", fileID)
		return 0, err
	}

	return max(highestOffCommited, highestOffInActiveLayer), nil
}

// LoadLayersByFileID loads all layers associated with a specific file ID from the database.
func (sm *Manager) LoadLayersByFileID(ctx context.Context, fileID int64, opts ...queryOpt) ([]*layer, error) {
	sm.log.Debug("Loading layers for file from metadata store", "fileID", fileID)

	query := `
		SELECT snapshot_layers.id, file_id, version_id, tag
		FROM snapshot_layers
		LEFT JOIN versions ON snapshot_layers.version_id = versions.id
		WHERE file_id = $1 ORDER BY id ASC;
	`

	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var rows *sql.Rows
	var err error

	if options.tx != nil {
		rows, err = options.tx.QueryContext(ctx, query, fileID)
	} else {
		rows, err = sm.db.QueryContext(ctx, query, fileID)
	}

	if err != nil {
		sm.log.Error("Failed to query layers for file", "fileID", fileID, "error", err)
		return nil, err
	}
	defer rows.Close()

	var layers []*layer
	layerCount := 0

	for rows.Next() {
		var id int64
		var versionID sql.NullInt64
		var tag sql.NullString
		if err := rows.Scan(&id, &fileID, &versionID, &tag); err != nil {
			sm.log.Error("Error scanning layer row", "error", err)
			return nil, err
		}

		layer := &layer{FileID: fileID}
		layer.id = id
		if versionID.Valid {
			layer.VersionID = versionID.Int64
		} else {
			layer.VersionID = 0
		}
		if tag.Valid {
			layer.Tag = tag.String
		}
		layers = append(layers, layer)

		sm.log.Debug("Loaded layer for file", "layerID", id, "versionID", layer.VersionID, "tag", layer.Tag)
		layerCount++
	}

	sm.log.Debug("Layers for file loaded successfully", "fileID", fileID, "count", layerCount)
	return layers, nil
}

func (sm *Manager) Checkpoint(ctx context.Context, filename string, version string) error {
	sm.mu.Lock()         // Lock before accessing activeLayers
	defer sm.mu.Unlock() // Ensure unlock when function returns

	tx, err := sm.db.BeginTx(ctx, nil)
	if err != nil {
		sm.log.Error("Failed to begin transaction", "error", err)
		return err
	}

	// Setup deferred rollback in case of error or panic
	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				sm.log.Error("Failed to rollback transaction after panic", "error", rbErr)
			}
			// Re-panic after rollback
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				sm.log.Error("Failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	fileID, err := sm.GetFileIDByName(ctx, filename, withTx(tx))
	if err != nil {
		if err == ErrNotFound {
			sm.log.Warn("File not found, nothing to checkpoint", "filename", filename)
			return nil
		}
		sm.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	activeLayer, exists := sm.memtable[fileID]
	if !exists || len(activeLayer.data) == 0 {
		sm.log.Warn("No active layer or data to checkpoint", "filename", filename)
		return nil // No active layer means no changes to checkpoint
	}

	insertVersionQ := `INSERT INTO versions (tag) VALUES ($1) RETURNING id;`
	var versionID int64
	err = tx.QueryRowContext(ctx, insertVersionQ, version).Scan(&versionID)
	if err != nil {
		sm.log.Error("Failed to insert new version", "tag", version, "error", err)
		return fmt.Errorf("failed to insert new version: %w", err)
	}

	objectKey := fmt.Sprintf("layers/%s/%d-%d", filename, fileID, versionID)

	err = sm.objectStore.PutObject(ctx, objectKey, activeLayer.data)
	if err != nil {
		sm.log.Error("Failed to upload data to object store", "error", err)
		return fmt.Errorf("failed to upload data to object store: %w", err)
	}

	insertLayerQ := `INSERT INTO snapshot_layers (file_id, version_id, s3_object_key) VALUES ($1, $2, $3) RETURNING id;`
	var layerID int64
	err = tx.QueryRowContext(ctx, insertLayerQ, fileID, versionID, objectKey).Scan(&layerID)
	if err != nil {
		sm.log.Error("Failed to commit layer with version", "error", err)
		return fmt.Errorf("failed to commit layer with version: %w", err)
	}

	for _, c := range activeLayer.chunks {
		err = sm.insertChunk(ctx, layerID, c, withTx(tx))
		if err != nil {
			sm.log.Error("Failed to commit layer's chunks", "error", err)
			return fmt.Errorf("failed to commit layer's chunks: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		sm.log.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	delete(sm.memtable, fileID)

	sm.log.Debug("Checkpoint successful", "layerID", layerID, "objectKey", objectKey)

	return nil
}

// fileInfo represents basic file information
type fileInfo struct {
	ID   int
	Name string
}

// GetAllFiles returns a list of all files in the database
func (sm *Manager) GetAllFiles(ctx context.Context) ([]fileInfo, error) {
	query := `SELECT id, name FROM files;`
	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		sm.log.Error("Failed to query files", "error", err)
		return nil, err
	}
	defer rows.Close()

	var files []fileInfo
	for rows.Next() {
		var file fileInfo
		if err := rows.Scan(&file.ID, &file.Name); err != nil {
			sm.log.Error("Failed to scan file row", "error", err)
			return nil, err
		}
		files = append(files, file)
	}

	if err := rows.Err(); err != nil {
		sm.log.Error("Error iterating file rows", "error", err)
		return nil, err
	}

	return files, nil
}

// parseRange parses strings of the form "[start, end)" into two uint64 values
func parseRange(rg string) (uint64, uint64, error) {
	parts := strings.Split(strings.Trim(rg, "[)"), ",")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format: %s", rg)
	}

	start, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	end, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return start, end, nil
}

// rangesOverlap checks if two ranges [start1, end1) and [start2, end2) overlap
func rangesOverlap(range1 [2]uint64, range2 [2]uint64) bool {
	return range1[0] < range2[1] && range2[0] < range1[1]
}

// getChunkData retrieves chunk data from the object store using range requests
func getChunkData(ctx context.Context, db *sql.DB, objectStore ObjectStore, c metadata) ([]byte, error) {
	var objectKey string

	// Get the object key from the snapshot_layers table
	err := db.QueryRowContext(ctx, `
		SELECT s3_object_key
		FROM snapshot_layers
		WHERE id = $1
	`, c.layerID).Scan(&objectKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("error retrieving object key: %w", err)
	}

	layerSize := c.layerRange[1] - c.layerRange[0]
	data, err := objectStore.GetObject(ctx, objectKey, [2]uint64{c.layerRange[0], c.layerRange[1] - 1})
	if err != nil {
		return nil, fmt.Errorf("error retrieving data from object store: %w", err)
	}

	if uint64(len(data)) != layerSize {
		return nil, fmt.Errorf("received incorrect number of bytes from object store: got %d, expected %d", len(data), layerSize)
	}

	return data, nil
}
