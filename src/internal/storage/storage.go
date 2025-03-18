package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/google/uuid"
)

type snapshotLayer struct {
	id        int64
	FileID    int64
	Active    bool // whether of not it is the current active layer that is being written to
	VersionID int64
	Tag       string
}

var ErrNotFound = errors.New("not found")

type Manager struct {
	db  *sql.DB
	log *log.Logger
}

// NewManager creates (or reloads) a StorageManager using the provided metadataStore.
func NewManager(db *sql.DB, log *log.Logger) *Manager {
	managerLog := log.With()
	managerLog.SetPrefix("💽 storage")

	sm := &Manager{
		db:  db,
		log: managerLog,
	}

	return sm
}

// WriteFile writes data to the active layer at the specified offset.
// It returns the active layer's ID and the offset where the data was written.
func (sm *Manager) WriteFile(filename string, data []byte, offset uint64) error {
	sm.log.Debug("Writing data", "filename", filename, "size", len(data), "offset", offset)

	// Begin transaction
	tx, err := sm.db.Begin()
	if err != nil {
		sm.log.Error("Failed to begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
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

	// Get the file ID from the file name
	fileID, err := sm.GetFileIDByName(filename, withTx(tx))
	if err != nil {
		sm.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	// Get the active layer
	query := `SELECT id FROM snapshot_layers WHERE file_id = $1 AND active = 1 ORDER BY id ASC LIMIT 1;`
	var layerID int64
	err = tx.QueryRow(query, fileID).Scan(&layerID)
	if err != nil {
		sm.log.Error("Failed to query active layer", "filename", filename, "error", err)
		return fmt.Errorf("failed to query active layer: %w", err)
	}

	fileSize, err := sm.calcSizeOf(fileID, withTx(tx))
	if err != nil {
		sm.log.Error("Failed to calculate file size", "fileID", fileID, "error", err)
		return fmt.Errorf("failed to calculate file size: %w", err)
	}

	if offset > fileSize {
		// Calculate how many zero bytes to add
		bytesToAdd := offset - fileSize

		// Create a buffer of zero bytes
		zeroes := make([]byte, bytesToAdd)

		layerRange, fileRange, err := sm.calculateRanges(layerID, fileSize, len(zeroes), withTx(tx))
		if err != nil {
			sm.log.Error("Failed to calculate ranges", "layerID", layerID, "offset", offset, "error", err)
			return fmt.Errorf("failed to calculate ranges: %w", err)
		}

		if err = sm.insertChunk(layerID, zeroes, layerRange, fileRange, withTx(tx)); err != nil {
			sm.log.Error("Failed to record chunk", "layerID", layerID, "offset", offset, "error", err)
			return fmt.Errorf("failed to record chunk: %w", err)
		}

		sm.log.Debug("Added chunk to active layer", "layerID", layerID, "offset", offset, "size", len(zeroes))
	}

	sm.log.Debug("Added chunk to active layer", "layerID", layerID, "offset", offset, "size", len(data))

	// Calculate ranges and record chunk in the database
	layerRange, fileRange, err := sm.calculateRanges(layerID, offset, len(data), withTx(tx))
	if err != nil {
		sm.log.Error("Failed to calculate ranges", "layerID", layerID, "offset", offset, "error", err)
		return fmt.Errorf("failed to calculate ranges: %w", err)
	}

	if err = sm.insertChunk(layerID, data, layerRange, fileRange, withTx(tx)); err != nil {
		sm.log.Error("Failed to record chunk", "layerID", layerID, "offset", offset, "error", err)
		return fmt.Errorf("failed to record chunk: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		sm.log.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	sm.log.Debug("Data written successfully", "layerID", layerID, "offset", offset, "size", len(data))
	return nil
}

// calculateRanges computes the layer-relative and file-absolute ranges for a chunk
func (sm *Manager) calculateRanges(layerID int64, offset uint64, dataSize int, opts ...queryOpt) ([2]uint64, [2]uint64, error) {
	dataLength := uint64(dataSize)

	layerSize, err := sm.calcLayerSize(layerID, opts...)
	if err != nil {
		return [2]uint64{}, [2]uint64{}, fmt.Errorf("failed to retrieve layer size: %w", err)
	}

	layerStart := layerSize
	layerEnd := layerStart + dataLength
	layerRange := [2]uint64{layerStart, layerEnd}

	fileStart := offset
	fileEnd := offset + dataLength
	fileRange := [2]uint64{fileStart, fileEnd}

	sm.log.Debug("Calculated ranges for chunk",
		"layerID", layerID,
		"layerRange", layerRange,
		"fileRange", fileRange)

	return layerRange, fileRange, nil
}

func (sm *Manager) SizeOf(filename string) (uint64, error) {
	fileID, err := sm.GetFileIDByName(filename)
	if err != nil {
		return 0, err
	}

	return sm.calcSizeOf(fileID)
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
func (sm *Manager) ReadFile(filename string, offset uint64, size uint64, opts ...readFileOpt) ([]byte, error) {
	// Process options
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

	// Begin transaction for consistent reads
	tx, err := sm.db.Begin()
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

	// Get the file ID from the file name
	fileID, err := sm.GetFileIDByName(filename, withTx(tx))
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
		err = tx.QueryRow(`
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
	if hasVersion {
		query = `
			SELECT c.snapshot_layer_id, c.data, c.layer_range, c.file_range
			FROM chunks c
			INNER JOIN snapshot_layers l ON c.snapshot_layer_id = l.id
			WHERE
				l.id <= $1 AND l.file_id = $2 AND
				c.file_range && int8range($3, $4)
			ORDER BY l.id ASC, c.id ASC;
		`
		rows, err = tx.Query(query, versionedLayerId, fileID, offset, offset+size)
	} else {
		query = `
			SELECT c.snapshot_layer_id, c.data, c.layer_range, c.file_range
			FROM chunks c
			INNER JOIN snapshot_layers l ON c.snapshot_layer_id = l.id
			WHERE
				l.file_id = $1 AND c.file_range && int8range($2, $3)
			ORDER BY l.id ASC, c.id ASC;
		`
		rows, err = tx.Query(query, fileID, offset, offset+size)
	}
	if err != nil {
		sm.log.Error("Failed to query chunks", "error", err)
		return nil, err
	}
	defer rows.Close()

	// Map to store chunks by layer ID
	chunks := make([]chunk, 0)

	var maxEndOffset uint64

	// Process chunks that could match the requested range
	for rows.Next() {
		var layerID int64
		var data []byte
		var layerRangeStr, fileRangeStr sql.NullString

		if err := rows.Scan(&layerID, &data, &layerRangeStr, &fileRangeStr); err != nil {
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
		chunks = append(chunks, chunk{
			layerID:    layerID,
			data:       data,
			layerRange: layerRange,
			fileRange:  fileRange,
		})

		if fileRange[1] > maxEndOffset {
			maxEndOffset = fileRange[1]
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

		// Handle case where chunk starts before the requested offset
		if chunk.fileRange[0] < offset {
			// Chunk starts before the requested offset
			// We only want to copy the portion starting from the requested offset
			chunkStartPos = offset - chunk.fileRange[0]
			bufferPos = 0 // Will start at the beginning of our buffer

			// Calculate how much data we're actually copying
			dataSize = uint64(len(chunk.data)) - chunkStartPos
		} else {
			// Chunk starts at or after the requested offset
			bufferPos = chunk.fileRange[0] - offset
			chunkStartPos = 0
			dataSize = uint64(len(chunk.data))
		}

		// Calculate the end position in the buffer
		endPos := bufferPos + dataSize

		if endPos <= uint64(len(buf)) {
			copy(buf[bufferPos:endPos], chunk.data[chunkStartPos:chunkStartPos+dataSize])
		} else {
			// Handle case where chunk extends beyond current buffer
			newBuf := make([]byte, endPos)
			copy(newBuf, buf)
			copy(newBuf[bufferPos:endPos], chunk.data[chunkStartPos:chunkStartPos+dataSize])
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

func (sm *Manager) calcLayerSize(layerID int64, opts ...queryOpt) (uint64, error) {
	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	query := `
		SELECT upper(layer_range)::bigint AS size
		FROM chunks
		WHERE snapshot_layer_id = $1
		ORDER BY upper(layer_range) DESC
		LIMIT 1;
	`

	var size sql.NullInt64
	var err error

	if options.tx != nil {
		err = options.tx.QueryRow(query, layerID).Scan(&size)
	} else {
		err = sm.db.QueryRow(query, layerID).Scan(&size)
	}

	if err != nil {
		// if the layer has no chunks, its size is 0
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	if !size.Valid {
		return 0, fmt.Errorf("invalid base value for layer %d", layerID)
	}

	return uint64(size.Int64), nil
}

// chunk holds data for a write chunk.
type chunk struct {
	layerID    int64
	data       []byte
	layerRange [2]uint64 // Range within a layer as an array of two integers
	fileRange  [2]uint64 // Range within the virtual file as an array of two integers
}

// insertActiveLayer inserts a new layer record, making it the active layer for the file.
// It accepts optional functional parameters for configuration.
func (sm *Manager) insertActiveLayer(layer *snapshotLayer, opts ...queryOpt) (int64, error) {
	sm.log.Debug("Recording new layer in metadata store", "layerID", layer.id)

	// Process options
	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	query := `INSERT INTO snapshot_layers (file_id, active) VALUES ($1, 1) RETURNING id;`
	var newID int64
	var err error

	if options.tx != nil {
		// Use the provided transaction
		err = options.tx.QueryRow(query, layer.FileID).Scan(&newID)
	} else {
		// Use the database connection directly
		err = sm.db.QueryRow(query, layer.FileID).Scan(&newID)
	}

	if err != nil {
		sm.log.Error("Failed to record new layer", "layerID", layer.id, "error", err)
		return 0, err
	}

	layer.id = newID

	sm.log.Debug("Layer recorded successfully", "layerID", newID)
	return newID, nil
}

// InsertFile inserts a new file into the files table and returns its ID.
func (sm *Manager) InsertFile(name string) (int64, error) {
	sm.log.Debug("Inserting new file into metadata store", "name", name)

	query := `INSERT INTO files (name) VALUES ($1) RETURNING id;`
	var fileID int64
	err := sm.db.QueryRow(query, name).Scan(&fileID)
	if err != nil {
		sm.log.Error("Failed to insert new file", "name", name, "error", err)
		return 0, err
	}

	// After inserting the file, create an initial active layer for it
	layer := &snapshotLayer{FileID: fileID}
	_, err = sm.insertActiveLayer(layer)
	if err != nil {
		sm.log.Error("Failed to create initial layer for new file", "fileID", fileID, "error", err)
		return 0, fmt.Errorf("failed to create initial layer for new file: %w", err)
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

func (sm *Manager) GetFileIDByName(name string, opts ...queryOpt) (int64, error) {
	query := `SELECT id FROM files WHERE name = $1;`
	var fileID int64

	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error

	if options.tx != nil {
		err = options.tx.QueryRow(query, name).Scan(&fileID)
	} else {
		err = sm.db.QueryRow(query, name).Scan(&fileID)
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

// insertChunk inserts a new chunk record.
// Now includes layer_range and file_range parameters
func (sm *Manager) insertChunk(layerID int64, data []byte, layerRange [2]uint64, fileRange [2]uint64, opts ...queryOpt) error {
	sm.log.Debug("Inserting chunk in the database",
		"layerID", layerID,
		"dataSize", len(data),
		"layerRange", layerRange,
		"fileRange", fileRange)

	layerRangeStr := fmt.Sprintf("[%d,%d)", layerRange[0], layerRange[1])
	fileRangeStr := fmt.Sprintf("[%d,%d)", fileRange[0], fileRange[1])

	query := `INSERT INTO chunks (snapshot_layer_id, data, layer_range, file_range) 
	         VALUES ($1, $2, $3, $4);`

	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	if options.tx != nil {
		_, err = options.tx.Exec(query, layerID, data, layerRangeStr, fileRangeStr)
	} else {
		_, err = sm.db.Exec(query, layerID, data, layerRangeStr, fileRangeStr)
	}

	if err != nil {
		sm.log.Error("Failed to insert chunk", "layerID", layerID, "error", err)
		return err
	}

	sm.log.Debug("Chunk inserted successfully", "layerID", layerID, "dataSize", len(data))
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
func (sm *Manager) calcSizeOf(fileID int64, opts ...queryOpt) (uint64, error) {
	query := `
		SELECT upper(e.file_range)
		FROM chunks e
			INNER JOIN snapshot_layers l ON e.snapshot_layer_id = l.id
		WHERE l.file_id = $1
		ORDER BY upper(e.file_range) DESC
		LIMIT 1;
	`
	var highestOff uint64

	options := queryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	if options.tx != nil {
		err = options.tx.QueryRow(query, fileID).Scan(&highestOff)
	} else {
		err = sm.db.QueryRow(query, fileID).Scan(&highestOff)
	}

	if err != nil {
		// If the file has no chunks, its size is 0
		if err == sql.ErrNoRows {
			return 0, nil
		}
		sm.log.Error("Failed to query file ranges", "error", err, "fileID", fileID)
		return 0, err
	}

	return highestOff, nil
}

// LoadLayersByFileID loads all layers associated with a specific file ID from the database.
func (sm *Manager) LoadLayersByFileID(fileID int64, opts ...queryOpt) ([]*snapshotLayer, error) {
	sm.log.Debug("Loading layers for file from metadata store", "fileID", fileID)

	query := `
		SELECT snapshot_layers.id, file_id, active, version_id, tag
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
		rows, err = options.tx.Query(query, fileID)
	} else {
		rows, err = sm.db.Query(query, fileID)
	}

	if err != nil {
		sm.log.Error("Failed to query layers for file", "fileID", fileID, "error", err)
		return nil, err
	}
	defer rows.Close()

	var layers []*snapshotLayer
	layerCount := 0

	for rows.Next() {
		var id, activeInt int64
		var versionID sql.NullInt64
		var tag sql.NullString
		if err := rows.Scan(&id, &fileID, &activeInt, &versionID, &tag); err != nil {
			sm.log.Error("Error scanning layer row", "error", err)
			return nil, err
		}

		layer := &snapshotLayer{FileID: fileID}
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

		active := activeInt != 0
		sm.log.Debug("Loaded layer for file", "layerID", id, "active", active, "versionID", layer.VersionID, "tag", layer.Tag)
		layerCount++
	}

	sm.log.Debug("Layers for file loaded successfully", "fileID", fileID, "count", layerCount)
	return layers, nil
}

// deleteFile removes a file and its associated layers and chunks from the database within a transaction.
// ! FIX(vinimdocarmo): this shouldn't touch the existing layers, but create a new one marking file as deleted.
func (sm *Manager) DeleteFile(name string) error {
	sm.log.Debug("Deleting file and its associated data from metadata store", "name", name)

	// Begin a transaction
	tx, err := sm.db.Begin()
	if err != nil {
		sm.log.Error("Failed to begin transaction", "error", err)
		return err
	}

	// Ensure the transaction is rolled back in case of an error
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			sm.log.Error("Transaction failed, rolling back", "error", err)
			tx.Rollback()
		} else {
			err = tx.Commit()
			if err != nil {
				sm.log.Error("Failed to commit transaction", "error", err)
			}
		}
	}()

	// When the DuckDB WAL file is delete it means a CHECKPOINT is being made
	// which in this case we want to commit the changes to the database
	// in a new version tag.
	if strings.HasSuffix(name, ".wal") {
		version := uuid.New().String()
		database := strings.TrimSuffix(name, ".wal")
		sm.log.Info("WAL is being deleted, commiting database checkpoint", "name", database, "version", version)
		err = sm.Checkpoint(database, version)
		if err != nil {
			sm.log.Error("Failed to checkpoint WAL file", "error", err)
			return err
		}
	}

	// Retrieve the file ID
	fileID, err := sm.GetFileIDByName(name, withTx(tx))
	if err != nil {
		sm.log.Error("Failed to retrieve file ID", "name", name, "error", err)
		return err
	}

	if fileID == 0 {
		sm.log.Error("File not found, nothing to delete", "name", name)
		return nil
	}

	// Delete all chunks associated with the file's layers
	deleteChunksQuery := `DELETE FROM chunks WHERE snapshot_layer_id IN (SELECT id FROM snapshot_layers WHERE file_id = $1);`
	_, err = tx.Exec(deleteChunksQuery, fileID)
	if err != nil {
		sm.log.Error("Failed to delete chunks for file", "name", name, "error", err)
		return err
	}

	// Delete all layers associated with the file
	deleteLayersQuery := `DELETE FROM snapshot_layers WHERE file_id = $1;`
	_, err = tx.Exec(deleteLayersQuery, fileID)
	if err != nil {
		sm.log.Error("Failed to delete layers for file", "name", name, "error", err)
		return err
	}

	// Delete the file itself
	deleteFileQuery := `DELETE FROM files WHERE id = $1;`
	_, err = tx.Exec(deleteFileQuery, fileID)
	if err != nil {
		sm.log.Error("Failed to delete file", "name", name, "error", err)
		return err
	}

	sm.log.Info("File and its associated data deleted successfully", "name", name)
	return nil
}

func (sm *Manager) Checkpoint(filename string, version string) error {
	// Start transaction
	tx, err := sm.db.Begin()
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

	// Get the file ID from the file name
	fileID, err := sm.GetFileIDByName(filename, withTx(tx))
	if err != nil {
		sm.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	// Get the current active snapshot layer ID
	query := `SELECT id FROM snapshot_layers WHERE file_id = $1 AND active = 1 ORDER BY id ASC LIMIT 1;`
	var layerID int64
	err = tx.QueryRow(query, fileID).Scan(&layerID)
	if err != nil {
		sm.log.Error("Failed to query active layer", "filename", filename, "error", err)
		return fmt.Errorf("failed to query active layer: %w", err)
	}

	// Insert version within transaction
	insertVersionQ := `INSERT INTO versions (tag) VALUES ($1) RETURNING id;`
	var versionID int64
	err = tx.QueryRow(insertVersionQ, version).Scan(&versionID)
	if err != nil {
		sm.log.Error("Failed to insert new version", "tag", version, "error", err)
		return err
	}

	// Update layer within transaction
	updateLayerQ := `UPDATE snapshot_layers SET active = 0, version_id = $1 WHERE id = $2;`
	_, err = tx.Exec(updateLayerQ, versionID, layerID)
	if err != nil {
		sm.log.Error("Failed to commit layer", "id", layerID, "error", err)
		return err
	}

	// Create a new active layer within the same transaction
	layer := &snapshotLayer{FileID: fileID}
	layerId, err := sm.insertActiveLayer(layer, withTx(tx))
	if err != nil {
		sm.log.Error("Failed to record new layer", "error", err)
		return fmt.Errorf("failed to record new layer: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		sm.log.Error("Failed to commit transaction", "error", err)
		return err
	}

	sm.log.Debug("Checkpoint successful", "layerID", layerId)

	return nil
}

// fileInfo represents basic file information
type fileInfo struct {
	ID   int
	Name string
}

// GetAllFiles returns a list of all files in the database
func (sm *Manager) GetAllFiles() ([]fileInfo, error) {
	query := `SELECT id, name FROM files;`
	rows, err := sm.db.Query(query)
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
