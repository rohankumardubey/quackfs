package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// MetadataStore manages persistent layer metadata using PostgreSQL.
type MetadataStore struct {
	db *sql.DB
}

func (ms *MetadataStore) GetLayerBase(layerID int) (uint64, error) {
	query := `
		SELECT lower(file_range)::bigint
		FROM entries
		WHERE layer_id = $1
		ORDER BY lower(file_range) ASC
		LIMIT 1;
	`

	var base sql.NullInt64
	err := ms.db.QueryRow(query, layerID).Scan(&base)
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

// NewMetadataStore opens a PostgreSQL database connection and initializes the tables.
// The connStr should be a PostgreSQL connection string in keyword/value format:
// "host=localhost port=5432 user=postgres password=postgres dbname=difffs sslmode=disable"
// Alternatively, URL format is also supported: "postgres://username:password@localhost/dbname?sslmode=disable"
func NewMetadataStore(conn string) (*MetadataStore, error) {
	Logger.Debug("Creating new metadata store with connection string")

	db, err := sql.Open("postgres", conn)
	if err != nil {
		Logger.Error("Failed to open database connection", "error", err)
		return nil, err
	}

	Logger.Debug("Testing database connection")
	if err = db.Ping(); err != nil {
		Logger.Error("Failed to connect to PostgreSQL", "error", err)
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	Logger.Debug("Database connection established successfully")

	ms := &MetadataStore{db: db}
	if err := ms.init(); err != nil {
		Logger.Error("Failed to initialize metadata store tables", "error", err)
		return nil, err
	}

	Logger.Debug("Metadata store successfully created and initialized")
	return ms, nil
}

// init creates the necessary tables if they do not exist.
func (ms *MetadataStore) init() error {
	Logger.Debug("Initializing metadata store tables")

	// Create files table.
	filesTable := `
	CREATE TABLE IF NOT EXISTS files (
		id SERIAL PRIMARY KEY,
		name TEXT UNIQUE NOT NULL
	);
	`
	// Create layers table.
	layerTable := `
	CREATE TABLE IF NOT EXISTS layers (
		id SERIAL PRIMARY KEY,
		file_id INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		sealed INTEGER DEFAULT 0
	);
	`
	// Create entries table with proper index creation and range columns
	entryTable := `
	CREATE TABLE IF NOT EXISTS entries (
		layer_id INTEGER REFERENCES layers(id),
		offset_value BIGINT NOT NULL,
		data BYTEA NOT NULL,
		layer_range INT8RANGE NOT NULL,
		file_range INT8RANGE NOT NULL,
		PRIMARY KEY (layer_id, offset_value)
	);
	`
	Logger.Debug("Creating files table if not exists")
	if _, err := ms.db.Exec(filesTable); err != nil {
		Logger.Error("Failed to create files table", "error", err)
		return err
	}

	Logger.Debug("Creating layers table if not exists")
	if _, err := ms.db.Exec(layerTable); err != nil {
		Logger.Error("Failed to create layers table", "error", err)
		return err
	}

	Logger.Debug("Creating entries table if not exists")
	if _, err := ms.db.Exec(entryTable); err != nil {
		Logger.Error("Failed to create entries table", "error", err)
		return err
	}

	Logger.Debug("Metadata store tables initialized successfully")
	return nil
}

// InsertFile inserts a new file into the files table and returns its ID.
func (ms *MetadataStore) InsertFile(name string) (int, error) {
	Logger.Debug("Inserting new file into metadata store", "name", name)

	query := `INSERT INTO files (name) VALUES ($1) RETURNING id;`
	var fileID int
	err := ms.db.QueryRow(query, name).Scan(&fileID)
	if err != nil {
		Logger.Error("Failed to insert new file", "name", name, "error", err)
		return 0, err
	}

	// After inserting the file, create an initial unsealed layer for it
	layer := NewLayer(fileID)
	_, err = ms.RecordNewLayer(layer)
	if err != nil {
		Logger.Error("Failed to create initial layer for new file", "fileID", fileID, "error", err)
		return 0, fmt.Errorf("failed to create initial layer for new file: %w", err)
	}

	Logger.Debug("File inserted successfully", "name", name, "fileID", fileID)
	return fileID, nil
}

// GetFileIDByName retrieves the file ID for a given file name.
func (ms *MetadataStore) GetFileIDByName(name string) (int, error) {
	Logger.Debug("Retrieving file ID by name", "name", name)

	query := `SELECT id FROM files WHERE name = $1;`
	var fileID int
	err := ms.db.QueryRow(query, name).Scan(&fileID)
	if err != nil {
		if err == sql.ErrNoRows {
			Logger.Warn("File not found", "name", name)
			return 0, nil
		}
		Logger.Error("Failed to retrieve file ID", "name", name, "error", err)
		return 0, err
	}

	Logger.Debug("File ID retrieved successfully", "name", name, "fileID", fileID)
	return fileID, nil
}

// RecordNewLayer inserts a new layer record.
func (ms *MetadataStore) RecordNewLayer(layer *Layer) (int, error) {
	Logger.Debug("Recording new layer in metadata store", "layerID", layer.ID)

	query := `INSERT INTO layers (file_id, sealed) VALUES ($1, 0) RETURNING id;`
	var newID int
	err := ms.db.QueryRow(query, layer.FileID).Scan(&newID)
	if err != nil {
		Logger.Error("Failed to record new layer", "layerID", layer.ID, "error", err)
		return 0, err
	}

	layer.ID = newID

	Logger.Debug("Layer recorded successfully", "layerID", newID)
	return newID, nil
}

// SealLayer marks the layer with the given id as sealed.
func (ms *MetadataStore) SealLayer(layerID int) error {
	Logger.Debug("Sealing layer in metadata store", "layerID", layerID)

	query := `UPDATE layers SET sealed = 1 WHERE id = $1;`
	result, err := ms.db.Exec(query, layerID)
	if err != nil {
		Logger.Error("Failed to seal layer", "layerID", layerID, "error", err)
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	Logger.Debug("Layer sealed successfully", "layerID", layerID, "rowsAffected", rowsAffected)
	return nil
}

// RecordEntry inserts a new entry record.
// Now includes layer_range and file_range parameters
func (ms *MetadataStore) RecordEntry(layerID int, offset uint64, data []byte, layerRange [2]uint64, fileRange [2]uint64) error {
	Logger.Debug("Recording entry in metadata store",
		"layerID", layerID,
		"offset", offset,
		"dataSize", len(data),
		"layerRange", layerRange,
		"fileRange", fileRange)

	layerRangeStr := fmt.Sprintf("[%d,%d)", layerRange[0], layerRange[1])
	fileRangeStr := fmt.Sprintf("[%d,%d)", fileRange[0], fileRange[1])

	query := `INSERT INTO entries (layer_id, offset_value, data, layer_range, file_range) 
	         VALUES ($1, $2, $3, $4, $5);`
	_, err := ms.db.Exec(query, layerID, offset, data, layerRangeStr, fileRangeStr)
	if err != nil {
		Logger.Error("Failed to record entry", "layerID", layerID, "offset", offset, "error", err)
		return err
	}

	Logger.Debug("Entry recorded successfully", "layerID", layerID, "offset", offset, "dataSize", len(data))
	return nil
}

// LoadLayers loads all layer metadata from the database sorted by id.
// It returns a slice of layers (with only the metadata), the next available layer id,
// and the highest base value among layers.
func (ms *MetadataStore) LoadLayers() ([]*Layer, error) {
	Logger.Debug("Loading layers from metadata store")

	query := `SELECT id, file_id, sealed FROM layers ORDER BY id ASC;`
	rows, err := ms.db.Query(query)
	if err != nil {
		Logger.Error("Failed to query layers", "error", err)
		return nil, err
	}
	defer rows.Close()

	var layers []*Layer
	layerCount := 0

	for rows.Next() {
		var id, fileID, sealedInt int
		if err := rows.Scan(&id, &fileID, &sealedInt); err != nil {
			Logger.Error("Error scanning layer row", "error", err)
			return nil, err
		}

		layer := NewLayer(fileID)
		layer.ID = id
		layers = append(layers, layer)

		sealed := sealedInt != 0
		Logger.Debug("Loaded layer", "layerID", id, "sealed", sealed)
		layerCount++
	}

	Logger.Debug("Layers loaded successfully", "count", layerCount)
	return layers, nil
}

// LoadEntries loads all entry records from the database and groups them by layer_id.
func (ms *MetadataStore) LoadEntries() (map[int][]EntryRecord, error) {
	Logger.Debug("Loading entries from metadata store")

	query := `SELECT layer_id, offset_value, data, layer_range, file_range FROM entries ORDER BY layer_id, offset_value ASC;`
	rows, err := ms.db.Query(query)
	if err != nil {
		Logger.Error("Failed to query entries", "error", err)
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
			Logger.Error("Error scanning entry row", "error", err)
			return nil, err
		}

		layerRange := [2]uint64{0, 0}
		if layerRangeStr.Valid {
			parts := strings.Split(strings.Trim(layerRangeStr.String, "[)"), ",")
			if len(parts) == 2 {
				start, err := strconv.ParseUint(parts[0], 10, 8)
				if err != nil {
					Logger.Error("Error parsing layer range start", "value", parts[0], "error", err)
					return nil, err
				}
				end, err := strconv.ParseUint(parts[1], 10, 8)
				if err != nil {
					Logger.Error("Error parsing layer range end", "value", parts[1], "error", err)
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
					Logger.Error("Error parsing file range start", "value", parts[0], "error", err)
					return nil, err
				}
				end, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					Logger.Error("Error parsing file range end", "value", parts[1], "error", err)
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
		Logger.Debug("Loaded entry", "layerID", layerID, "offset", offset, "dataSize", len(data))
	}

	layerCount := len(result)
	Logger.Debug("Entries loaded successfully",
		"totalEntries", entriesCount,
		"layerCount", layerCount,
		"totalDataSize", totalDataSize)

	return result, nil
}

// LoadActiveLayer loads only the active (unsealed) layer and its entries.
// Returns the active layer or nil if no active layer is found.
func (ms *MetadataStore) LoadActiveLayer() (*Layer, error) {
	Logger.Debug("Loading active (unsealed) layer from metadata store")

	// Use a single query with JOIN to get the active layer and its entries
	query := `
		SELECT l.id, l.file_id, e.offset_value, e.data, e.layer_range, e.file_range 
		FROM layers l
		LEFT JOIN entries e ON l.id = e.layer_id
		WHERE l.sealed = 0
		ORDER BY l.id DESC
		LIMIT 1;
	`

	rows, err := ms.db.Query(query)
	if err != nil {
		Logger.Error("Failed to query active layer and entries", "error", err)
		return nil, err
	}
	defer rows.Close()

	// Check if we found any rows
	if !rows.Next() {
		Logger.Debug("No active layer found")
		return nil, nil
	}

	// Create the layer
	var fileID int
	rows.Scan(&fileID)
	layer := NewLayer(fileID)

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
			Logger.Error("Error scanning row", "error", err)
			return nil, err
		}

		// Set or verify the layer ID
		if layerID == 0 {
			layerID = id
			layer.ID = id
			Logger.Debug("Found active layer", "layerID", id)
		} else if id != layerID {
			// This shouldn't happen with our query, but check anyway
			Logger.Warn("Unexpected layer ID in results", "expected", layerID, "got", id)
		}

		// Add entry data if present
		if offset.Valid && data != nil {
			offsetValue := uint64(offset.Int64)
			layer.AddEntry(offsetValue, data)
			entriesCount++
			totalDataSize += len(data)
			Logger.Debug("Loaded entry", "layerID", id, "offset", offsetValue, "dataSize", len(data))
		}

		// Move to next row or exit loop if done
		if !rows.Next() {
			break
		}
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		Logger.Error("Error iterating over rows", "error", err)
		return nil, err
	}

	Logger.Debug("Active layer loaded successfully",
		"layerID", layerID,
		"entriesCount", entriesCount,
		"totalDataSize", totalDataSize)

	return layer, nil
}

// Close closes the database.
func (ms *MetadataStore) Close() error {
	Logger.Debug("Closing metadata store database connection")
	err := ms.db.Close()
	if err != nil {
		Logger.Error("Error closing database connection", "error", err)
	} else {
		Logger.Debug("Database connection closed successfully")
	}
	return err
}

// CalculateVirtualFileSize calculates the total byte size of the virtual file from all layers and their entries, respecting layer creation order and handling overlapping file ranges.
func (ms *MetadataStore) CalculateVirtualFileSize(fileID int) (uint64, error) {
	Logger.Debug("Calculating virtual file size from metadata store", "fileID", fileID)

	query := `
		SELECT e.file_range
		FROM entries e
		JOIN layers l ON e.layer_id = l.id
		WHERE l.file_id = $1
		ORDER BY l.created_at ASC, lower(e.file_range) ASC;
	`
	rows, err := ms.db.Query(query, fileID)
	if err != nil {
		Logger.Error("Failed to query file ranges", "error", err, "fileID", fileID)
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
			Logger.Error("Error scanning file range row", "error", err)
			return 0, err
		}

		if fileRangeStr.Valid {
			parts := strings.Split(strings.Trim(fileRangeStr.String, "[)"), ",")
			if len(parts) == 2 {
				start, err := strconv.ParseUint(parts[0], 10, 64)
				if err != nil {
					Logger.Error("Error parsing file range start", "value", parts[0], "error", err)
					return 0, err
				}
				end, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					Logger.Error("Error parsing file range end", "value", parts[1], "error", err)
					return 0, err
				}
				ranges = append(ranges, Range{start: start, end: end})
			}
		}
	}

	if err = rows.Err(); err != nil {
		Logger.Error("Error iterating over file range rows", "error", err)
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

	Logger.Debug("Virtual file size calculated successfully", "totalSize", totalSize)
	return totalSize, nil
}

// LoadLayersByFileID loads all layers associated with a specific file ID from the database.
func (ms *MetadataStore) LoadLayersByFileID(fileID int) ([]*Layer, error) {
	Logger.Debug("Loading layers for file from metadata store", "fileID", fileID)

	query := `SELECT id, file_id, sealed FROM layers WHERE file_id = $1 ORDER BY id ASC;`
	rows, err := ms.db.Query(query, fileID)
	if err != nil {
		Logger.Error("Failed to query layers for file", "fileID", fileID, "error", err)
		return nil, err
	}
	defer rows.Close()

	var layers []*Layer
	layerCount := 0

	for rows.Next() {
		var id, sealedInt int
		if err := rows.Scan(&id, &fileID, &sealedInt); err != nil {
			Logger.Error("Error scanning layer row", "error", err)
			return nil, err
		}

		layer := NewLayer(fileID)
		layer.ID = id
		layers = append(layers, layer)

		sealed := sealedInt != 0
		Logger.Debug("Loaded layer for file", "layerID", id, "sealed", sealed)
		layerCount++
	}

	Logger.Debug("Layers for file loaded successfully", "fileID", fileID, "count", layerCount)
	return layers, nil
}

// DeleteFile removes a file and its associated layers and entries from the database within a transaction.
func (ms *MetadataStore) DeleteFile(name string) error {
	Logger.Debug("Deleting file and its associated data from metadata store", "name", name)

	// Begin a transaction
	tx, err := ms.db.Begin()
	if err != nil {
		Logger.Error("Failed to begin transaction", "error", err)
		return err
	}

	// Ensure the transaction is rolled back in case of an error
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			Logger.Error("Transaction failed, rolling back", "error", err)
			tx.Rollback()
		} else {
			err = tx.Commit()
			if err != nil {
				Logger.Error("Failed to commit transaction", "error", err)
			}
		}
	}()

	// Retrieve the file ID
	fileID, err := ms.GetFileIDByName(name)
	if err != nil {
		Logger.Error("Failed to retrieve file ID", "name", name, "error", err)
		return err
	}

	if fileID == 0 {
		Logger.Warn("File not found, nothing to delete", "name", name)
		return nil
	}

	// Delete all entries associated with the file's layers
	deleteEntriesQuery := `DELETE FROM entries WHERE layer_id IN (SELECT id FROM layers WHERE file_id = $1);`
	_, err = tx.Exec(deleteEntriesQuery, fileID)
	if err != nil {
		Logger.Error("Failed to delete entries for file", "name", name, "error", err)
		return err
	}

	// Delete all layers associated with the file
	deleteLayersQuery := `DELETE FROM layers WHERE file_id = $1;`
	_, err = tx.Exec(deleteLayersQuery, fileID)
	if err != nil {
		Logger.Error("Failed to delete layers for file", "name", name, "error", err)
		return err
	}

	// Delete the file itself
	deleteFileQuery := `DELETE FROM files WHERE id = $1;`
	_, err = tx.Exec(deleteFileQuery, fileID)
	if err != nil {
		Logger.Error("Failed to delete file", "name", name, "error", err)
		return err
	}

	Logger.Info("File and its associated data deleted successfully", "name", name)
	return nil
}
