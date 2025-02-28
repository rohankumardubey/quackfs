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
func NewMetadataStore(connStr string) (*MetadataStore, error) {
	Logger.Debug("Creating new metadata store with connection string")

	db, err := sql.Open("postgres", connStr)
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

	// Create layers table.
	layerTable := `
	CREATE TABLE IF NOT EXISTS layers (
		id INTEGER PRIMARY KEY,
		base BIGINT NOT NULL,
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

// RecordNewLayer inserts a new layer record.
func (ms *MetadataStore) RecordNewLayer(layer *Layer) error {
	Logger.Debug("Recording new layer in metadata store", "layerID", layer.ID, "base", layer.base)

	query := `INSERT INTO layers (id, base, sealed) VALUES ($1, $2, 0);`
	_, err := ms.db.Exec(query, layer.ID, layer.base)
	if err != nil {
		Logger.Error("Failed to record new layer", "layerID", layer.ID, "error", err)
		return err
	}

	Logger.Debug("Layer recorded successfully", "layerID", layer.ID)
	return nil
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
func (ms *MetadataStore) LoadLayers() ([]*Layer, int, uint64, error) {
	Logger.Debug("Loading layers from metadata store")

	query := `SELECT id, base, sealed FROM layers ORDER BY id ASC;`
	rows, err := ms.db.Query(query)
	if err != nil {
		Logger.Error("Failed to query layers", "error", err)
		return nil, 0, 0, err
	}
	defer rows.Close()

	var layers []*Layer
	var maxID int
	var maxBase uint64
	layerCount := 0

	for rows.Next() {
		var id int
		var base uint64
		var sealedInt int
		if err := rows.Scan(&id, &base, &sealedInt); err != nil {
			Logger.Error("Error scanning layer row", "error", err)
			return nil, 0, 0, err
		}

		layer := NewLayer(id, base)
		layers = append(layers, layer)
		if id > maxID {
			maxID = id
		}
		if base > maxBase {
			maxBase = base
		}

		sealed := sealedInt != 0
		Logger.Debug("Loaded layer", "layerID", id, "base", base, "sealed", sealed)
		layerCount++
	}

	nextID := maxID + 1
	Logger.Debug("Layers loaded successfully", "count", layerCount, "nextID", nextID, "maxBase", maxBase)
	return layers, nextID, maxBase, nil
}

// LoadEntries loads all entry records from the database and groups them by layer_id.
func (ms *MetadataStore) LoadEntries() (map[int][]EntryRecord, error) {
	Logger.Debug("Loading entries from metadata store")

	query := `SELECT layer_id, offset_value, data, layer_range, file_range FROM entries;`
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
