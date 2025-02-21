package main

import (
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// MetadataStore manages persistent layer metadata using SQLite.
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
	LayerID int
	Offset  int64
	Data    []byte
}

// NewMetadataStore opens (or creates) a SQLite database at dbPath and initializes the tables.
func NewMetadataStore(dbPath string) (*MetadataStore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	ms := &MetadataStore{db: db}
	if err := ms.init(); err != nil {
		return nil, err
	}
	return ms, nil
}

// init creates the necessary tables if they do not exist.
func (ms *MetadataStore) init() error {
	// Create layers table.
	layerTable := `
	CREATE TABLE IF NOT EXISTS layers (
		id INTEGER PRIMARY KEY,
		base INTEGER NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		sealed INTEGER DEFAULT 0
	);
	`
	// Create entries table.
	entryTable := `
	CREATE TABLE IF NOT EXISTS entries (
		layer_id INTEGER NOT NULL,
		offset INTEGER NOT NULL,
		data BLOB NOT NULL,
		PRIMARY KEY (layer_id, offset)
	);
	`
	if _, err := ms.db.Exec(layerTable); err != nil {
		return err
	}
	if _, err := ms.db.Exec(entryTable); err != nil {
		return err
	}
	return nil
}

// RecordNewLayer inserts a new layer record.
func (ms *MetadataStore) RecordNewLayer(layer *Layer) error {
	query := `INSERT INTO layers (id, base, sealed) VALUES (?, ?, 0);`
	_, err := ms.db.Exec(query, layer.ID, layer.base)
	return err
}

// SealLayer marks the layer with the given id as sealed.
func (ms *MetadataStore) SealLayer(layerID int) error {
	query := `UPDATE layers SET sealed = 1 WHERE id = ?;`
	_, err := ms.db.Exec(query, layerID)
	return err
}

// RecordEntry inserts a new entry record.
func (ms *MetadataStore) RecordEntry(layerID int, offset int64, data []byte) error {
	query := `INSERT INTO entries (layer_id, offset, data) VALUES (?, ?, ?);`
	_, err := ms.db.Exec(query, layerID, offset, data)
	return err
}

// LoadLayers loads all layer metadata from the database sorted by id.
// It returns a slice of layers (with only the metadata), the next available layer id,
// and the highest base value among layers.
func (ms *MetadataStore) LoadLayers() ([]*Layer, int, int64, error) {
	query := `SELECT id, base, sealed FROM layers ORDER BY id ASC;`
	rows, err := ms.db.Query(query)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()

	var layers []*Layer
	var maxID int
	var maxBase int64
	for rows.Next() {
		var id int
		var base int64
		var sealedInt int
		if err := rows.Scan(&id, &base, &sealedInt); err != nil {
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
	}
	nextID := maxID + 1
	return layers, nextID, maxBase, nil
}

// LoadEntries loads all entry records from the database and groups them by layer_id.
func (ms *MetadataStore) LoadEntries() (map[int][]EntryRecord, error) {
	query := `SELECT layer_id, offset, data FROM entries;`
	rows, err := ms.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int][]EntryRecord)
	for rows.Next() {
		var layerID int
		var offset int64
		var data []byte
		if err := rows.Scan(&layerID, &offset, &data); err != nil {
			return nil, err
		}
		result[layerID] = append(result[layerID], EntryRecord{
			LayerID: layerID,
			Offset:  offset,
			Data:    data,
		})
	}
	return result, nil
}

// Close closes the database.
func (ms *MetadataStore) Close() error {
	return ms.db.Close()
}
