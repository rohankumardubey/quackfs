package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrNotFound = errors.New("not found")

// FileInfo represents basic file information
type FileInfo struct {
	ID   int
	Name string
}

// Chunk holds information about where data was written in the layer data
type Chunk struct {
	LayerID    int64     // 0 if flushed is false
	Flushed    bool      // whether the chunk metadata has been persisted to the database
	LayerRange [2]uint64 // Range within a layer as an array of two integers
	FileRange  [2]uint64 // Range within the virtual file as an array of two integers
}

// Layer represents a snapshot layer.
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
type Layer struct {
	ID        int64
	FileID    int64
	Active    bool // whether or not it is the current active layer (memory resident)
	VersionID int64
	Tag       string
	Chunks    []Chunk
	Size      uint64
	Data      []byte
	ObjectKey string
}

type MetadataStore struct {
	db *sql.DB
}

func NewMetadataStore(db *sql.DB) *MetadataStore {
	return &MetadataStore{
		db: db,
	}
}

type QueryOpt func(*QueryOpts)

type QueryOpts struct {
	tx *sql.Tx
}

func WithTx(tx *sql.Tx) QueryOpt {
	return func(opts *QueryOpts) {
		opts.tx = tx
	}
}

func (ms *MetadataStore) GetFileIDByName(ctx context.Context, name string, opts ...QueryOpt) (int64, error) {
	query := `SELECT id FROM files WHERE name = $1;`
	var fileID int64

	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error

	if options.tx != nil {
		err = options.tx.QueryRowContext(ctx, query, name).Scan(&fileID)
	} else {
		err = ms.db.QueryRowContext(ctx, query, name).Scan(&fileID)
	}

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, ErrNotFound
		}
		return 0, err
	}

	return fileID, nil
}

func (ms *MetadataStore) InsertFile(ctx context.Context, name string) (int64, error) {
	query := `INSERT INTO files (name) VALUES ($1) RETURNING id;`
	var fileID int64
	err := ms.db.QueryRowContext(ctx, query, name).Scan(&fileID)
	if err != nil {
		return 0, err
	}

	return fileID, nil
}

func (ms *MetadataStore) GetAllFiles(ctx context.Context) ([]FileInfo, error) {
	query := `SELECT id, name FROM files;`
	rows, err := ms.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []FileInfo
	for rows.Next() {
		var file FileInfo
		if err := rows.Scan(&file.ID, &file.Name); err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
}

// CalcSizeOf calculates the total byte size of the DuckDB database file
func (ms *MetadataStore) CalcSizeOf(ctx context.Context, fileID int64, opts ...QueryOpt) (uint64, error) {
	query := `
		SELECT upper(e.file_range)
		FROM chunks e
			INNER JOIN snapshot_layers l ON e.snapshot_layer_id = l.id
		WHERE l.file_id = $1
		ORDER BY upper(e.file_range) DESC
		LIMIT 1;
	`
	var highestOffset uint64

	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	if options.tx != nil {
		err = options.tx.QueryRowContext(ctx, query, fileID).Scan(&highestOffset)
	} else {
		err = ms.db.QueryRowContext(ctx, query, fileID).Scan(&highestOffset)
	}

	if err != nil {
		// If the file has no chunks, its size is 0
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return highestOffset, nil
}

func (ms *MetadataStore) InsertChunk(ctx context.Context, layerID int64, c Chunk, opts ...QueryOpt) error {
	layerRangeStr := fmt.Sprintf("[%d,%d)", c.LayerRange[0], c.LayerRange[1])
	fileRangeStr := fmt.Sprintf("[%d,%d)", c.FileRange[0], c.FileRange[1])

	query := `INSERT INTO chunks (snapshot_layer_id, layer_range, file_range) 
	         VALUES ($1, $2, $3);`

	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	if options.tx != nil {
		_, err = options.tx.ExecContext(ctx, query, layerID, layerRangeStr, fileRangeStr)
	} else {
		_, err = ms.db.ExecContext(ctx, query, layerID, layerRangeStr, fileRangeStr)
	}

	if err != nil {
		return fmt.Errorf("failed to insert chunk: %w", err)
	}

	return nil
}

func (ms *MetadataStore) LoadLayersByFileID(ctx context.Context, fileID int64, opts ...QueryOpt) ([]*Layer, error) {
	query := `
		SELECT snapshot_layers.id, file_id, version_id, tag, object_key
		FROM snapshot_layers
		LEFT JOIN versions ON snapshot_layers.version_id = versions.id
		WHERE file_id = $1 ORDER BY id ASC;
	`

	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var rows *sql.Rows
	var err error

	if options.tx != nil {
		rows, err = options.tx.QueryContext(ctx, query, fileID)
	} else {
		rows, err = ms.db.QueryContext(ctx, query, fileID)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var layers []*Layer

	for rows.Next() {
		var id int64
		var versionID sql.NullInt64
		var tag sql.NullString
		var objectKey sql.NullString
		if err := rows.Scan(&id, &fileID, &versionID, &tag, &objectKey); err != nil {
			return nil, err
		}

		layer := &Layer{FileID: fileID}
		layer.ID = id
		if versionID.Valid {
			layer.VersionID = versionID.Int64
		} else {
			layer.VersionID = 0
		}
		if tag.Valid {
			layer.Tag = tag.String
		}
		if objectKey.Valid {
			layer.ObjectKey = objectKey.String
		}
		layers = append(layers, layer)
	}

	return layers, nil
}

func (ms *MetadataStore) InsertVersion(ctx context.Context, tx *sql.Tx, version string) (int64, error) {
	insertVersionQ := `INSERT INTO versions (tag) VALUES ($1) RETURNING id;`
	var versionID int64
	err := tx.QueryRowContext(ctx, insertVersionQ, version).Scan(&versionID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert new version: %w", err)
	}
	return versionID, nil
}

func (ms *MetadataStore) InsertLayer(ctx context.Context, tx *sql.Tx, fileID int64, versionID int64, objectKey string) (int64, error) {
	insertLayerQ := `INSERT INTO snapshot_layers (file_id, version_id, object_key) VALUES ($1, $2, $3) RETURNING id;`
	var layerID int64
	err := tx.QueryRowContext(ctx, insertLayerQ, fileID, versionID, objectKey).Scan(&layerID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert layer: %w", err)
	}
	return layerID, nil
}

func (ms *MetadataStore) GetObjectKey(ctx context.Context, layerID int64) (string, error) {
	var objectKey string
	err := ms.db.QueryRowContext(ctx, `
		SELECT object_key
		FROM snapshot_layers
		WHERE id = $1
	`, layerID).Scan(&objectKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("error retrieving object key: %w", err)
	}
	return objectKey, nil
}

func (ms *MetadataStore) GetLayerByVersion(ctx context.Context, fileID int64, versionTag string, tx *sql.Tx) (*Layer, error) {
	query := `
		SELECT snapshot_layers.id, snapshot_layers.file_id, snapshot_layers.version_id, 
			   versions.tag, snapshot_layers.object_key
		FROM snapshot_layers
		INNER JOIN versions ON versions.id = snapshot_layers.version_id
		WHERE snapshot_layers.file_id = $1 AND versions.tag = $2
	`

	var layer Layer
	var versionID sql.NullInt64
	var tag sql.NullString
	var objectKey sql.NullString

	err := tx.QueryRowContext(ctx, query, fileID, versionTag).Scan(
		&layer.ID,
		&layer.FileID,
		&versionID,
		&tag,
		&objectKey,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("version tag not found")
		}
		return nil, fmt.Errorf("failed to fetch layer: %w", err)
	}

	// Set optional fields
	if versionID.Valid {
		layer.VersionID = versionID.Int64
	}
	if tag.Valid {
		layer.Tag = tag.String
	}
	if objectKey.Valid {
		layer.ObjectKey = objectKey.String
	}

	// Load the chunk metadata for this layer
	chunks, err := ms.GetLayerChunks(ctx, layer.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to load layer chunks: %w", err)
	}
	layer.Chunks = chunks

	return &layer, nil
}

// ParseRange parses strings of the form "[start, end)" into two uint64 values
func ParseRange(rg string) (uint64, uint64, error) {
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

// RangesOverlap checks if two ranges [start1, end1) and [start2, end2) overlap
func RangesOverlap(range1 [2]uint64, range2 [2]uint64) bool {
	return range1[0] < range2[1] && range2[0] < range1[1]
}

func (ms *MetadataStore) GetLayerChunks(ctx context.Context, layerID int64) ([]Chunk, error) {
	query := `
		SELECT layer_range, file_range
		FROM chunks
		WHERE snapshot_layer_id = $1
		ORDER BY id ASC;
	`

	rows, err := ms.db.QueryContext(ctx, query, layerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []Chunk

	for rows.Next() {
		var layerRangeStr, fileRangeStr sql.NullString
		if err := rows.Scan(&layerRangeStr, &fileRangeStr); err != nil {
			return nil, err
		}

		layerRange := [2]uint64{0, 0}
		if layerRangeStr.Valid {
			start, end, err := ParseRange(layerRangeStr.String)
			if err != nil {
				return nil, err
			}
			layerRange[0] = start
			layerRange[1] = end
		} else {
			return nil, fmt.Errorf("invalid layer range")
		}

		fileRange := [2]uint64{0, 0}
		if fileRangeStr.Valid {
			start, end, err := ParseRange(fileRangeStr.String)
			if err != nil {
				return nil, err
			}
			fileRange[0] = start
			fileRange[1] = end
		} else {
			return nil, fmt.Errorf("invalid file range")
		}

		chunks = append(chunks, Chunk{
			LayerID:    layerID,
			Flushed:    true,
			LayerRange: layerRange,
			FileRange:  fileRange,
		})
	}

	return chunks, nil
}

type ChunkQueryOpt func(*ChunkQueryOpts)

type ChunkQueryOpts struct {
	versionedLayerID int64
}

// WithVersionedLayerID specifies a versioned layer ID to filter chunks by
// if 0 the value is ignored
func WithVersionedLayerID(id int64) ChunkQueryOpt {
	return func(opts *ChunkQueryOpts) {
		opts.versionedLayerID = id
	}
}

// GetOverlappingChunks retrieves chunks that overlap with a specific range for a file
func (ms *MetadataStore) GetOverlappingChunks(ctx context.Context, tx *sql.Tx, fileID int64, offsetRange [2]uint64, opts ...ChunkQueryOpt) ([]Chunk, error) {
	options := ChunkQueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var query string
	var rows *sql.Rows
	var err error

	if options.versionedLayerID > 0 {
		query = `
			SELECT c.snapshot_layer_id, c.layer_range, c.file_range
			FROM chunks c
			INNER JOIN snapshot_layers l ON c.snapshot_layer_id = l.id
			WHERE
				l.id <= $1 AND l.file_id = $2 AND
				c.file_range && int8range($3, $4)
			ORDER BY l.id ASC, c.id ASC;
		`
		rows, err = tx.QueryContext(ctx, query, options.versionedLayerID, fileID, offsetRange[0], offsetRange[1])
	} else {
		query = `
			SELECT c.snapshot_layer_id, c.layer_range, c.file_range
			FROM chunks c
			INNER JOIN snapshot_layers l ON c.snapshot_layer_id = l.id
			WHERE
				l.file_id = $1 AND c.file_range && int8range($2, $3)
			ORDER BY l.id ASC, c.id ASC;
		`
		rows, err = tx.QueryContext(ctx, query, fileID, offsetRange[0], offsetRange[1])
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query chunks: %w", err)
	}
	defer rows.Close()

	var chunks []Chunk

	for rows.Next() {
		var layerID int64
		var layerRangeStr, fileRangeStr sql.NullString

		if err := rows.Scan(&layerID, &layerRangeStr, &fileRangeStr); err != nil {
			return nil, fmt.Errorf("error scanning chunk row: %w", err)
		}

		layerRange := [2]uint64{0, 0}
		if layerRangeStr.Valid {
			start, end, err := ParseRange(layerRangeStr.String)
			if err != nil {
				return nil, err
			}
			layerRange[0] = start
			layerRange[1] = end
		} else {
			return nil, fmt.Errorf("invalid layer range")
		}

		fileRange := [2]uint64{0, 0}
		if fileRangeStr.Valid {
			start, end, err := ParseRange(fileRangeStr.String)
			if err != nil {
				return nil, err
			}
			fileRange[0] = start
			fileRange[1] = end
		} else {
			return nil, fmt.Errorf("invalid file range")
		}

		chunk := Chunk{
			LayerID:    layerID,
			Flushed:    true,
			LayerRange: layerRange,
			FileRange:  fileRange,
		}
		chunks = append(chunks, chunk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating chunk rows: %w", err)
	}

	return chunks, nil
}

func (ms *MetadataStore) GetAllOverlappingChunks(ctx context.Context, tx *sql.Tx, fileID int64, offsetRange [2]uint64, activeLayer *Layer, opts ...ChunkQueryOpt) ([]Chunk, error) {
	options := ChunkQueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	chunks, err := ms.GetOverlappingChunks(ctx, tx, fileID, offsetRange, opts...)
	if err != nil {
		return nil, err
	}

	hasVersion := options.versionedLayerID > 0

	if activeLayer != nil && !hasVersion {
		for _, chunk := range activeLayer.Chunks {
			if RangesOverlap(chunk.FileRange, offsetRange) {
				chunks = append(chunks, chunk)
			}
		}
	}

	return chunks, nil
}
