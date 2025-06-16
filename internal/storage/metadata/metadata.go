package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/vinimdocarmo/quackfs/db/sqlc"
	"github.com/vinimdocarmo/quackfs/db/types"
)

// Chunk holds information about where data was written in the layer data
type Chunk struct {
	LayerID    uint64    // 0 if flushed is false
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
	ID        uint64
	FileID    uint64
	Active    bool // whether or not it is the current active layer (memory resident)
	VersionID uint64
	Tag       string
	Chunks    []Chunk
	Size      uint64
	Data      []byte
	ObjectKey string
}

type MetadataStore struct {
	queries *sqlc.Queries
}

func NewMetadataStore(db *sql.DB) *MetadataStore {
	return &MetadataStore{
		queries: sqlc.New(db),
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

func (ms *MetadataStore) GetFileIDByName(ctx context.Context, name string, opts ...QueryOpt) (uint64, error) {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var fileID uint64
	var err error

	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	fileID, err = queries.GetFileIDByName(ctx, name)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, types.ErrNotFound
		}
		return 0, err
	}

	return fileID, nil
}

func (ms *MetadataStore) InsertFile(ctx context.Context, name string) (uint64, error) {
	fileID, err := ms.queries.InsertFile(ctx, name)
	if err != nil {
		return 0, err
	}

	return fileID, nil
}

func (ms *MetadataStore) GetAllFiles(ctx context.Context) ([]sqlc.File, error) {
	return ms.queries.GetAllFiles(ctx)
}

// CalcSizeOf calculates the total byte size of the DuckDB database file
func (ms *MetadataStore) CalcSizeOf(ctx context.Context, fileID uint64, opts ...QueryOpt) (uint64, error) {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var fileSize int64
	var err error

	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	fileSize, err = queries.CalcFileSize(ctx, fileID)

	if err != nil {
		// If the file has no chunks, its size is 0
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return uint64(fileSize), nil
}

func (ms *MetadataStore) InsertChunk(ctx context.Context, layerID uint64, c Chunk, opts ...QueryOpt) error {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error

	layerRange := types.Range(c.LayerRange)
	fileRange := types.Range(c.FileRange)

	params := sqlc.InsertChunkParams{
		SnapshotLayerID: layerID,
		LayerRange:      layerRange,
		FileRange:       fileRange,
	}

	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	err = queries.InsertChunk(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to insert chunk: %w", err)
	}

	return nil
}

func (ms *MetadataStore) LoadLayersByFileID(ctx context.Context, fileID uint64, opts ...QueryOpt) ([]*Layer, error) {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var rows []sqlc.GetLayersByFileIDRow
	var err error

	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	rows, err = queries.GetLayersByFileID(ctx, fileID)
	if err != nil {
		return nil, err
	}

	var layers []*Layer

	for _, row := range rows {
		layer := &Layer{FileID: row.FileID}
		layer.ID = row.ID
		if row.VersionID.Valid {
			layer.VersionID = uint64(row.VersionID.Int64)
		} else {
			layer.VersionID = 0
		}
		if row.Tag.Valid {
			layer.Tag = row.Tag.String
		}
		layer.ObjectKey = row.ObjectKey
		layers = append(layers, layer)
	}

	return layers, nil
}

func (ms *MetadataStore) InsertVersion(ctx context.Context, tx *sql.Tx, version string) (uint64, error) {
	queries := ms.queries.WithTx(tx)
	versionID, err := queries.InsertVersion(ctx, version)
	if err != nil {
		return 0, fmt.Errorf("failed to insert new version: %w", err)
	}
	return versionID, nil
}

func (ms *MetadataStore) InsertLayer(ctx context.Context, tx *sql.Tx, fileID uint64, versionID uint64, objectKey string) (uint64, error) {
	params := sqlc.InsertLayerParams{
		FileID:    fileID,
		VersionID: sql.NullInt64{Int64: int64(versionID), Valid: true},
		ObjectKey: objectKey,
	}

	layerID, err := ms.queries.WithTx(tx).InsertLayer(ctx, params)
	if err != nil {
		return 0, fmt.Errorf("failed to insert layer: %w", err)
	}
	return layerID, nil
}

func (ms *MetadataStore) GetObjectKey(ctx context.Context, layerID uint64) (string, error) {
	objectKey, err := ms.queries.GetObjectKey(ctx, layerID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("error retrieving object key: %w", err)
	}
	return objectKey, nil
}

func (ms *MetadataStore) GetLayerByVersion(ctx context.Context, fileID uint64, versionTag string, tx *sql.Tx) (*Layer, error) {
	params := sqlc.GetLayerByVersionParams{
		FileID: fileID,
		Tag:    versionTag,
	}

	row, err := ms.queries.WithTx(tx).GetLayerByVersion(ctx, params)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("version tag not found")
		}
		return nil, fmt.Errorf("failed to fetch layer: %w", err)
	}

	layer := &Layer{
		ID:     row.ID,
		FileID: row.FileID,
	}

	// Set optional fields
	if row.VersionID.Valid {
		layer.VersionID = uint64(row.VersionID.Int64)
	}
	layer.Tag = row.Tag
	layer.ObjectKey = row.ObjectKey

	// Load the chunk metadata for this layer
	chunks, err := ms.GetLayerChunks(ctx, layer.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to load layer chunks: %w", err)
	}
	layer.Chunks = chunks

	return layer, nil
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

// Helper function to convert chunk row data into a Chunk struct
func toChunk(layerID uint64, layerRange types.Range, fileRange types.Range, flushed bool) Chunk {
	return Chunk{
		LayerID:    layerID,
		Flushed:    flushed,
		LayerRange: [2]uint64(layerRange),
		FileRange:  [2]uint64(fileRange),
	}
}

func (ms *MetadataStore) GetLayerChunks(ctx context.Context, layerID uint64) ([]Chunk, error) {
	rows, err := ms.queries.GetLayerChunks(ctx, layerID)
	if err != nil {
		return nil, err
	}

	var chunks []Chunk

	for _, row := range rows {
		chunk := toChunk(layerID, row.LayerRange, row.FileRange, true)
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

type ChunkQueryOpt func(*ChunkQueryOpts)

type ChunkQueryOpts struct {
	versionedLayerID uint64
}

// WithVersionedLayerID specifies a versioned layer ID to filter chunks by
// if 0 the value is ignored
func WithVersionedLayerID(id uint64) ChunkQueryOpt {
	return func(opts *ChunkQueryOpts) {
		opts.versionedLayerID = id
	}
}

// getOverlappingChunks retrieves chunks that overlap with a specific range for a file
func (ms *MetadataStore) getOverlappingChunks(ctx context.Context, tx *sql.Tx, fileID uint64, offsetRange [2]uint64, opts ...ChunkQueryOpt) ([]Chunk, error) {
	options := ChunkQueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	queries := ms.queries.WithTx(tx)

	var chunks []Chunk

	params := sqlc.GetOverlappingChunksWithVersionParams{
		VersionedLayerID: options.versionedLayerID,
		FileID:           fileID,
		Range:            types.Range(offsetRange),
	}
	rows, err := queries.GetOverlappingChunksWithVersion(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunks with version: %w", err)
	}

	for _, row := range rows {
		chunk := toChunk(row.SnapshotLayerID, row.LayerRange, row.FileRange, true)
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func (ms *MetadataStore) GetAllOverlappingChunks(ctx context.Context, tx *sql.Tx, fileID uint64, offsetRange [2]uint64, activeLayer *Layer, opts ...ChunkQueryOpt) ([]Chunk, error) {
	options := ChunkQueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	chunks, err := ms.getOverlappingChunks(ctx, tx, fileID, offsetRange, opts...)
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

// SetHead sets the head pointer for a file to a specific version
func (ms *MetadataStore) SetHead(ctx context.Context, fileID uint64, versionID uint64, opts ...QueryOpt) error {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	err = queries.SetHead(ctx, sqlc.SetHeadParams{
		FileID:    fileID,
		VersionID: versionID,
	})
	if err != nil {
		return fmt.Errorf("failed to set head: %w", err)
	}
	return nil
}

// GetHeadVersion gets the current version the file head is pointing to
func (ms *MetadataStore) GetHeadVersion(ctx context.Context, fileID uint64, opts ...QueryOpt) (uint64, string, error) {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var version sqlc.GetHeadVersionRow
	var err error

	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	version, err = queries.GetHeadVersion(ctx, fileID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, "", types.ErrNotFound
		}
		return 0, "", err
	}
	return version.VersionID, version.VersionTag, nil
}

// DeleteHead removes the head pointer for a file
func (ms *MetadataStore) DeleteHead(ctx context.Context, fileID uint64, opts ...QueryOpt) error {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var err error
	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	err = queries.DeleteHead(ctx, fileID)
	if err != nil {
		return fmt.Errorf("failed to delete head: %w", err)
	}
	return nil
}

// GetAllHeads returns all head pointers
func (ms *MetadataStore) GetAllHeads(ctx context.Context) ([]sqlc.GetAllHeadsRow, error) {
	rows, err := ms.queries.GetAllHeads(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all heads: %w", err)
	}
	return rows, nil
}

// GetFileVersions returns all versions for a specific file ID
func (ms *MetadataStore) GetFileVersions(ctx context.Context, fileID uint64, opts ...QueryOpt) ([]sqlc.Version, error) {
	options := QueryOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	var versions []sqlc.Version
	var err error

	queries := ms.queries

	if options.tx != nil {
		queries = ms.queries.WithTx(options.tx)
	}

	versions, err = queries.GetFileVersions(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to get file versions: %w", err)
	}

	return versions, nil
}
