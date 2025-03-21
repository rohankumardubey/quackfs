package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/charmbracelet/log"
	"github.com/vinimdocarmo/quackfs/src/internal/storage/metadata"
)

// Use ErrNotFound from metadata package
var ErrNotFound = metadata.ErrNotFound

type objectStore interface {
	// PutObject uploads data to the object store.
	PutObject(ctx context.Context, key string, data []byte) error
	// GetObject returns a slice of data from the given offset up to size bytes.
	// Range is inclusive of the start and the end (i.e. [start, end])
	GetObject(ctx context.Context, key string, dataRange [2]uint64) ([]byte, error)
}

type Manager struct {
	db          *sql.DB
	log         *log.Logger
	mu          sync.RWMutex              // Add a mutex to protect memtable
	memtable    map[int64]*metadata.Layer // Stores a mapping of file ids to their active layer
	objectStore objectStore
	metaStore   *metadata.MetadataStore
}

// NewManager creates (or reloads) a StorageManager using the provided metadataStore.
func NewManager(db *sql.DB, store objectStore, log *log.Logger) *Manager {
	managerLog := log.With()
	managerLog.SetPrefix("💽 storage")

	sm := &Manager{
		db:          db,
		log:         managerLog,
		memtable:    make(map[int64]*metadata.Layer),
		objectStore: store,
		metaStore:   metadata.NewMetadataStore(db),
	}

	return sm
}

// WriteFile writes data to the active layer at the specified offset.
func (mgr *Manager) WriteFile(ctx context.Context, filename string, data []byte, offset uint64) error {
	mgr.mu.Lock()         // Lock before accessing activeLayers
	defer mgr.mu.Unlock() // Ensure unlock when function returns

	mgr.log.Debug("Writing data", "filename", filename, "size", len(data), "offset", offset)

	// Get the file ID from the file name
	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename)
	if err != nil {
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	activeLayer, exists := mgr.memtable[fileID]
	if !exists {
		activeLayer = &metadata.Layer{
			FileID: fileID,
			Chunks: []metadata.Chunk{},
			Data:   []byte{},
			Active: true,
		}
		mgr.memtable[fileID] = activeLayer
	}

	fileSize, err := mgr.calcSizeOf(ctx, fileID)
	if err != nil {
		mgr.log.Error("Failed to calculate size of file", "error", err)
		return fmt.Errorf("failed to calculate size of file: %w", err)
	}

	if offset > fileSize {
		// Calculate how many zero bytes to add
		bytesToAdd := offset - fileSize

		// Create a buffer of zero bytes
		zeroes := make([]byte, bytesToAdd)

		var layerSize uint64 = 0
		if len(activeLayer.Chunks) > 0 {
			layerSize = activeLayer.Chunks[len(activeLayer.Chunks)-1].FileRange[1]
		}

		layerRange := [2]uint64{layerSize, layerSize + bytesToAdd}
		fileRange := [2]uint64{fileSize, fileSize + bytesToAdd}

		activeLayer.Data = append(activeLayer.Data, zeroes...)
		activeLayer.Chunks = append(activeLayer.Chunks, metadata.Chunk{
			LayerRange: layerRange,
			FileRange:  fileRange,
			Flushed:    false, // since we're writing to the active layer, it's not flushed yet
		})
		activeLayer.Size = layerRange[1]
	}

	var layerSize uint64 = 0
	if len(activeLayer.Chunks) > 0 {
		layerSize = activeLayer.Chunks[len(activeLayer.Chunks)-1].LayerRange[1]
	}

	layerRange := [2]uint64{layerSize, layerSize + uint64(len(data))}
	fileRange := [2]uint64{offset, offset + uint64(len(data))}

	activeLayer.Data = append(activeLayer.Data, data...)
	activeLayer.Chunks = append(activeLayer.Chunks, metadata.Chunk{
		LayerRange: layerRange,
		FileRange:  fileRange,
		Flushed:    false, // since we're writing to the active layer, it's not flushed yet
	})
	activeLayer.Size = layerRange[1]

	return nil
}

func (mgr *Manager) GetActiveLayerSize(ctx context.Context, fileID int64) uint64 {
	mgr.mu.RLock() // Read lock is sufficient for reading
	defer mgr.mu.RUnlock()

	activeLayer, exists := mgr.memtable[fileID]
	if !exists {
		return 0
	}
	return activeLayer.Size
}

func (mgr *Manager) GetActiveLayerData(ctx context.Context, fileID int64) []byte {
	mgr.mu.RLock() // Read lock is sufficient for reading
	defer mgr.mu.RUnlock()

	l, exists := mgr.memtable[fileID]
	if !exists {
		return nil
	}

	return l.Data
}

func (mgr *Manager) SizeOf(ctx context.Context, filename string) (uint64, error) {
	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename)
	if err != nil {
		return 0, err
	}

	return mgr.calcSizeOf(ctx, fileID)
}

// readFileOpt defines functional options for GetDataRange
type readFileOpt func(*readFileOpts)

// readFileOpts holds all options for GetDataRange
type readFileOpts struct {
	version string
}

// WithVersion specifies a version tag to retrieve data up to
// If no version is specified, the current database state is used
func WithVersion(v string) readFileOpt {
	return func(opts *readFileOpts) {
		opts.version = v
	}
}

// ReadFile returns a slice of data from the given offset up to size bytes.
// Optional version tag can be specified to retrieve data up to a specific version.
func (mgr *Manager) ReadFile(ctx context.Context, filename string, offset uint64, size uint64, opts ...readFileOpt) ([]byte, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	options := readFileOpts{}
	for _, opt := range opts {
		opt(&options)
	}

	hasVersion := options.version != ""
	var versionedLayerId int64

	if hasVersion {
		mgr.log.Debug("reading file",
			"filename", filename,
			"offset", offset,
			"size", size,
			"version", options.version)
	} else {
		mgr.log.Debug("reading file",
			"filename", filename,
			"offset", offset,
			"size", size)
	}

	tx, err := mgr.db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		mgr.log.Error("Failed to begin transaction", "error", err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				mgr.log.Error("Failed to rollback transaction after panic", "error", rbErr)
			}
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				mgr.log.Error("Failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename, metadata.WithTx(tx))
	if fileID == 0 {
		mgr.log.Error("File not found", "filename", filename)
		return nil, fmt.Errorf("file not found")
	}
	if err != nil {
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return nil, fmt.Errorf("failed to get file ID: %w", err)
	}

	// check if there's a layer for this file with the given version tag
	if hasVersion {
		versionedLayer, err := mgr.metaStore.GetLayerByVersion(ctx, fileID, options.version, tx)
		if err != nil {
			mgr.log.Error("Version tag not found or error fetching layer", "version", options.version, "filename", filename, "error", err)
			return nil, err
		}
		versionedLayerId = versionedLayer.ID
	}

	activeLayer, exists := mgr.memtable[fileID]
	var activeLayerPtr *metadata.Layer
	if exists {
		activeLayerPtr = activeLayer
	}

	chunks, err := mgr.metaStore.GetAllOverlappingChunks(ctx, tx, fileID, [2]uint64{offset, offset + size},
		activeLayerPtr, metadata.WithVersionedLayerID(versionedLayerId))
	if err != nil {
		mgr.log.Error("Failed to get overlapping chunks", "error", err)
		return nil, err
	}

	var maxEndOffset uint64
	for _, chunk := range chunks {
		if chunk.FileRange[1] > maxEndOffset {
			maxEndOffset = chunk.FileRange[1]
		}
	}

	buf := make([]byte, maxEndOffset-offset)

	for _, chunk := range chunks {
		var bufferPos uint64
		var chunkStartPos uint64
		var dataSize uint64
		var data []byte

		// The layer for this chunk hasn't been flushed to storage yet. It's in the active layer.
		if !chunk.Flushed {
			data = activeLayer.Data[chunk.LayerRange[0]:chunk.LayerRange[1]]
		} else {
			data, err = mgr.getChunkData(ctx, mgr.metaStore, mgr.objectStore, chunk)
			if err != nil {
				mgr.log.Error("Failed to get chunk data", "error", err)
				return nil, fmt.Errorf("failed to get chunk data: %w", err)
			}
		}

		if chunk.FileRange[0] < offset {
			// Chunk starts before the requested offset
			// We only want to copy the portion starting from the requested offset
			chunkStartPos = offset - chunk.FileRange[0]
			bufferPos = 0

			dataSize = uint64(len(data)) - chunkStartPos
		} else {
			bufferPos = chunk.FileRange[0] - offset
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

	if uint64(len(buf)) > size {
		buf = buf[:size]
	}

	if err = tx.Commit(); err != nil {
		mgr.log.Error("Failed to commit transaction", "error", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if hasVersion {
		mgr.log.Debug("Returning data range with version",
			"offset", offset,
			"size", len(buf),
			"version", options.version)
	} else {
		mgr.log.Debug("Returning data range",
			"offset", offset,
			"size", len(buf))
	}

	return buf, nil
}

// InsertFile inserts a new file into the files table and returns its ID.
func (mgr *Manager) InsertFile(ctx context.Context, name string) (int64, error) {
	mgr.log.Debug("Inserting new file into metadata store", "name", name)

	fileID, err := mgr.metaStore.InsertFile(ctx, name)
	if err != nil {
		mgr.log.Error("Failed to insert new file", "name", name, "error", err)
		return 0, err
	}

	mgr.log.Debug("File inserted successfully", "name", name, "fileID", fileID)
	return fileID, nil
}

func (mgr *Manager) calcSizeOf(ctx context.Context, fileID int64, opts ...metadata.QueryOpt) (uint64, error) {
	activeLayer, exists := mgr.memtable[fileID]
	if exists && activeLayer != nil && len(activeLayer.Chunks) > 0 {
		endOffset := uint64(0)
		for _, chunk := range activeLayer.Chunks {
			if chunk.FileRange[1] > endOffset {
				endOffset = chunk.FileRange[1]
			}
		}
		return endOffset, nil
	}

	highestOffsetCommited, err := mgr.metaStore.CalcSizeOf(ctx, fileID, opts...)
	if err != nil {
		return 0, err
	}

	var highestOffsetInActiveLayer uint64
	if exists && activeLayer != nil {
		for _, chunk := range activeLayer.Chunks {
			if chunk.FileRange[1] > highestOffsetInActiveLayer {
				highestOffsetInActiveLayer = chunk.FileRange[1]
			}
		}
	}

	return max(highestOffsetCommited, highestOffsetInActiveLayer), nil
}

// Checkpoint persists the active layer to storage and creates a new version
func (mgr *Manager) Checkpoint(ctx context.Context, filename string, version string) error {
	mgr.mu.Lock()         // Lock before accessing activeLayers
	defer mgr.mu.Unlock() // Ensure unlock when function returns

	tx, err := mgr.db.BeginTx(ctx, nil)
	if err != nil {
		mgr.log.Error("Failed to begin transaction", "error", err)
		return err
	}

	// Setup deferred rollback in case of error or panic
	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				mgr.log.Error("Failed to rollback transaction after panic", "error", rbErr)
			}
			// Re-panic after rollback
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				mgr.log.Error("Failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename, metadata.WithTx(tx))
	if err != nil {
		if err == metadata.ErrNotFound {
			mgr.log.Warn("File not found, nothing to checkpoint", "filename", filename)
			return nil
		}
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	activeLayer, exists := mgr.memtable[fileID]
	if !exists || len(activeLayer.Data) == 0 {
		mgr.log.Warn("No active layer or data to checkpoint", "filename", filename)
		return nil // No active layer means no changes to checkpoint
	}

	versionID, err := mgr.metaStore.InsertVersion(ctx, tx, version)
	if err != nil {
		mgr.log.Error("Failed to insert new version", "tag", version, "error", err)
		return fmt.Errorf("failed to insert new version: %w", err)
	}

	objectKey := fmt.Sprintf("layers/%s/%d-%d", filename, fileID, versionID)

	err = mgr.objectStore.PutObject(ctx, objectKey, activeLayer.Data)
	if err != nil {
		mgr.log.Error("Failed to upload data to object store", "error", err)
		return fmt.Errorf("failed to upload data to object store: %w", err)
	}

	layerID, err := mgr.metaStore.InsertLayer(ctx, tx, fileID, versionID, objectKey)
	if err != nil {
		mgr.log.Error("Failed to commit layer with version", "error", err)
		return fmt.Errorf("failed to commit layer with version: %w", err)
	}

	for _, c := range activeLayer.Chunks {
		err = mgr.metaStore.InsertChunk(ctx, layerID, c, metadata.WithTx(tx))
		if err != nil {
			mgr.log.Error("Failed to commit layer's chunks", "error", err)
			return fmt.Errorf("failed to commit layer's chunks: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		mgr.log.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	delete(mgr.memtable, fileID)

	mgr.log.Debug("Checkpoint successful", "layerID", layerID, "objectKey", objectKey)

	return nil
}

// GetAllFiles returns a list of all files in the database
func (mgr *Manager) GetAllFiles(ctx context.Context) ([]metadata.FileInfo, error) {
	return mgr.metaStore.GetAllFiles(ctx)
}

// LoadLayersByFileID delegates to the metadata store
func (mgr *Manager) LoadLayersByFileID(ctx context.Context, fileID int64, opts ...metadata.QueryOpt) ([]*metadata.Layer, error) {
	return mgr.metaStore.LoadLayersByFileID(ctx, fileID, opts...)
}

// getChunkData retrieves chunk data from the object store using range requests
func (mgr *Manager) getChunkData(ctx context.Context, ms *metadata.MetadataStore, objectStore objectStore, c metadata.Chunk) ([]byte, error) {
	objectKey, err := ms.GetObjectKey(ctx, c.LayerID)
	if err != nil {
		return nil, fmt.Errorf("error retrieving object key: %w", err)
	}

	if objectKey == "" {
		return []byte{}, nil
	}

	layerSize := c.LayerRange[1] - c.LayerRange[0]
	data, err := objectStore.GetObject(ctx, objectKey, [2]uint64{c.LayerRange[0], c.LayerRange[1] - 1})
	if err != nil {
		return nil, fmt.Errorf("error retrieving data from object store: %w", err)
	}

	if uint64(len(data)) != layerSize {
		return nil, fmt.Errorf("received incorrect number of bytes from object store: got %d, expected %d", len(data), layerSize)
	}

	return data, nil
}

// close closes the database.
func (mgr *Manager) Close() error {
	mgr.log.Debug("Closing metadata store database connection")
	err := mgr.db.Close()
	if err != nil {
		mgr.log.Error("Error closing database connection", "error", err)
	} else {
		mgr.log.Debug("Database connection closed successfully")
	}
	return err
}
