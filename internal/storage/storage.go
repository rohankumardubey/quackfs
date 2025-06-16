package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/charmbracelet/log"
	"github.com/dustin/go-humanize"
	"github.com/vinimdocarmo/quackfs/db/sqlc"
	"github.com/vinimdocarmo/quackfs/db/types"
	"github.com/vinimdocarmo/quackfs/internal/storage/metadata"
)

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
	mu          sync.RWMutex               // Add a mutex to protect memtable
	memtable    map[uint64]*metadata.Layer // Stores a mapping of file ids to their active layer
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
		memtable:    make(map[uint64]*metadata.Layer),
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

	// Check if file has a head pointer, if so it's in read-only mode
	_, _, err = mgr.metaStore.GetHeadVersion(ctx, fileID)
	if err == nil {
		mgr.log.Error("Cannot write to file with head pointing to version", "filename", filename)
		return fmt.Errorf("cannot write to file: %s is in read-only mode because a head is set", filename)
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

	mgr.log.Debug("active layer info", "chunks", len(activeLayer.Chunks), "bytes", humanize.Bytes(layerSize))

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

func (mgr *Manager) GetActiveLayerSize(ctx context.Context, fileID uint64) uint64 {
	mgr.mu.RLock() // Read lock is sufficient for reading
	defer mgr.mu.RUnlock()

	activeLayer, exists := mgr.memtable[fileID]
	if !exists {
		return 0
	}
	return activeLayer.Size
}

func (mgr *Manager) GetActiveLayerData(ctx context.Context, fileID uint64) []byte {
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

// ReadFile returns a slice of data from the given offset up to size bytes.
// It automatically uses the head version if available, otherwise uses the latest version.
func (mgr *Manager) ReadFile(ctx context.Context, filename string, offset uint64, size uint64) ([]byte, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	mgr.log.Debug("reading file",
		"filename", filename,
		"offset", offset,
		"size", size)

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

	// Check if the file has a head pointer and use that version if available
	var versionedLayerId uint64
	headVersionId, headVersionTag, err := mgr.metaStore.GetHeadVersion(ctx, fileID, metadata.WithTx(tx))
	hasHeadVersion := headVersionId > 0

	if hasHeadVersion {
		mgr.log.Debug("using head version for file", "filename", filename, "version", headVersionTag)
		versionedLayer, err := mgr.metaStore.GetLayerByVersion(ctx, fileID, headVersionTag, tx)
		if err != nil {
			mgr.log.Error("Error fetching layer for head version", "version", headVersionTag, "filename", filename, "error", err)
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
			data, err = mgr.getChunkData(ctx, chunk)
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
		}
	}

	if uint64(len(buf)) > size {
		buf = buf[:size]
	}

	if err = tx.Commit(); err != nil {
		mgr.log.Error("Failed to commit transaction", "error", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if hasHeadVersion {
		mgr.log.Debug("Returning data range with head version",
			"offset", offset,
			"size", len(buf),
			"version", headVersionTag)
	} else {
		mgr.log.Debug("Returning data range (latest version)",
			"offset", offset,
			"size", len(buf))
	}

	return buf, nil
}

// InsertFile inserts a new file into the files table and returns its ID.
func (mgr *Manager) InsertFile(ctx context.Context, name string) (uint64, error) {
	mgr.log.Debug("Inserting new file into metadata store", "name", name)

	fileID, err := mgr.metaStore.InsertFile(ctx, name)
	if err != nil {
		mgr.log.Error("Failed to insert new file", "name", name, "error", err)
		return 0, err
	}

	mgr.log.Debug("File inserted successfully", "name", name, "fileID", fileID)
	return fileID, nil
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
func (mgr *Manager) calcSizeOf(ctx context.Context, fileID uint64, opts ...metadata.QueryOpt) (uint64, error) {
	activeLayer, exists := mgr.memtable[fileID]
	if exists && len(activeLayer.Chunks) > 0 {
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
		if err == types.ErrNotFound {
			mgr.log.Warn("File not found, nothing to checkpoint", "filename", filename)
			return nil
		}
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	// Check if file has a head pointer, if so it's in read-only mode
	_, _, err = mgr.metaStore.GetHeadVersion(ctx, fileID, metadata.WithTx(tx))
	if err == nil {
		mgr.log.Error("Cannot checkpoint file with head pointing to version", "filename", filename)
		return fmt.Errorf("cannot checkpoint file: %s is in read-only mode because a head is set, use DeleteHead first", filename)
	} else if err != types.ErrNotFound {
		mgr.log.Error("Failed to check head version", "filename", filename, "error", err)
		return fmt.Errorf("failed to check head version: %w", err)
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
func (mgr *Manager) GetAllFiles(ctx context.Context) ([]sqlc.File, error) {
	return mgr.metaStore.GetAllFiles(ctx)
}

// LoadLayersByFileID delegates to the metadata store
func (mgr *Manager) LoadLayersByFileID(ctx context.Context, fileID uint64, opts ...metadata.QueryOpt) ([]*metadata.Layer, error) {
	return mgr.metaStore.LoadLayersByFileID(ctx, fileID, opts...)
}

// getChunkData retrieves chunk data from the object store using range requests
func (mgr *Manager) getChunkData(ctx context.Context, c metadata.Chunk) ([]byte, error) {
	objectKey, err := mgr.metaStore.GetObjectKey(ctx, c.LayerID)
	if err != nil {
		return nil, fmt.Errorf("error retrieving object key: %w", err)
	}

	if objectKey == "" {
		return []byte{}, nil
	}

	layerSize := c.LayerRange[1] - c.LayerRange[0]
	dataRange := [2]uint64{c.LayerRange[0], c.LayerRange[1] - 1} // layer range is exclusive of the end, but object range is inclusive
	data, err := mgr.objectStore.GetObject(ctx, objectKey, dataRange)
	if err != nil {
		return nil, fmt.Errorf("error retrieving data from object store: %w", err)
	}

	if uint64(len(data)) != layerSize {
		return nil, fmt.Errorf("received incorrect number of bytes from object store: got %d, expected %d", len(data), layerSize)
	}

	return data, nil
}

// SetHead sets the head pointer for a file to a specific version
func (mgr *Manager) SetHead(ctx context.Context, filename string, version string) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

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
			panic(p)
		} else if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				mgr.log.Error("Failed to rollback transaction", "error", rbErr)
			}
		}
	}()

	// Get the file ID
	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename, metadata.WithTx(tx))
	if err != nil {
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	// Make sure the version exists by getting its layer
	layer, err := mgr.metaStore.GetLayerByVersion(ctx, fileID, version, tx)
	if err != nil {
		mgr.log.Error("Failed to get layer for version", "version", version, "error", err)
		return fmt.Errorf("failed to get layer for version: %w", err)
	}

	// Set the head
	err = mgr.metaStore.SetHead(ctx, fileID, layer.VersionID, metadata.WithTx(tx))
	if err != nil {
		mgr.log.Error("Failed to set head", "filename", filename, "version", version, "error", err)
		return fmt.Errorf("failed to set head: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		mgr.log.Error("Failed to commit transaction", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	mgr.log.Info("Head set successfully", "filename", filename, "version", version)

	return nil
}

// GetHead gets the current version the file head is pointing to
func (mgr *Manager) GetHead(ctx context.Context, filename string) (string, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	// Get the file ID
	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename)
	if err != nil {
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return "", fmt.Errorf("failed to get file ID: %w", err)
	}

	// Get the head version
	_, versionTag, err := mgr.metaStore.GetHeadVersion(ctx, fileID)
	if err != nil {
		if err == types.ErrNotFound {
			mgr.log.Info("No head set for file", "filename", filename)
			return "", nil
		}
		mgr.log.Error("Failed to get head version", "filename", filename, "error", err)
		return "", fmt.Errorf("failed to get head version: %w", err)
	}

	return versionTag, nil
}

// DeleteHead removes the head pointer for a file
func (mgr *Manager) DeleteHead(ctx context.Context, filename string) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// Get the file ID
	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename)
	if err != nil {
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return fmt.Errorf("failed to get file ID: %w", err)
	}

	// Delete the head
	err = mgr.metaStore.DeleteHead(ctx, fileID)
	if err != nil {
		mgr.log.Error("Failed to delete head", "filename", filename, "error", err)
		return fmt.Errorf("failed to delete head: %w", err)
	}

	mgr.log.Info("Head deleted successfully", "filename", filename)

	return nil
}

// GetAllHeads returns all head pointers with file names and version tags
func (mgr *Manager) GetAllHeads(ctx context.Context) ([]sqlc.GetAllHeadsRow, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	heads, err := mgr.metaStore.GetAllHeads(ctx)
	if err != nil {
		mgr.log.Error("Failed to get all heads", "error", err)
		return nil, fmt.Errorf("failed to get all heads: %w", err)
	}

	return heads, nil
}

// GetFileVersions returns all versions for a specific file
func (mgr *Manager) GetFileVersions(ctx context.Context, filename string) ([]sqlc.Version, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	// Get the file ID
	fileID, err := mgr.metaStore.GetFileIDByName(ctx, filename)
	if err != nil {
		mgr.log.Error("Failed to get file ID", "filename", filename, "error", err)
		return nil, fmt.Errorf("failed to get file ID: %w", err)
	}

	// Get all versions for the file
	versions, err := mgr.metaStore.GetFileVersions(ctx, fileID)
	if err != nil {
		mgr.log.Error("Failed to get file versions", "filename", filename, "error", err)
		return nil, fmt.Errorf("failed to get file versions: %w", err)
	}

	return versions, nil
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
