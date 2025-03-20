package fsx

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/charmbracelet/log"
	"github.com/vinimdocarmo/quackfs/src/internal/storage"
)

// FS implements the FUSE filesystem.
type FS struct {
	sm  *storage.Manager
	log *log.Logger
	wm  *storage.WALManager
}

// Check interface satisfied
var _ fs.FS = (*FS)(nil)

func NewFS(sm *storage.Manager, log *log.Logger, walPath string) *FS {
	fsLog := log.With()
	fsLog.SetPrefix("📄 fsx")

	walManager := storage.NewWALManager(walPath, sm, fsLog)

	return &FS{
		sm:  sm,
		log: fsLog,
		wm:  walManager,
	}
}

func (fs *FS) Root() (fs.Node, error) {
	return Dir{
		sm:         fs.sm,
		log:        fs.log,
		walManager: fs.wm,
	}, nil
}

// Dir represents the root directory.
type Dir struct {
	sm         *storage.Manager
	log        *log.Logger
	walManager *storage.WALManager
}

var _ fs.Node = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)
var _ fs.NodeCreater = (*Dir)(nil)
var _ fs.NodeRemover = (*Dir)(nil)

func (dir Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	dir.log.Debug("Getting directory attributes")
	now := time.Now()
	a.Mode = os.ModeDir | 0755
	a.Atime = now
	a.Mtime = now
	a.Ctime = now
	a.Valid = 1 * time.Second
	return nil
}

// Lookup looks up a specific file in the directory.
func (dir Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	dir.log.Debug("Looking up file", "name", name)

	// Check if the file has a valid extension
	if !checkValidExtension(name) {
		dir.log.Error("File has invalid extension", "name", name)
		return nil, syscall.ENOENT
	}

	// For .duckdb.wal files, check if they exist in the filesystem
	if storage.IsWALFile(name) {
		exists, err := dir.walManager.Exists(name)
		if err != nil {
			dir.log.Error("Failed to check if WAL file exists", "name", name, "error", err)
			return nil, err
		}

		if !exists {
			return nil, syscall.ENOENT
		}

		// Get file info
		size, err := dir.walManager.GetFileSize(name)
		if err != nil {
			dir.log.Error("Failed to get WAL file size", "name", name, "error", err)
			return nil, err
		}

		modTime, err := dir.walManager.GetModTime(name)
		if err != nil {
			dir.log.Error("Failed to get WAL file mod time", "name", name, "error", err)
			return nil, err
		}

		// Create a new file object for the .duckdb.wal file
		now := time.Now()
		file := &File{
			name:     name,
			created:  modTime,
			modified: modTime,
			accessed: now,
			fileSize: size,
			sm:       dir.sm,
			log:      dir.log,
			wm:       dir.walManager,
		}

		return file, nil
	}

	// Get file size for regular files
	size, err := dir.sm.SizeOf(ctx, name)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, syscall.ENOENT
		}
		return nil, err
	}

	// Create a new file object
	now := time.Now()
	file := &File{
		name:     name,
		created:  now,
		modified: now,
		accessed: now,
		fileSize: size,
		sm:       dir.sm,
		log:      dir.log,
		wm:       dir.walManager,
	}

	return file, nil
}

func (dir Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.log.Debug("Reading directory contents")
	all := []fuse.Dirent{}

	// Query the database for all files
	files, err := dir.sm.GetAllFiles(ctx)
	if err != nil {
		dir.log.Error("Failed to read directory from database", "error", err)
		return nil, err
	}

	for _, file := range files {
		all = append(all, fuse.Dirent{Name: file.Name, Type: fuse.DT_File})
	}

	// Add .duckdb.wal files from the WAL manager
	walFiles, err := dir.walManager.ListWALFiles()
	if err != nil {
		dir.log.Error("Failed to list WAL files", "error", err)
		return nil, err
	}

	for _, walFile := range walFiles {
		all = append(all, fuse.Dirent{Name: walFile, Type: fuse.DT_File})
	}

	dir.log.Debug("Directory read complete", "totalFiles", len(all))
	return all, nil
}

// Remove handles the removal of a file or directory.
func (dir Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	dir.log.Debug("Directory received remove request", "name", req.Name)

	// For directories, we would check req.Dir, but we don't support directory removal yet
	if req.Dir {
		dir.log.Warn("Directory removal not supported", "name", req.Name)
		return syscall.ENOSYS // Operation not supported
	}

	if !checkValidExtension(req.Name) {
		dir.log.Error("File has invalid extension", "name", req.Name)
		return syscall.EINVAL
	}

	if !storage.IsWALFile(req.Name) {
		dir.log.Error("File removal is only supported for WAL files for now", "name", req.Name)
		return syscall.ENOSYS
	}

	// For .duckdb.wal files, use the WAL manager to remove them
	err := dir.walManager.Remove(ctx, req.Name)
	if err != nil {
		dir.log.Error("Failed to remove WAL file", "name", req.Name, "error", err)
		return err
	}

	dir.log.Info("WAL file removed successfully", "name", req.Name)
	return nil
}

// Create creates and opens a file.
func (dir Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dir.log.Info("Creating file", "filename", req.Name, "flags", req.Flags, "mode", req.Mode)

	// Check if the file has a valid extension
	if !checkValidExtension(req.Name) {
		dir.log.Info("Rejecting file with invalid extension", "filename", req.Name)
		return nil, nil, syscall.EINVAL
	}

	// For .duckdb.wal files, use the WAL manager to create them
	if storage.IsWALFile(req.Name) {
		dir.log.Info("Creating WAL file", "filename", req.Name)

		err := dir.walManager.Create(req.Name)
		if err != nil {
			dir.log.Error("Failed to create WAL file", "name", req.Name, "error", err)
			return nil, nil, err
		}

		// Create a new file object
		now := time.Now()
		walFile := &File{
			name:     req.Name,
			created:  now,
			modified: now,
			accessed: now,
			fileSize: 0,
			sm:       dir.sm,
			log:      dir.log,
			wm:       dir.walManager,
		}

		dir.log.Debug("WAL file created successfully", "filename", req.Name)
		return walFile, walFile, nil
	}

	// Otherwise, it's a database file, so we need to insert it into the storage manager
	_, err := dir.sm.InsertFile(ctx, req.Name)
	if err != nil {
		dir.log.Error("Failed to insert file into database", "name", req.Name, "error", err)
		return nil, nil, err
	}

	// Create a new file object
	now := time.Now()
	file := &File{
		name:     req.Name,
		created:  now,
		modified: now,
		accessed: now,
		fileSize: 0,
		sm:       dir.sm,
		log:      dir.log,
		wm:       dir.walManager,
	}

	dir.log.Debug("File created successfully", "filename", req.Name)
	return file, file, nil
}

// checkValidExtension checks if the file has a valid extension (.duckdb or .duckdb.wal)
func checkValidExtension(filename string) bool {
	return filename == "duckdb.wal" || filename == "duckdb" || filename == "tmp" ||
		(len(filename) > 0 && (filename[0] != '.' && (filename[len(filename)-7:] == ".duckdb" ||
			filename[len(filename)-11:] == ".duckdb.wal")))
}

// File represents our file whose contents are stored in the layer manager.
type File struct {
	name     string
	created  time.Time
	modified time.Time
	accessed time.Time
	fileSize uint64
	sm       *storage.Manager
	log      *log.Logger
	wm       *storage.WALManager
}

var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeFsyncer = (*File)(nil)
var _ fs.NodeRemover = (*File)(nil)

// Attr sets the file attributes
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.log.Debug("Getting file attributes", "name", f.name)

	if !checkValidExtension(f.name) {
		f.log.Error("File has invalid extension", "name", f.name)
		return syscall.EINVAL
	}

	// For .duckdb.wal files, get attributes from the WAL manager
	if storage.IsWALFile(f.name) {
		size, err := f.wm.GetFileSize(f.name)
		if err != nil {
			f.log.Error("Failed to get WAL file size", "name", f.name, "error", err)
			return err
		}

		modTime, err := f.wm.GetModTime(f.name)
		if err != nil {
			// If file doesn't exist yet, return default attributes
			if os.IsNotExist(err) {
				a.Mode = 0644
				a.Size = 0
				a.Mtime = f.modified
				a.Ctime = f.created
				a.Atime = f.accessed
				a.Valid = 1 * time.Second
				return nil
			}
			f.log.Error("Failed to get WAL file mod time", "name", f.name, "error", err)
			return err
		}

		// Set attributes from file info
		a.Mode = 0644
		a.Size = size
		a.Mtime = modTime
		a.Ctime = modTime
		a.Atime = time.Now() // OS doesn't typically track atime precisely
		a.Valid = 1 * time.Second

		f.log.Debug("Retrieved WAL file attributes", "name", f.name, "size", a.Size)
		return nil
	}

	// For regular files, get size from the storage manager
	size, err := f.sm.SizeOf(ctx, f.name)
	if err != nil {
		f.log.Error("Failed to get file size", "name", f.name, "error", err)
		return err
	}

	a.Mode = 0644
	a.Size = size
	a.Mtime = f.modified
	a.Ctime = f.created
	a.Atime = f.accessed
	a.Valid = 1 * time.Second

	f.log.Debug("Retrieved file attributes", "name", f.name, "size", a.Size)
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f.log.Debug("Opening file", "name", f.name, "flags", req.Flags)
	return f, nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.log.Debug("Reading file", "name", f.name, "offset", req.Offset, "size", req.Size)

	if !checkValidExtension(f.name) {
		f.log.Error("File has invalid extension", "name", f.name)
		return syscall.EINVAL
	}

	// For .duckdb.wal files, use the WAL manager
	if storage.IsWALFile(f.name) {
		f.log.Debug("Reading WAL file", "name", f.name)
		data, err := f.wm.Read(f.name, uint64(req.Offset), uint64(req.Size))
		if err != nil {
			f.log.Error("Failed to read WAL file", "name", f.name, "error", err)
			return err
		}
		resp.Data = data
		f.log.Debug("Read successful for WAL file", "name", f.name, "bytesRead", len(resp.Data))
		return nil
	}

	// Read from the layer manager for all other files
	data, err := f.sm.ReadFile(ctx, f.name, uint64(req.Offset), uint64(req.Size))
	if err != nil {
		f.log.Error("Failed to read data", "name", f.name, "error", err)
		return err
	}

	resp.Data = data
	f.log.Debug("Read successful", "name", f.name, "bytesRead", len(resp.Data))
	return nil
}

// Write appends data via the layer manager. We require that writes are at the end of the file.
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.log.Debug("Writing to file", "name", f.name, "size", len(req.Data), "offset", req.Offset, "fileFlags", req.FileFlags)

	if !checkValidExtension(f.name) {
		f.log.Error("File has invalid extension", "name", f.name)
		return syscall.EINVAL
	}

	// For consistency, we'll make a copy of the data to prevent races
	dataCopy := make([]byte, len(req.Data))
	copy(dataCopy, req.Data)

	// For .duckdb.wal files, use the WAL manager
	if storage.IsWALFile(f.name) {
		f.log.Debug("Writing WAL file", "name", f.name)
		bytesWritten, err := f.wm.Write(f.name, dataCopy, uint64(req.Offset))
		if err != nil {
			f.log.Error("Failed to write WAL file", "name", f.name, "error", err)
			return fmt.Errorf("failed to write WAL data: %v", err)
		}

		f.fileSize = uint64(req.Offset) + uint64(bytesWritten)
		f.modified = time.Now()

		resp.Size = bytesWritten
		f.log.Debug("Write successful for WAL file", "name", f.name, "bytesWritten", resp.Size)
		return nil
	}

	// Use the layer manager to handle the write operation for all other files
	err := f.sm.WriteFile(ctx, f.name, dataCopy, uint64(req.Offset))
	if err != nil {
		f.log.Error("Failed to write data", "name", f.name, "error", err)
		return fmt.Errorf("failed to write data: %v", err)
	}

	f.fileSize = uint64(req.Offset) + uint64(len(dataCopy))
	f.modified = time.Now()

	resp.Size = len(req.Data)
	f.log.Debug("Write successful", "name", f.name, "bytesWritten", resp.Size)
	return nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.log.Debug("Releasing file", "name", f.name, "flags", req.Flags)
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f.log.Debug("Syncing file", "name", f.name)

	// For WAL files, we need to sync them to disk
	if storage.IsWALFile(f.name) {
		err := f.wm.Sync(f.name)
		if err != nil {
			f.log.Error("Failed to sync WAL file", "name", f.name, "error", err)
			return err
		}
	}

	return nil
}

func (f *File) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	f.log.Debug("Removing file", "name", f.name)

	if !checkValidExtension(f.name) {
		f.log.Error("File has invalid extension", "name", f.name)
		return syscall.EINVAL
	}

	if !storage.IsWALFile(f.name) {
		f.log.Error("File removal is only supported for WAL files for now", "name", f.name)
		return syscall.EINVAL
	}

	// For .duckdb.wal files, use the WAL manager to remove them
	err := f.wm.Remove(ctx, f.name)
	if err != nil {
		f.log.Error("Failed to remove WAL file", "name", f.name, "error", err)
		return err
	}

	f.log.Info("WAL file removed successfully", "name", f.name)
	return nil
}
