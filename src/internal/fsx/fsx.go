package fsx

import (
	"context"
	"fmt"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/charmbracelet/log"
	"github.com/vinimdocarmo/difffs/src/internal/storage"
)

// FS implements the FUSE filesystem.
type FS struct {
	sm  *storage.Manager
	log *log.Logger
}

// Check interface satisfied
var _ fs.FS = (*FS)(nil)

func NewFS(sm *storage.Manager, log *log.Logger) *FS {
	fsLog := log.With()
	fsLog.SetPrefix("📄 fsx")

	return &FS{
		sm:  sm,
		log: fsLog,
	}
}

func (fs *FS) Root() (fs.Node, error) {
	return Dir{
		sm:  fs.sm,
		log: fs.log,
	}, nil
}

// Dir represents the root directory.
type Dir struct {
	sm  *storage.Manager
	log *log.Logger
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

	// Check if the file exists in our database
	fileID, err := dir.sm.GetFileIDByName(name)
	if err != nil || fileID == 0 {
		dir.log.Debug("File not found in database", "name", name)
		return nil, fuse.ENOENT
	}

	// Get file size
	size, err := dir.sm.FileSize(fileID)
	if err != nil {
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
	}

	return file, nil
}

func (dir Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.log.Debug("Reading directory contents")
	all := []fuse.Dirent{}

	// Query the database for all files
	files, err := dir.sm.GetAllFiles()
	if err != nil {
		dir.log.Error("Failed to read directory from database", "error", err)
		return nil, err
	}

	for _, file := range files {
		all = append(all, fuse.Dirent{Name: file.Name, Type: fuse.DT_File})
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
		return fuse.ENOSYS // Operation not supported
	}

	// For files, we need to delete the file from our database
	err := dir.sm.DeleteFile(req.Name)
	if err != nil {
		dir.log.Error("Failed to remove file from database", "name", req.Name, "error", err)
		return err
	}

	dir.log.Info("File removed successfully", "name", req.Name)
	return nil
}

// Create creates and opens a file.
func (dir Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dir.log.Info("Creating file", "filename", req.Name, "flags", req.Flags, "mode", req.Mode)

	// Insert the file into the database
	_, err := dir.sm.InsertFile(req.Name)
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
	}

	dir.log.Debug("File created successfully", "filename", req.Name)
	return file, file, nil
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
}

var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeFsyncer = (*File)(nil)
var _ fs.NodeGetxattrer = (*File)(nil)
var _ fs.NodeListxattrer = (*File)(nil)
var _ fs.NodeSetxattrer = (*File)(nil)
var _ fs.NodeRemovexattrer = (*File)(nil)
var _ fs.NodeReadlinker = (*File)(nil)
var _ fs.NodeRemover = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)

// Attr sets the file attributes
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.log.Debug("Getting file attributes", "name", f.name)

	// Get file metadata from the database
	fileID, err := f.sm.GetFileIDByName(f.name)
	if err != nil {
		f.log.Error("Failed to get file ID", "name", f.name, "error", err)
		return err
	}

	size, err := f.sm.FileSize(fileID)
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

	// Read from the layer manager for all files
	data, err := f.sm.ReadFile(f.name, uint64(req.Offset), uint64(req.Size))
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

	// For consistency, we'll make a copy of the data to prevent races
	dataCopy := make([]byte, len(req.Data))
	copy(dataCopy, req.Data)

	// Use the layer manager to handle the write operation
	// Pass the file name to the layer manager
	err := f.sm.WriteFile(f.name, dataCopy, uint64(req.Offset))
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

// Setattr implements the fs.NodeSetattrer interface
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	f.log.Debug("Setting file attributes", "name", f.name, "valid", req.Valid)

	// Update the times if specified in the request
	if req.Valid.Atime() {
		f.accessed = req.Atime
		f.log.Debug("Updated atime", "name", f.name, "atime", req.Atime)
	}

	if req.Valid.Mtime() {
		f.modified = req.Mtime
		f.log.Debug("Updated mtime", "name", f.name, "mtime", req.Mtime)
	}

	// TODO: truncate file if size is different than current size

	f.log.Debug("File attributes updated successfully", "name", f.name)

	return nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.log.Debug("Releasing file", "name", f.name, "flags", req.Flags)
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f.log.Debug("Syncing file", "name", f.name)
	return nil
}

func (f *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	f.log.Debug("Getting extended attribute", "name", f.name, "attr", req.Name)
	return nil
}

func (f *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	f.log.Debug("Listing extended attributes", "name", f.name)
	return nil
}

func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	f.log.Debug("Setting extended attribute", "name", f.name, "attr", req.Name)
	return nil
}

func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	f.log.Debug("Removing extended attribute", "name", f.name, "attr", req.Name)
	return nil
}

func (f *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	f.log.Debug("Reading symlink", "name", f.name)
	return "", fuse.EIO
}

func (f *File) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	f.log.Debug("Removing file", "name", f.name)

	// Remove the file from the database
	err := f.sm.DeleteFile(f.name)
	if err != nil {
		f.log.Error("Failed to remove file from database", "name", f.name, "error", err)
		return err
	}

	f.log.Info("File removed successfully", "name", f.name)
	return nil
}
