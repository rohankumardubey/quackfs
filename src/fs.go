package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// FS implements the FUSE filesystem.
type FS struct {
	lm *LayerManager
}

// Check interface satisfied
var _ fs.FS = (*FS)(nil)

func NewFS(lm *LayerManager) *FS {
	return &FS{lm: lm}
}

func (fs *FS) Root() (fs.Node, error) {
	return Dir{lm: fs.lm}, nil
}

// Dir represents the root directory.
type Dir struct {
	lm *LayerManager
}

var _ fs.Node = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)
var _ fs.NodeCreater = (*Dir)(nil)
var _ fs.NodeRemover = (*Dir)(nil)

func (Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	Logger.Debug("Getting directory attributes")
	now := time.Now()
	a.Mode = os.ModeDir | 0755
	a.Atime = now
	a.Mtime = now
	a.Ctime = now
	return nil
}

// Lookup looks up a specific file in the directory.
func (dir Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	Logger.Debug("Looking up file", "name", name)

	// Check if the file exists in our database
	fileID, err := dir.lm.metadata.GetFileIDByName(name)
	if err != nil || fileID == 0 {
		Logger.Debug("File not found in database", "name", name)
		return nil, fuse.ENOENT
	}

	// Return a file node for the existing file
	return &File{
		name:     name,
		created:  time.Now(),
		modified: time.Now(),
		accessed: time.Now(),
		lm:       dir.lm,
	}, nil
}

func (dir Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	Logger.Debug("Reading directory contents")
	entries := []fuse.Dirent{}

	// Query the database for all files
	rows, err := dir.lm.metadata.db.Query("SELECT name FROM files")
	if err != nil {
		Logger.Error("Failed to read directory from database", "error", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			Logger.Error("Failed to scan file name", "error", err)
			return nil, err
		}
		Logger.Debug("Adding file to directory listing", "name", name)
		entries = append(entries, fuse.Dirent{Name: name, Type: fuse.DT_File})
	}

	if err := rows.Err(); err != nil {
		Logger.Error("Error iterating over file rows", "error", err)
		return nil, err
	}

	Logger.Debug("Directory read complete", "totalFiles", len(entries))
	return entries, nil
}

// Remove handles the removal of a file or directory.
func (dir Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	Logger.Debug("Directory received remove request", "name", req.Name)

	// For directories, we would check req.Dir, but we don't support directory removal yet
	if req.Dir {
		Logger.Warn("Directory removal not supported", "name", req.Name)
		return fuse.ENOSYS // Operation not supported
	}

	// For files, we need to delete the file from our database
	err := dir.lm.metadata.DeleteFile(req.Name)
	if err != nil {
		Logger.Error("Failed to remove file from database", "name", req.Name, "error", err)
		return err
	}

	Logger.Info("File removed successfully", "name", req.Name)
	return nil
}

// Create creates and opens a file.
func (dir Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	Logger.Info("Creating file", "filename", req.Name, "flags", req.Flags, "mode", req.Mode)

	// Insert the file into the database
	_, err := dir.lm.metadata.InsertFile(req.Name)
	if err != nil {
		Logger.Error("Failed to insert file into database", "name", req.Name, "error", err)
		return nil, nil, err
	}

	// Create the file object
	file := &File{
		name:     req.Name,
		created:  time.Now(),
		modified: time.Now(),
		accessed: time.Now(),
		fileSize: 0,
		lm:       dir.lm,
	}

	Logger.Debug("File created successfully", "filename", req.Name)
	return file, file, nil
}

// File represents our file whose contents are stored in the layer manager.
type File struct {
	name     string
	created  time.Time
	modified time.Time
	accessed time.Time
	fileSize uint64
	lm       *LayerManager
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

// Attr sets the file attributes
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	Logger.Debug("Getting file attributes", "name", f.name)

	// Get file metadata from the database
	fileID, err := f.lm.metadata.GetFileIDByName(f.name)
	if err != nil {
		Logger.Error("Failed to get file ID", "name", f.name, "error", err)
		return err
	}

	size, err := f.lm.FileSize(fileID)
	if err != nil {
		Logger.Error("Failed to get file size", "name", f.name, "error", err)
		return err
	}

	a.Mode = 0644
	a.Size = size
	a.Mtime = f.modified
	a.Ctime = f.created
	a.Atime = f.accessed

	Logger.Debug("Retrieved file attributes", "name", f.name, "size", a.Size)
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	Logger.Debug("Opening file", "name", f.name, "flags", req.Flags)
	return f, nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	Logger.Debug("Reading file", "name", f.name, "offset", req.Offset, "size", req.Size)

	// Read from the layer manager for all files
	data, err := f.lm.GetDataRange(f.name, uint64(req.Offset), uint64(req.Size))
	if err != nil {
		Logger.Error("Failed to read data", "name", f.name, "error", err)
		return err
	}

	resp.Data = data
	Logger.Debug("Read successful", "name", f.name, "bytesRead", len(resp.Data))
	return nil
}

// Write appends data via the layer manager. We require that writes are at the end of the file.
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	Logger.Debug("Writing to file", "name", f.name, "offset", req.Offset, "size", len(req.Data))

	// For consistency, we'll make a copy of the data to prevent races
	dataCopy := make([]byte, len(req.Data))
	copy(dataCopy, req.Data)

	// Use the layer manager to handle the write operation
	// Pass the file name to the layer manager
	_, _, err := f.lm.Write(f.name, dataCopy, uint64(req.Offset))
	if err != nil {
		Logger.Error("Failed to write data", "name", f.name, "error", err)
		return fmt.Errorf("failed to write data: %v", err)
	}

	// Update the file size if this write extends the file
	newEndOffset := uint64(req.Offset) + uint64(len(req.Data))
	if newEndOffset > f.fileSize {
		f.fileSize = newEndOffset
		f.modified = time.Now()
		Logger.Debug("Updated file size after write", "name", f.name, "newSize", f.fileSize)
	}

	resp.Size = len(req.Data)
	Logger.Debug("Write successful", "name", f.name, "bytesWritten", resp.Size)
	return nil
}

// SetAttr sets file attributes
func (f *File) SetAttr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	Logger.Debug("Setting file attributes", "name", f.name, "valid", req.Valid)

	// Update the times if specified in the request
	if req.Valid.Atime() {
		f.accessed = req.Atime
		Logger.Debug("Updated atime", "name", f.name, "atime", req.Atime)
	}

	if req.Valid.Mtime() {
		f.modified = req.Mtime
		Logger.Debug("Updated mtime", "name", f.name, "mtime", req.Mtime)
	}

	// Update size if specified
	if req.Valid.Size() {
		f.fileSize = req.Size
		Logger.Debug("Updated file size", "name", f.name, "size", req.Size)
	}

	Logger.Debug("File attributes updated successfully", "name", f.name)

	return nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	Logger.Debug("Releasing file", "name", f.name, "flags", req.Flags)
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	Logger.Debug("Syncing file", "name", f.name)
	return nil
}

func (f *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	Logger.Debug("Getting extended attribute", "name", f.name, "attr", req.Name)
	return fuse.ENODATA
}

func (f *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	Logger.Debug("Listing extended attributes", "name", f.name)
	return nil
}

func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	Logger.Debug("Setting extended attribute", "name", f.name, "attr", req.Name)
	return nil
}

func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	Logger.Debug("Removing extended attribute", "name", f.name, "attr", req.Name)
	return nil
}

func (f *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	Logger.Debug("Reading symlink", "name", f.name)
	return "", fuse.EIO
}

func (f *File) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	Logger.Debug("Removing file", "name", f.name)

	// Remove the file from the database
	err := f.lm.metadata.DeleteFile(f.name)
	if err != nil {
		Logger.Error("Failed to remove file from database", "name", f.name, "error", err)
		return err
	}

	Logger.Info("File removed successfully", "name", f.name)
	return nil
}
