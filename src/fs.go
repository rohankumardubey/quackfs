package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// FS implements the FUSE filesystem.
type FS struct{}

// Check interface satisfied
var _ fs.FS = (*FS)(nil)

var globalLM *LayerManager
var filesMutex sync.RWMutex
var createdFiles = make(map[string]File)

// initFS initializes the global layer manager using a PostgreSQL metadata store.
// The connection string is constructed from environment variables or defaults to a local connection.
func initFS() {
	Logger.Debug("Initializing filesystem")
	// Get PostgreSQL connection details from environment variables or use defaults
	host := getEnvOrDefault("POSTGRES_HOST", "localhost")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")
	user := getEnvOrDefault("POSTGRES_USER", "postgres")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "difffs")

	Logger.Debug("Using env vars", "host", host, "port", port, "user", user, "dbname", dbname)

	// Construct the connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	Logger.Debug("Connecting to metadata store", "host", host, "port", port, "user", user, "dbname", dbname)
	ms, err := NewMetadataStore(connStr)
	if err != nil {
		panic(fmt.Sprintf("failed to create metadata store: %v", err))
	}
	Logger.Info("Metadata store created successfully")

	Logger.Debug("Initializing layer manager")
	lm, err := NewLayerManager(ms)
	if err != nil {
		panic(fmt.Sprintf("failed to create layer manager: %v", err))
	}
	Logger.Info("Layer manager created successfully")

	globalLM = lm
	Logger.Debug("Filesystem initialization complete")
}

// getEnvOrDefault returns the environment variable value or a default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func (FS) Root() (fs.Node, error) {
	Logger.Debug("Getting filesystem root")
	return Dir{}, nil
}

// Dir represents the root directory.
type Dir struct{}

func (Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	Logger.Debug("Getting directory attributes")
	now := time.Now()
	a.Mode = os.ModeDir | 0755
	a.Atime = now
	a.Mtime = now
	a.Ctime = now
	return nil
}

func (Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	Logger.Debug("Looking up file", "name", name)
	if name == "db.duckdb" {
		Logger.Debug("Found primary db file", "name", name)
		return File{
			name:    name,
			created: time.Now(),
		}, nil
	}

	// Check if it's a file we've created during this session
	filesMutex.RLock()
	file, exists := createdFiles[name]
	filesMutex.RUnlock()

	if exists {
		Logger.Debug("Found file in created files map", "name", name)
		return file, nil
	}

	Logger.Debug("File not found", "name", name)
	return nil, fuse.ENOENT
}

func (Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	Logger.Debug("Reading directory contents")
	// Start with our default file
	entries := []fuse.Dirent{
		{Name: "db.duckdb", Type: fuse.DT_File},
	}

	// Add any created files
	filesMutex.RLock()
	fileCount := len(createdFiles)
	for name := range createdFiles {
		Logger.Debug("Adding created file to directory listing", "name", name)
		entries = append(entries, fuse.Dirent{
			Name: name,
			Type: fuse.DT_File,
		})
	}
	filesMutex.RUnlock()

	Logger.Debug("Directory read complete", "totalFiles", len(entries), "createdFiles", fileCount)
	return entries, nil
}

// Create creates and opens a file.
func (Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	Logger.Info("Creating file", "filename", req.Name, "flags", req.Flags, "mode", req.Mode)

	// Create the file
	file := File{
		name:     req.Name,
		created:  time.Now(),
		modified: time.Now(),
		accessed: time.Now(),
		fileSize: 0,
	}

	// Store in our created files map
	filesMutex.Lock()
	createdFiles[req.Name] = file
	filesMutex.Unlock()

	Logger.Debug("File created successfully", "filename", req.Name)
	// Return both the node and the handle (same object implements both interfaces)
	return file, file, nil
}

// File represents our file whose contents are stored in the layer manager.
type File struct {
	name     string
	created  time.Time
	modified time.Time
	accessed time.Time
	fileSize uint64
}

var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeFsyncer = (*File)(nil)
var _ fs.NodeGetxattrer = (*File)(nil)
var _ fs.NodeListxattrer = (*File)(nil)
var _ fs.NodeSetxattrer = (*File)(nil)
var _ fs.NodeRemovexattrer = (*File)(nil)
var _ fs.NodeReadlinker = (*File)(nil)

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	Logger.Debug("Getting file attributes", "name", f.name)

	if f.name == "db.duckdb" {
		size, err := globalLM.FileSize()
		if err != nil {
			Logger.Error("Failed to get file size", "name", f.name, "error", err)
			return err
		}
		a.Mode = 0644
		a.Size = size
		Logger.Debug("Retrieved primary DB file attributes", "name", f.name, "size", a.Size)
		return nil
	}

	// For other files, use our tracked metadata
	a.Mode = 0644
	a.Size = f.fileSize
	a.Atime = f.accessed
	a.Mtime = f.modified
	a.Ctime = f.created

	Logger.Debug("Retrieved file attributes", "name", f.name, "size", a.Size, "modified", a.Mtime)
	return nil
}

func (f File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	Logger.Debug("Opening file", "name", f.name, "flags", req.Flags)
	return f, nil
}

func (f File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	Logger.Debug("Reading file", "name", f.name, "offset", req.Offset, "size", req.Size)

	// Only read from the primary DB file
	if f.name != "db.duckdb" {
		Logger.Debug("Skipping read for non-primary file", "name", f.name)
		return nil // Return empty data for other files
	}

	data, err := globalLM.GetDataRange(uint64(req.Offset), uint64(req.Size))
	if err != nil {
		Logger.Error("Failed to read data", "name", f.name, "error", err)
		return err
	}

	resp.Data = data
	Logger.Debug("Read successful", "name", f.name, "bytesRead", len(resp.Data))
	return nil
}

// Write appends data via the layer manager. We require that writes are at the end of the file.
func (f File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	Logger.Debug("Writing to file", "name", f.name, "offset", req.Offset, "size", len(req.Data))

	// Only write to the primary DB file
	if f.name != "db.duckdb" {
		// For other files, just pretend to write
		resp.Size = len(req.Data)

		// Update the file size if needed
		newSize := uint64(req.Offset) + uint64(len(req.Data))
		filesMutex.Lock()
		if file, exists := createdFiles[f.name]; exists {
			if newSize > file.fileSize {
				file.fileSize = newSize
				file.modified = time.Now()
				createdFiles[f.name] = file
				Logger.Debug("Updated non-primary file size", "name", f.name, "newSize", newSize)
			}
		}
		filesMutex.Unlock()
		Logger.Debug("Simulated write to non-primary file complete", "name", f.name, "bytesWritten", resp.Size)
		return nil
	}

	// For consistency, we'll make a copy of the data to prevent races
	dataCopy := make([]byte, len(req.Data))
	copy(dataCopy, req.Data)

	// Use the layer manager to handle the write operation
	_, _, err := globalLM.Write(req.Data, uint64(req.Offset))
	if err != nil {
		Logger.Error("Failed to write data", "name", f.name, "error", err)
		return fmt.Errorf("failed to write data: %v", err)
	}

	resp.Size = len(req.Data)
	Logger.Debug("Write successful", "name", f.name, "bytesWritten", resp.Size)
	return nil
}

// SetAttr sets file attributes
func (f File) SetAttr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	Logger.Debug("Setting file attributes", "name", f.name, "valid", req.Valid)

	if f.name == "db.duckdb" {
		// For the primary file, most attrs are ignored
		Logger.Debug("Ignoring SetAttr for primary DB file", "name", f.name)
		return nil
	}

	filesMutex.Lock()
	defer filesMutex.Unlock()

	file, exists := createdFiles[f.name]
	if !exists {
		Logger.Debug("File not found in SetAttr", "name", f.name)
		return fuse.ENOENT
	}

	// Update the times if specified in the request
	if req.Valid.Atime() {
		file.accessed = req.Atime
		Logger.Debug("Updated atime", "name", f.name, "atime", req.Atime)
	}

	if req.Valid.Mtime() {
		file.modified = req.Mtime
		Logger.Debug("Updated mtime", "name", f.name, "mtime", req.Mtime)
	}

	// Update size if specified
	if req.Valid.Size() {
		file.fileSize = req.Size
		Logger.Debug("Updated file size", "name", f.name, "size", req.Size)
	}

	// Store updated file
	createdFiles[f.name] = file
	Logger.Debug("File attributes updated successfully", "name", f.name)

	return nil
}

func (f File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	Logger.Debug("Releasing file", "name", f.name, "flags", req.Flags)
	return nil
}

func (f File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	Logger.Debug("Syncing file", "name", f.name)
	return nil
}

func (f File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	Logger.Debug("Getting xattr", "name", f.name, "attr", req.Name)
	return nil
}

func (f File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	Logger.Debug("Listing xattrs", "name", f.name)
	return nil
}

func (f File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	Logger.Debug("Setting xattr", "name", f.name, "attr", req.Name)
	return nil
}

func (f File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	Logger.Debug("Removing xattr", "name", f.name, "attr", req.Name)
	return nil
}

func (f File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	Logger.Debug("Reading link", "name", f.name)
	return "", nil
}
