package main

import (
	"context"
	"fmt"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// FS implements the FUSE filesystem.
type FS struct{}

var globalLM *LayerManager

// init initializes the global layer manager using a PostgreSQL metadata store.
// The connection string is constructed from environment variables or defaults to a local connection.
func init() {
	// Get PostgreSQL connection details from environment variables or use defaults
	host := getEnvOrDefault("POSTGRES_HOST", "localhost")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")
	user := getEnvOrDefault("POSTGRES_USER", "postgres")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "difffs")

	// Construct the connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	ms, err := NewMetadataStore(connStr)
	if err != nil {
		panic(fmt.Sprintf("failed to create metadata store: %v", err))
	}

	lm, err := NewLayerManager(ms)
	if err != nil {
		panic(fmt.Sprintf("failed to create layer manager: %v", err))
	}
	globalLM = lm
}

// getEnvOrDefault returns the environment variable value or a default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func (FS) Root() (fs.Node, error) {
	return Dir{}, nil
}

// Dir represents the root directory.
type Dir struct{}

func (Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0755
	return nil
}

func (Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if name == "dummy.txt" {
		return File{}, nil
	}
	return nil, fuse.ENOENT
}

func (Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{
		{Name: "dummy.txt", Type: fuse.DT_File},
	}, nil
}

// File represents our file whose contents are stored in the layer manager.
type File struct{}

func (File) Attr(ctx context.Context, a *fuse.Attr) error {
	fullContent := globalLM.GetFullContent()
	a.Mode = 0644
	a.Size = uint64(len(fullContent))
	return nil
}

func (File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return File{}, nil
}

func (f File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	data, err := globalLM.GetDataRange(req.Offset, req.Size)
	if err != nil {
		return err
	}
	resp.Data = data
	return nil
}

// Write appends data via the layer manager. We require that writes are at the end of the file.
func (f File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// Ignore req.Offset to always append at the end of the file.
	// This is necessary because shell commands using O_APPEND expect the data to be appended
	// to the end of the file regardless of the offset provided in the write request.
	// By ignoring req.Offset, we ensure that the data is always appended to the end,
	// which fixes the issue with O_APPEND writes from shell commands.
	_, _ = globalLM.Write(req.Data)
	resp.Size = len(req.Data)
	return nil
}

func (f File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}
