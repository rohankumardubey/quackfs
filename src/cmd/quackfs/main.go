package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "github.com/lib/pq"
	"github.com/vinimdocarmo/quackfs/src/internal/fsx"
	"github.com/vinimdocarmo/quackfs/src/internal/logger"
	"github.com/vinimdocarmo/quackfs/src/internal/storage"
)

func main() {
	// Initialize logger first thing
	log := logger.New(os.Stderr)

	mountpoint := flag.String("mount", "", "Mount point for the FUSE filesystem")
	flag.Parse()

	if *mountpoint == "" {
		log.Info("Usage: ./quackfs -mount <mountpoint>")
		os.Exit(1)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal("Failed to get home directory", "error", err)
	}

	walPath := flag.String("wal-path", homeDir, "Path to the WAL file")
	flag.Parse()

	fmt.Println(`
  __
>(o )___
 (  ._> /
  '---'
Differential Storage System for DuckDB
	`)

	host := getEnvOrDefault("POSTGRES_HOST", "localhost")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")
	user := getEnvOrDefault("POSTGRES_USER", "postgres")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "quackfs")

	log.Debug("Using env vars", "host", host, "port", port, "user", user, "dbname", dbname)

	// Construct the connection string
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Fatal("Failed to create database connection", "error", err)
	}
	defer db.Close()

	sm := storage.NewManager(db, log)

	// Mount the FUSE filesystem.
	c, err := fuse.Mount(*mountpoint, fuse.FSName("quackfs"))
	if err != nil {
		log.Fatal("Failed to mount FUSE", "error", err)
	}
	defer c.Close()

	log.Info("FUSE filesystem mounted", "mountpoint", *mountpoint)
	log.Info("Storing WAL file in", "path", *walPath)
	log.Info("Using PostgreSQL for persistence", "host", os.Getenv("POSTGRES_HOST"))

	// Serve the filesystem. fs.Serve blocks until the filesystem is unmounted.
	if err := fs.Serve(c, fsx.NewFS(sm, log, *walPath)); err != nil {
		log.Fatal("Failed to serve FUSE FS", "error", err)
	}
}

// getEnvOrDefault returns the environment variable value or a default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
