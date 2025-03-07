package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	log "github.com/charmbracelet/log"
)

// Initialize our global logger
func initLogger() {
	// Set log level from environment variable
	logLevel := getEnvOrDefault("LOG_LEVEL", "info")

	Logger = log.NewWithOptions(os.Stderr, log.Options{
		ReportCaller:    logLevel == "debug",
		ReportTimestamp: true,
		TimeFormat:      time.Kitchen,
	})

	if logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err == nil {
			Logger.SetLevel(level)
		}
	}
}

func main() {
	// Initialize logger first thing
	initLogger()

	mountpoint := flag.String("mount", "", "Mount point for the FUSE filesystem")
	flag.Parse()

	if *mountpoint == "" {
		Logger.Info("Usage: ./difffs -mount <mountpoint>")
		os.Exit(1)
	}

	fmt.Println(`

██████╗░██╗███████╗███████╗███████╗░██████╗
██╔══██╗██║██╔════╝██╔════╝██╔════╝██╔════╝
██║░░██║██║█████╗░░█████╗░░█████╗░░╚█████╗░
██║░░██║██║██╔══╝░░██╔══╝░░██╔══╝░░░╚═══██╗
██████╔╝██║██║░░░░░██║░░░░░██║░░░░░██████╔╝
╚═════╝░╚═╝╚═╝░░░░░╚═╝░░░░░╚═╝░░░░░╚═════╝░
Differential Storage System

	`)

	host := getEnvOrDefault("POSTGRES_HOST", "localhost")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")
	user := getEnvOrDefault("POSTGRES_USER", "postgres")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "difffs")

	Logger.Debug("Using env vars", "host", host, "port", port, "user", user, "dbname", dbname)

	// Construct the connection string
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	ms, err := NewMetadataStore(conn)
	if err != nil {
		panic(fmt.Sprintf("failed to create metadata store: %v", err))
	}

	lm, err := NewLayerManager(ms)
	if err != nil {
		panic(fmt.Sprintf("failed to create layer manager: %v", err))
	}

	// Mount the FUSE filesystem.
	c, err := fuse.Mount(*mountpoint)
	if err != nil {
		Logger.Fatal("Failed to mount FUSE", "error", err)
	}
	defer c.Close()

	Logger.Info("FUSE filesystem mounted at", *mountpoint)
	Logger.Info("Using PostgreSQL for persistence: host=", os.Getenv("POSTGRES_HOST"))

	// Serve the filesystem. fs.Serve blocks until the filesystem is unmounted.
	if err := fs.Serve(c, NewFS(lm)); err != nil {
		Logger.Fatal("Failed to serve FUSE FS", "error", err)
	}
}

// getEnvOrDefault returns the environment variable value or a default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
