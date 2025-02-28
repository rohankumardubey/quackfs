package main

import (
	"bufio"
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

	// Initialize the filesystem
	initFS()

	// Start a goroutine to listen for keypresses.
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			b, err := reader.ReadByte()
			if err != nil {
				continue
			}
			if b == 'c' {
				simulateCheckpoint(globalLM)
			}
		}
	}()

	// Mount the FUSE filesystem.
	c, err := fuse.Mount(*mountpoint)
	if err != nil {
		Logger.Fatal("Failed to mount FUSE", "error", err)
	}
	defer c.Close()

	Logger.Info("FUSE filesystem mounted at", *mountpoint)
	Logger.Info("Press 'c' to trigger a checkpoint")
	Logger.Info("Using PostgreSQL for persistence: host=", os.Getenv("POSTGRES_HOST"))

	// Serve the filesystem. fs.Serve blocks until the filesystem is unmounted.
	if err := fs.Serve(c, FS{}); err != nil {
		Logger.Fatal("Failed to serve FUSE FS", "error", err)
	}
}
