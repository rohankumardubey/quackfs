package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	log "github.com/charmbracelet/log"
)

func main() {
	// Set log level from environment variable
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err == nil {
			log.SetLevel(level)
		}
	}

	mountpoint := flag.String("mount", "", "Mount point for the FUSE filesystem")
	flag.Parse()

	if *mountpoint == "" {
		log.Info("Usage: ./difffs -mount <mountpoint>")
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
		log.Fatalf("Failed to mount FUSE: %v", err)
	}
	defer c.Close()

	log.Info("FUSE filesystem mounted at", *mountpoint)
	log.Info("Press 'c' to trigger a checkpoint")
	log.Info("Using PostgreSQL for persistence: host=", os.Getenv("POSTGRES_HOST"))

	// Serve the filesystem. fs.Serve blocks until the filesystem is unmounted.
	if err := fs.Serve(c, FS{}); err != nil {
		log.Fatalf("Failed to serve FUSE FS: %v", err)
	}
}
