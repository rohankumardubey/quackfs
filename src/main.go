package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

func main() {
	mountpoint := flag.String("mount", "", "Mount point for the FUSE filesystem")
	flag.Parse()

	if *mountpoint == "" {
		fmt.Println("Usage: ./difffs -mount <mountpoint>")
		os.Exit(1)
	}

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

	fmt.Println("FUSE filesystem mounted at", *mountpoint)
	fmt.Println("Press 'c' to trigger a checkpoint")
	fmt.Println("Using PostgreSQL for persistence: host=", os.Getenv("POSTGRES_HOST"))

	// Serve the filesystem. fs.Serve blocks until the filesystem is unmounted.
	if err := fs.Serve(c, FS{}); err != nil {
		log.Fatalf("Failed to serve FUSE FS: %v", err)
	}
}
