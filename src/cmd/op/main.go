package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	log "github.com/charmbracelet/log"
	_ "github.com/lib/pq"
	"github.com/vinimdocarmo/difffs/src/internal/logger"
	"github.com/vinimdocarmo/difffs/src/internal/storage"
)

// Initialize our global logger
func initLogger() {
	// Set log level from environment variable
	logLevel := getEnvOrDefault("LOG_LEVEL", "info")

	logger.Log = log.NewWithOptions(os.Stderr, log.Options{
		ReportCaller:    logLevel == "debug",
		ReportTimestamp: true,
		TimeFormat:      time.Kitchen,
	})

	if logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err == nil {
			logger.Log.SetLevel(level)
		}
	}
}

func main() {
	// Initialize logger first thing
	initLogger()

	// Check if a subcommand was provided
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Extract the subcommand
	command := os.Args[1]

	// Remove the subcommand from os.Args to make flag parsing work correctly
	os.Args = slices.Delete(os.Args, 1, 2)

	// Connect to the database
	db := newDB()
	defer db.Close()

	// Create a storage manager
	sm, err := storage.NewManager(db)
	if err != nil {
		logger.Log.Fatal("Failed to create storage manager", "error", err)
	}
	defer sm.Close()

	// Execute the appropriate command
	switch command {
	case "write":
		executeWriteCommand(sm)
	case "checkpoint":
		executeCheckpointCommand(sm)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

// printUsage prints the usage information for the CLI tool
func printUsage() {
	fmt.Println("Usage: op <command> [options]")
	fmt.Println("Commands:")
	fmt.Println("  write      - Write data to a file")
	fmt.Println("  checkpoint - Checkpoint a file")
}

// executeWriteCommand handles the "write" subcommand
func executeWriteCommand(sm *storage.Manager) {
	// Define command-line flags for write command
	writeCmd := flag.NewFlagSet("write", flag.ExitOnError)
	fileName := writeCmd.String("file", "", "Target file to write to")
	offset := writeCmd.Uint64("offset", 0, "Offset in the file to start writing from")
	data := writeCmd.String("data", "", "ASCII data to write to the file")
	allowBeyondSize := writeCmd.Bool("allow-beyond-size", true, "Allow writing beyond current file size (fills gap with null bytes)")

	// Parse the flags
	writeCmd.Parse(os.Args[1:])

	// Validate required flags
	if *fileName == "" {
		logger.Log.Error("Missing required flag: -file")
		fmt.Println("Usage: op write -file <filename> -offset <offset> -data <data>")
		os.Exit(1)
	}

	if *data == "" {
		logger.Log.Error("Missing required flag: -data")
		fmt.Println("Usage: op write -file <filename> -offset <offset> -data <data>")
		os.Exit(1)
	}

	// Check if the file exists
	fileID, err := sm.GetFileIDByName(*fileName)
	if err != nil {
		logger.Log.Fatal("Failed to check if file exists", "error", err)
	}

	// If the file doesn't exist, create it
	if fileID == 0 {
		logger.Log.Info("File does not exist, creating it", "fileName", *fileName)
		fileID, err = sm.InsertFile(*fileName)
		if err != nil {
			logger.Log.Fatal("Failed to create file", "error", err)
		}
	}

	// Get current file size
	fileSize := uint64(0)
	if fileID != 0 {
		fileSize, err = sm.FileSize(fileID)
		if err != nil {
			logger.Log.Fatal("Failed to get file size", "error", err)
		}
	}

	// If writing beyond file size and it's allowed, fill the gap with null bytes
	if *offset > fileSize && *allowBeyondSize {
		if *offset > fileSize {
			logger.Log.Info("Writing beyond file size, filling gap with null bytes",
				"fileName", *fileName,
				"currentSize", fileSize,
				"targetOffset", *offset)

			// Calculate the gap size
			gapSize := *offset - fileSize

			// Only fill the gap if it's not too large (prevent accidental huge allocations)
			if gapSize > 1024*1024*10 { // 10MB limit
				logger.Log.Fatal("Gap size too large, aborting", "gapSize", gapSize)
			}

			// Fill the gap with null bytes if needed
			if gapSize > 0 {
				nullBytes := make([]byte, gapSize)
				_, _, err := sm.Write(*fileName, nullBytes, fileSize)
				if err != nil {
					logger.Log.Fatal("Failed to fill gap with null bytes", "error", err)
				}
				logger.Log.Info("Gap filled with null bytes", "gapSize", gapSize)
			}
		}
	}

	// Write the data to the file at the specified offset
	dataBytes := []byte(*data)
	layerID, writtenOffset, err := sm.Write(*fileName, dataBytes, *offset)
	if err != nil {
		logger.Log.Fatal("Failed to write data", "error", err)
	}

	logger.Log.Info("Data written successfully",
		"fileName", *fileName,
		"offset", *offset,
		"dataSize", len(dataBytes),
		"layerID", layerID,
		"writtenOffset", writtenOffset)

	fmt.Printf("Successfully wrote %d bytes to %s at offset %d\n", len(dataBytes), *fileName, *offset)
}

// executeCheckpointCommand handles the "checkpoint" subcommand
func executeCheckpointCommand(sm *storage.Manager) {
	// Define command-line flags for checkpoint command
	checkpointCmd := flag.NewFlagSet("checkpoint", flag.ExitOnError)
	fileName := checkpointCmd.String("file", "", "Target file to checkpoint")

	// Parse the flags
	checkpointCmd.Parse(os.Args[1:])

	// Validate required flags
	if *fileName == "" {
		logger.Log.Error("Missing required flag: -file")
		fmt.Println("Usage: op checkpoint -file <filename>")
		os.Exit(1)
	}

	// Check if the file exists
	fileID, err := sm.GetFileIDByName(*fileName)
	if err != nil {
		logger.Log.Fatal("Failed to check if file exists", "error", err)
	}

	// If the file doesn't exist, report an error
	if fileID == 0 {
		logger.Log.Error("File does not exist", "fileName", *fileName)
		fmt.Printf("Error: File '%s' does not exist\n", *fileName)
		os.Exit(1)
	}

	// Checkpoint the file
	err = sm.Checkpoint(*fileName)
	if err != nil {
		logger.Log.Fatal("Failed to checkpoint file", "error", err)
	}

	logger.Log.Info("File checkpointed successfully", "fileName", *fileName)
	fmt.Printf("Successfully checkpointed file %s\n", *fileName)
}

// newDB establishes a connection to the database
func newDB() *sql.DB {
	// Connect to the database
	host := getEnvOrDefault("POSTGRES_HOST", "localhost")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")
	user := getEnvOrDefault("POSTGRES_USER", "postgres")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "difffs")

	logger.Log.Debug("Using env vars", "host", host, "port", port, "user", user, "dbname", dbname)

	// Construct the connection string
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		logger.Log.Fatal("Failed to create database connection", "error", err)
	}

	return db
}

// getEnvOrDefault returns the environment variable value or a default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
