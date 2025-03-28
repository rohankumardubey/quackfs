package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"
	"github.com/vinimdocarmo/quackfs/internal/fsx"
	"github.com/vinimdocarmo/quackfs/internal/storage"
	objectstore "github.com/vinimdocarmo/quackfs/internal/storage/object"
	"github.com/vinimdocarmo/quackfs/pkg/logger"
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

	// Initialize AWS S3 client (using LocalStack)
	s3Endpoint := getEnvOrDefault("AWS_ENDPOINT_URL", "http://localhost:4566")
	s3Region := getEnvOrDefault("AWS_REGION", "us-east-1")
	s3BucketName := getEnvOrDefault("S3_BUCKET_NAME", "quackfs-bucket")

	log.Debug("Using S3 settings", "endpoint", s3Endpoint, "region", s3Region, "bucket", s3BucketName)

	// Load AWS SDK configuration
	cfgOptions := []func(*config.LoadOptions) error{
		config.WithRegion(s3Region),
	}

	// Add static credentials for LocalStack
	cfgOptions = append(cfgOptions,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"test", "test", "test")))

	cfg, err := config.LoadDefaultConfig(context.Background(), cfgOptions...)
	if err != nil {
		log.Fatal("Failed to configure AWS client", "error", err)
	}

	// Create an S3 client with custom endpoint for LocalStack
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true // Required for LocalStack
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	objectStore := objectstore.NewS3(s3Client, s3BucketName)

	sm := storage.NewManager(db, objectStore, log)

	// Mount the FUSE filesystem.
	c, err := fuse.Mount(*mountpoint, fuse.FSName("quackfs"))
	if err != nil {
		log.Fatal("Failed to mount FUSE", "error", err)
	}
	defer c.Close()

	log.Info("FUSE filesystem mounted", "mountpoint", *mountpoint)
	log.Info("Storing WAL file in", "path", *walPath)
	log.Info("Using PostgreSQL for metadata", "host", os.Getenv("POSTGRES_HOST"))
	log.Info("Using S3 for data storage", "endpoint", s3Endpoint, "bucket", s3BucketName, "region", s3Region)

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
