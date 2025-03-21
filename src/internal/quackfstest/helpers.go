package quackfstest

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"

	"github.com/vinimdocarmo/quackfs/src/internal/logger"
	"github.com/vinimdocarmo/quackfs/src/internal/storage"
)

func SetupStorageManager(t *testing.T) (*storage.Manager, func()) {
	connStr := GetTestConnectionString(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	// Create a test log
	log := logger.New(os.Stderr)

	// Set up S3 client for tests
	s3Endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if s3Endpoint == "" {
		s3Endpoint = "http://localhost:4566"
	}

	s3Region := os.Getenv("AWS_REGION")
	if s3Region == "" {
		s3Region = "us-east-1"
	}

	s3BucketName := os.Getenv("S3_BUCKET_NAME")
	if s3BucketName == "" {
		s3BucketName = "quackfs-bucket-test"
	}

	// Load AWS SDK configuration with static credentials for LocalStack
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(s3Region),
		// Use static credentials for LocalStack
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"test", "test", "test")),
	)
	if err != nil {
		t.Fatalf("Failed to configure AWS client: %v", err)
	}

	// Create S3 client with LocalStack endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true // Required for LocalStack
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	sm := storage.NewManager(db, s3Client, s3BucketName, log)

	cleanup := func() {
		// delete all rows in all tables
		_, err = db.Exec("DELETE FROM chunks")
		if err != nil {
			t.Fatalf("Failed to clean chunks table: %v", err)
		}
		_, err = db.Exec("DELETE FROM snapshot_layers")
		if err != nil {
			t.Fatalf("Failed to clean snapshot_layers table: %v", err)
		}
		_, err = db.Exec("DELETE FROM files")
		if err != nil {
			t.Fatalf("Failed to clean files table: %v", err)
		}
		sm.Close()
	}

	return sm, cleanup
}

// GetTestConnectionString returns the PostgreSQL connection string for tests
func GetTestConnectionString(t *testing.T) string {
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}
	return connStr
}

func SetupDB(t *testing.T) *sql.DB {
	connStr := GetTestConnectionString(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	return db
}
