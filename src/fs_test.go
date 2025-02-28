package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"bazil.org/fuse"
)

func TestFuseReadWrite(t *testing.T) {
	// Fail if PostgreSQL connection isn't available for testing
	connStr := os.Getenv("POSTGRES_TEST_CONN")
	if connStr == "" {
		t.Fatal("PostgreSQL connection string not provided. Set POSTGRES_TEST_CONN environment variable")
	}

	// Create a temporary mount directory.
	mountDir, err := os.MkdirTemp("", "fusemnt")
	if err != nil {
		t.Fatalf("TempDir error: %v", err)
	}
	defer os.RemoveAll(mountDir)

	// Build the FUSE filesystem binary (build from current directory).
	binaryPath := filepath.Join(os.TempDir(), "myfusefs_test")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Build error: %v, output: %s", err, output)
	}
	defer os.Remove(binaryPath)

	// Set environment variables for the test process
	env := os.Environ()
	env = append(env, "POSTGRES_TEST_CONN="+connStr)
	env = append(env, "POSTGRES_HOST="+os.Getenv("POSTGRES_HOST"))
	env = append(env, "POSTGRES_PORT="+os.Getenv("POSTGRES_PORT"))
	env = append(env, "POSTGRES_USER="+os.Getenv("POSTGRES_USER"))
	env = append(env, "POSTGRES_PASSWORD="+os.Getenv("POSTGRES_PASSWORD"))
	env = append(env, "POSTGRES_DB="+os.Getenv("POSTGRES_DB"))

	// Start the FUSE filesystem process.
	cmd := exec.Command(binaryPath, "-mount", mountDir)
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start FUSE FS: %v", err)
	}

	// Allow time for the mount to complete.
	time.Sleep(2 * time.Second)

	// Open the file for read/write.
	filePath := filepath.Join(mountDir, "dummy.txt")
	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filePath, err)
	}

	// Write some data.
	dataToWrite := []byte("test data")
	n, err := f.Write(dataToWrite)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if n != len(dataToWrite) {
		t.Fatalf("Write incomplete: wrote %d bytes, expected %d", n, len(dataToWrite))
	}

	// Seek back to the beginning.
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	// Read the data back.
	readBuf := make([]byte, len(dataToWrite))
	n, err = f.Read(readBuf)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	if n != len(dataToWrite) {
		t.Fatalf("Read incomplete: read %d bytes, expected %d", n, len(dataToWrite))
	}
	if !bytes.Equal(readBuf, dataToWrite) {
		t.Fatalf("Expected %q, got %q", dataToWrite, readBuf)
	}

	// Close the file handle to ensure it's not busy.
	if err := f.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Retry unmounting multiple times in case the FS is still busy.
	var unmountErr error
	for i := 0; i < 5; i++ {
		unmountErr = fuse.Unmount(mountDir)
		if unmountErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if unmountErr != nil {
		t.Fatalf("Failed to unmount after retries: %v", unmountErr)
	}

	// Wait for the FUSE process to exit.
	cmd.Wait()
}
