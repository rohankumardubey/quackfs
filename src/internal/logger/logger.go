package logger

import (
	os "os"
	"time"

	log "github.com/charmbracelet/log"
)

// New creates a new logger instance
func New(output *os.File) *log.Logger {
	// Set log level from environment variable
	level := os.Getenv("LOG_LEVEL")

	logger := log.NewWithOptions(os.Stderr, log.Options{
		ReportCaller:    level == "debug",
		ReportTimestamp: true,
		TimeFormat:      time.TimeOnly,
	})

	if level != "" {
		level, err := log.ParseLevel(level)
		if err == nil {
			logger.SetLevel(level)
		}
	}
	return logger
}
