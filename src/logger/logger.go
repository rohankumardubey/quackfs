package logger

import (
	os "os"

	log "github.com/charmbracelet/log"
)

// Log is our custom logger instance
var Log *log.Logger = log.New(os.Stdout)
