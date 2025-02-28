package main

import (
	os "os"

	log "github.com/charmbracelet/log"
)

// Logger is our custom logger instance
var Logger *log.Logger = log.New(os.Stdout)
