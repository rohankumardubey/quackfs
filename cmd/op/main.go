package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	log "github.com/charmbracelet/log"
	_ "github.com/lib/pq"
	"github.com/vinimdocarmo/quackfs/db/sqlc"
	"github.com/vinimdocarmo/quackfs/internal/storage"
	objectstore "github.com/vinimdocarmo/quackfs/internal/storage/object"
	"github.com/vinimdocarmo/quackfs/pkg/logger"
)

func main() {
	// Initialize logger first thing
	log := logger.New(os.Stderr)

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
	db := newDB(log)
	defer db.Close()

	// Set up S3 client
	s3Endpoint := getEnvOrDefault("AWS_ENDPOINT_URL", "http://localhost:4566")
	s3Region := getEnvOrDefault("AWS_REGION", "us-east-1")
	s3BucketName := getEnvOrDefault("S3_BUCKET_NAME", "quackfs-bucket")

	// Load AWS SDK configuration
	cfgOptions := []func(*config.LoadOptions) error{
		config.WithRegion(s3Region),
	}

	log.Debug("Using static credentials for LocalStack")
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
	})

	objectStore := objectstore.NewS3(s3Client, s3BucketName)

	// Create a storage manager
	sm := storage.NewManager(db, objectStore, log)

	// Execute the appropriate command
	switch command {
	case "log":
		executeLogCommand(sm, log)
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
	fmt.Println("  log        - List all versions for a specific file and indicate head pointer")
	fmt.Println("")
	fmt.Println("For detailed command usage:")
	fmt.Println("  op log -h")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  op log -file myfile.txt")
}

func executeLogCommand(sm *storage.Manager, log *log.Logger) {
	logCmd := flag.NewFlagSet("log", flag.ExitOnError)
	fileName := logCmd.String("file", "", "Target file to show version history for")

	logCmd.Parse(os.Args[1:])

	if *fileName == "" {
		log.Error("Missing required flag: -file")
		fmt.Println("Usage: op log -file <filename>")
		os.Exit(1)
	}

	ctx := context.Background()

	versions, err := sm.GetFileVersions(ctx, *fileName)
	if err != nil {
		log.Fatal("Failed to get file versions", "error", err)
	}

	if len(versions) == 0 {
		fmt.Printf("No versions found for file: %s\n", *fileName)
		return
	}

	headVersion, err := sm.GetHead(ctx, *fileName)
	if err != nil {
		log.Fatal("Failed to get head version", "error", err)
	}

	runBubbleteaUI(versions, headVersion, *fileName, sm)
}

// Model represents the UI state
type Model struct {
	table       table.Model
	fileName    string
	headVersion string
	versions    []sqlc.Version
	sm          *storage.Manager
}

// Init initializes the model
func (m Model) Init() tea.Cmd {
	return nil
}

// Update handles user input and updates the model
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "enter":
			selectedRow := m.table.SelectedRow()
			if len(selectedRow) > 0 {
				selectedVersion := selectedRow[0]

				if selectedVersion != m.headVersion {
					ctx := context.Background()
					if err := m.sm.SetHead(ctx, m.fileName, selectedVersion); err == nil {
						m.headVersion = selectedVersion
						m.table = m.refreshTableModel()
						return m, nil
					}
				}
			}
		case "d":
			// Delete head pointer
			ctx := context.Background()
			if err := m.sm.DeleteHead(ctx, m.fileName); err == nil {
				m.headVersion = "" // Clear the head version
				m.table = m.refreshTableModel()
				return m, nil
			}
		}
	}
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

// View renders the current UI state
func (m Model) View() string {
	title := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFDF5")).
		Background(lipgloss.Color("#4A5568")).
		Padding(0, 1).
		Bold(true).
		Width(len(m.fileName) + 24).
		Render(fmt.Sprintf(" Version History for: %s ", m.fileName))

	helpText := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#626262")).
		Render("↑/↓: Navigate • enter: Set as head • d: Delete head • q: Quit")

	spacer := lipgloss.NewStyle().Height(1).Render("")

	return lipgloss.JoinVertical(
		lipgloss.Left,
		title,
		spacer,
		m.table.View(),
		spacer,
		helpText,
	)
}

func (m Model) refreshTableModel() table.Model {
	columns := []table.Column{
		{Title: "VERSION", Width: 20},
		{Title: "TIMESTAMP", Width: 30},
		{Title: "HEAD", Width: 5},
	}

	rows := make([]table.Row, len(m.versions))
	for i, v := range m.versions {
		headIndicator := ""
		if v.Tag == m.headVersion {
			headIndicator = "✓"
		}

		timestamp := "N/A"
		if v.CreatedAt.Valid {
			timestamp = v.CreatedAt.Time.Format("2006-01-02 15:04:05.000")
		}

		rows[i] = table.Row{v.Tag, timestamp, headIndicator}
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(10),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(true)
	s.Cell = s.Cell.
		PaddingLeft(1).
		PaddingRight(1)
	t.SetStyles(s)

	// Set the cursor to the head version if it exists
	if m.headVersion != "" {
		for i, v := range m.versions {
			if v.Tag == m.headVersion {
				t.SetCursor(i)
				break
			}
		}
	}

	return t
}

func runBubbleteaUI(versions []sqlc.Version, headVersion string, fileName string, sm *storage.Manager) {
	m := Model{
		fileName:    fileName,
		headVersion: headVersion,
		versions:    versions,
		sm:          sm,
	}

	m.table = m.refreshTableModel()

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error running UI: %v\n", err)

		fmt.Printf("Version history for file: %s\n", fileName)
		fmt.Printf("%-20s %-30s %s\n", "VERSION", "TIMESTAMP", "HEAD")
		fmt.Println(strings.Repeat("-", 60))

		for _, version := range versions {
			headIndicator := ""
			if version.Tag == headVersion {
				headIndicator = "<---"
			}
			timestamp := "N/A"
			if version.CreatedAt.Valid {
				timestamp = version.CreatedAt.Time.Format("2006-01-02 15:04:05.000")
			}
			fmt.Printf("%-20s %-30s %s\n", version.Tag, timestamp, headIndicator)
		}
	}
}

// newDB creates a new database connection
func newDB(log *log.Logger) *sql.DB {
	host := getEnvOrDefault("POSTGRES_HOST", "localhost")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")
	user := getEnvOrDefault("POSTGRES_USER", "postgres")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "quackfs")

	log.Debug("Using env vars", "host", host, "port", port, "user", user, "dbname", dbname)

	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Fatal("Failed to create database connection", "error", err)
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to connect to database", "error", err)
	}

	log.Info("Connected to PostgreSQL database")
	return db
}

// getEnvOrDefault returns the environment variable value or a default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
