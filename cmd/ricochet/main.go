package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/server"
)

func main() {
	// Flags
	port := flag.Int("port", 0, "Listen port (default: 55223)")
	development := flag.Bool("development", false, "Use development configuration")
	production := flag.Bool("production", false, "Use production configuration")
	dataDir := flag.String("data-dir", "", "Data directory path")
	identityFile := flag.String("identity-file", "", "Path to identity key file")

	// PostgreSQL flags
	pgHost := flag.String("pg-host", "", "PostgreSQL host")
	pgPort := flag.Int("pg-port", 0, "PostgreSQL port")
	pgDatabase := flag.String("pg-database", "", "PostgreSQL database")
	pgUsername := flag.String("pg-username", "", "PostgreSQL username")
	pgPassword := flag.String("pg-password", "", "PostgreSQL password")
	pgSSLMode := flag.String("pg-sslmode", "", "PostgreSQL SSL mode")

	flag.Parse()

	// Build configuration
	var cfg *core.ServerConfig
	if *development {
		cfg = core.DevelopmentConfig()
	} else if *production {
		cfg = core.ProductionConfig()
	} else {
		cfg = core.DefaultConfig()
	}

	// Apply CLI overrides
	if *port > 0 {
		cfg.Port = *port
	}
	if *dataDir != "" {
		cfg.DataDirectory = *dataDir
	}
	if *identityFile != "" {
		cfg.IdentityFile = *identityFile
	}
	if *pgHost != "" {
		cfg.Storage.Postgres.Host = *pgHost
	}
	if *pgPort > 0 {
		cfg.Storage.Postgres.Port = *pgPort
	}
	if *pgDatabase != "" {
		cfg.Storage.Postgres.Database = *pgDatabase
	}
	if *pgUsername != "" {
		cfg.Storage.Postgres.Username = *pgUsername
	}
	if *pgPassword != "" {
		cfg.Storage.Postgres.Password = *pgPassword
	}
	if *pgSSLMode != "" {
		cfg.Storage.Postgres.SSLMode = *pgSSLMode
	}

	// Setup logger
	level := slog.LevelInfo
	if *development {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// Create and start server
	srv := server.NewServer(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		logger.Error("failed to start server", "error", err)
		os.Exit(1)
	}

	fmt.Printf("Ricochet server running. Peer ID: %s\n", srv.PeerID())

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	if err := srv.Stop(); err != nil {
		logger.Error("error during shutdown", "error", err)
		os.Exit(1)
	}
}
