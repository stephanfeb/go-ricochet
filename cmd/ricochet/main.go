package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/gologshim"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/server"
)

func init() {
	// Bridge go-libp2p's gologshim to go-log/v2 so that SetLogLevel()
	// controls all libp2p subsystems (relay, swarm, etc.).
	gologshim.SetDefaultHandler(logging.SlogHandler())
}

// pgPasswordEnv names the environment variable the PostgreSQL password is
// read from. It is applied after the config file and before the CLI flags.
const pgPasswordEnv = "RICOCHET_PG_PASSWORD"

func main() {
	// Flags
	port := flag.Int("port", 0, "Listen port (default: 55223)")
	development := flag.Bool("development", false, "Use development configuration")
	production := flag.Bool("production", false, "Use production configuration")
	highCapacity := flag.Bool("high-capacity", false, "Use high-capacity configuration")
	dataDir := flag.String("data-dir", "", "Data directory path")
	identityFile := flag.String("identity-file", "", "Path to identity key file")

	// PostgreSQL flags
	pgHost := flag.String("pg-host", "", "PostgreSQL host")
	pgPort := flag.Int("pg-port", 0, "PostgreSQL port")
	pgDatabase := flag.String("pg-database", "", "PostgreSQL database")
	pgUsername := flag.String("pg-username", "", "PostgreSQL username")
	pgPassword := flag.String("pg-password", "", "PostgreSQL password (prefer the "+pgPasswordEnv+" environment variable: a flag is visible to every local user in the process list)")
	pgSSLMode := flag.String("pg-sslmode", "", "PostgreSQL SSL mode")
	externalAddrs := flag.String("external-addrs", "", "Comma-separated external multiaddrs to advertise (e.g., /ip4/1.2.3.4/udp/55223/udx)")
	configFile := flag.String("config", "", "Path to YAML config file (e.g., /etc/ricochet/config.yaml)")
	debugDHT := flag.Bool("debug-dht", false, "Enable verbose DHT debug logging")

	flag.Parse()

	// Build configuration: preset → config file → CLI overrides
	var cfg *core.ServerConfig
	presets := 0
	for _, on := range []bool{*development, *production, *highCapacity} {
		if on {
			presets++
		}
	}
	if presets > 1 {
		fmt.Fprintln(os.Stderr, "choose one of --development, --production, --high-capacity")
		os.Exit(2)
	}
	switch {
	case *development:
		cfg = core.DevelopmentConfig()
	case *production:
		cfg = core.ProductionConfig()
	case *highCapacity:
		cfg = core.HighCapacityConfig()
	default:
		cfg = core.DefaultConfig()
	}

	// Load YAML config file if specified (or check default location)
	cfgPath := *configFile
	if cfgPath == "" {
		// Check default location
		if _, err := os.Stat("/etc/ricochet/config.yaml"); err == nil {
			cfgPath = "/etc/ricochet/config.yaml"
		}
	}
	// A config file that cannot be read or parsed is fatal. Starting on the
	// preset instead would run the server with settings the operator did not
	// choose, announced by one warning line in a log nobody reads at boot.
	if cfgPath != "" {
		if err := core.LoadConfigFromFile(cfgPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "failed to load config file %s: %v\n", cfgPath, err)
			os.Exit(2)
		}
		fmt.Printf("Loaded config from %s\n", cfgPath)
	}

	// Apply CLI overrides (take precedence over config file)
	if *port > 0 {
		cfg.SetPort(*port)
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
	// The environment sits between the config file and the flags: a
	// deployment keeps the password out of the world-readable command line
	// by exporting it, and an explicit flag still wins for local runs.
	if pw := os.Getenv(pgPasswordEnv); pw != "" {
		cfg.Storage.Postgres.Password = pw
	}
	if *pgPassword != "" {
		cfg.Storage.Postgres.Password = *pgPassword
	}
	if *pgSSLMode != "" {
		cfg.Storage.Postgres.SSLMode = *pgSSLMode
	}
	if *externalAddrs != "" {
		cfg.ExternalAddresses = strings.Split(*externalAddrs, ",")
	}

	// Enable DHT debug logging if requested
	if *debugDHT {
		logging.SetLogLevel("dht", "debug")
		logging.SetLogLevel("dht/RtRefreshManager", "debug")
		logging.SetLogLevel("basichost", "info")
		logging.SetLogLevel("relay", "debug")
		logging.SetLogLevel("relaysvc", "debug")
	}

	// Setup logger
	level := slog.LevelInfo
	if *development {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// Startup banner
	fmt.Print(`
 ███████████   █████   █████████     ███████      █████████  █████   █████ ██████████ ███████████
░░███░░░░░███ ░░███   ███░░░░░███  ███░░░░░███   ███░░░░░███░░███   ░░███ ░░███░░░░░█░█░░░███░░░█
 ░███    ░███  ░███  ███     ░░░  ███     ░░███ ███     ░░░  ░███    ░███  ░███  █ ░ ░   ░███  ░
 ░██████████   ░███ ░███         ░███      ░███░███          ░███████████  ░██████       ░███
 ░███░░░░░███  ░███ ░███         ░███      ░███░███          ░███░░░░░███  ░███░░█       ░███
 ░███    ░███  ░███ ░░███     ███░░███     ███ ░░███     ███ ░███    ░███  ░███ ░   █    ░███
 █████   █████ █████ ░░█████████  ░░░███████░   ░░█████████  █████   █████ ██████████    █████
░░░░░   ░░░░░ ░░░░░   ░░░░░░░░░     ░░░░░░░      ░░░░░░░░░  ░░░░░   ░░░░░ ░░░░░░░░░░    ░░░░░

`)

	// Create and start server
	srv := server.NewServer(cfg, logger)

	if err := srv.Start(context.Background()); err != nil {
		logger.Error("failed to start server", "error", err)
		os.Exit(1)
	}

	fmt.Printf("Ricochet server running. Peer ID: %s\n", srv.PeerID())

	// Wait for a signal. The buffer holds the second one that forces exit.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	waitForShutdown(sigCh, logger, srv.Stop, os.Exit)
}
