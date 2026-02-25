package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/p2p"
	"github.com/twostack/go-ricochet/internal/presence"
	"github.com/twostack/go-ricochet/internal/protocol/maa"
	"github.com/twostack/go-ricochet/internal/protocol/mma"
	"github.com/twostack/go-ricochet/internal/protocol/msa"
	"github.com/twostack/go-ricochet/internal/protocol/sca"
	"github.com/twostack/go-ricochet/internal/protocol/sda"
	"github.com/twostack/go-ricochet/internal/protocol/sfa"
	"github.com/twostack/go-ricochet/internal/registry"
	"github.com/twostack/go-ricochet/internal/storage"
	"github.com/twostack/go-ricochet/internal/storage/postgres"
)

// Server is the main Ricochet store-and-forward server.
type Server struct {
	config *core.ServerConfig
	logger *slog.Logger

	// Core components
	host    host.Host
	node    *p2p.Node
	storage storage.Storage
	mdaSrv  *mda.MailboxServer
	mtaRtr  *mta.Router

	// Services
	registry        *registry.Registry
	presenceCache   *presence.Cache
	presenceMonitor *presence.Monitor
	presenceService *presence.Service

	// State
	isRunning bool
	startTime time.Time
}

// NewServer creates a new server instance.
func NewServer(cfg *core.ServerConfig, logger *slog.Logger) *Server {
	return &Server{
		config: cfg,
		logger: logger,
	}
}

// Start starts the server.
func (s *Server) Start(ctx context.Context) error {
	if s.isRunning {
		return fmt.Errorf("server already running")
	}

	s.logger.Info("starting Ricochet store-and-forward server")
	s.startTime = time.Now()

	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Initialize storage
	if err := s.initializeStorage(ctx); err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}

	// Initialize P2P
	if err := s.initializeP2P(ctx); err != nil {
		return fmt.Errorf("initialize p2p: %w", err)
	}

	// Initialize services (MDA, MTA, registry, presence)
	s.initializeServices(ctx)

	// Register protocol handlers
	s.registerProtocolHandlers()

	// Start background services
	s.startServices(ctx)

	s.isRunning = true

	peerID := s.host.ID()
	addrs := s.host.Addrs()
	s.logger.Info("server started",
		"peer_id", peerID.String(),
		"addrs", fmt.Sprintf("%v", addrs),
	)

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	if !s.isRunning {
		return nil
	}

	s.logger.Info("stopping server")
	s.isRunning = false

	// Stop services
	if s.presenceService != nil {
		s.presenceService.Stop()
	}
	if s.presenceMonitor != nil {
		s.presenceMonitor.StopMonitoring()
	}
	if s.registry != nil {
		if err := s.registry.Stop(); err != nil {
			s.logger.Warn("error stopping registry", "error", err)
		}
	}

	// Close P2P node (closes host, DHT, PubSub)
	if s.node != nil {
		if err := s.node.Close(); err != nil {
			s.logger.Warn("error closing p2p node", "error", err)
		}
	}

	// Close MDA (which closes storage)
	if s.mdaSrv != nil {
		if err := s.mdaSrv.Close(); err != nil {
			s.logger.Warn("error closing MDA", "error", err)
		}
	}

	s.logger.Info("server stopped")
	return nil
}

// PeerID returns the server's peer ID.
func (s *Server) PeerID() peer.ID {
	if s.host == nil {
		return ""
	}
	return s.host.ID()
}

// IsRunning returns whether the server is running.
func (s *Server) IsRunning() bool {
	return s.isRunning
}

func (s *Server) initializeStorage(ctx context.Context) error {
	s.logger.Info("initializing storage", "backend", s.config.Storage.Backend)

	if s.config.Storage.UsePostgres() {
		store, err := postgres.NewPostgresStorage(s.config.Storage.Postgres, s.logger)
		if err != nil {
			return err
		}
		if err := store.InitializeWithConfig(ctx, s.config.Storage.Postgres); err != nil {
			return err
		}
		s.storage = store
		s.logger.Info("PostgreSQL storage initialized")
	} else {
		return fmt.Errorf("unsupported storage backend: %s", s.config.Storage.Backend)
	}

	return nil
}

func (s *Server) initializeP2P(ctx context.Context) error {
	s.logger.Info("initializing P2P stack")

	// Load or create identity
	priv, err := s.loadIdentity()
	if err != nil {
		return fmt.Errorf("load identity: %w", err)
	}

	peerID, err := p2p.PeerIDFromIdentity(priv)
	if err != nil {
		return fmt.Errorf("derive peer id: %w", err)
	}
	s.logger.Info("loaded identity", "peer_id", peerID.String())

	// Create host
	h, err := p2p.CreateHost(s.config, priv, s.logger)
	if err != nil {
		return fmt.Errorf("create host: %w", err)
	}
	s.host = h

	// Create node (DHT + PubSub)
	node, err := p2p.NewNode(ctx, s.config, priv, h, s.logger)
	if err != nil {
		h.Close()
		return fmt.Errorf("create node: %w", err)
	}
	s.node = node

	s.logger.Info("P2P stack initialized")
	return nil
}

func (s *Server) initializeServices(ctx context.Context) {
	// Create MDA
	s.mdaSrv = mda.NewMailboxServer(s.storage, s.logger)
	s.logger.Info("MDA initialized")

	// Create MTA
	s.mtaRtr = mta.NewRouter(
		s.mdaSrv,
		s.config.RateLimitWindow,
		s.config.MaxRequestsPerWindow,
		s.logger,
	)
	s.logger.Info("MTA initialized")

	// Create push notifier
	if s.config.EnablePushDelivery {
		notifier := mda.NewNotifier(s.host, s.node, s.presenceMonitor, s.logger)
		s.mdaSrv.SetNotifier(notifier)
		s.logger.Info("push notifier initialized")
	}

	// Create service registry
	s.registry = registry.NewRegistry(s.node, s.config, s.host.ID(), s.logger)
	s.logger.Info("service registry initialized")

	// Create presence monitor
	if s.config.EnablePresenceMonitoring {
		s.presenceCache = presence.NewCache(30 * time.Second)
		s.presenceMonitor = presence.NewMonitor(s.presenceCache, s.host, s.logger)
		s.logger.Info("presence monitor initialized")
	}

	// Create presence broadcast service
	if s.config.EnablePresenceBroadcast {
		if s.presenceCache == nil {
			s.presenceCache = presence.NewCache(30 * time.Second)
		}
		presCfg := &presence.PresenceConfig{
			HeartbeatInterval: s.config.PresenceHeartbeatInterval,
			TimeoutDuration:   s.config.PresenceTimeoutDuration,
			BatchWindow:       s.config.PresenceBatchWindow,
			MaxBatchSize:      50,
			EnableBroadcast:   true,
		}
		s.presenceService = presence.NewService(s.host, s.node, s.presenceCache, presCfg, s.logger)
		s.logger.Info("presence broadcast service initialized")
	}
}

func (s *Server) registerProtocolHandlers() {
	// MSA — Mail Submission Agent (write path)
	msaHandler := msa.NewHandler(s.mtaRtr, s.logger)
	s.host.SetStreamHandler(msa.ProtocolID, msaHandler.HandleStream)
	s.logger.Info("registered MSA handler", "protocol", msa.ProtocolID)

	// MAA — Mail Access Agent (read path)
	maaHandler := maa.NewHandler(s.mdaSrv, s.logger)
	s.host.SetStreamHandler(maa.ProtocolID, maaHandler.HandleStream)
	s.logger.Info("registered MAA handler", "protocol", maa.ProtocolID)

	// MMA — Mailbox Management Agent (admin path)
	mmaHandler := mma.NewHandler(s.mdaSrv, s.config, s.logger)
	s.host.SetStreamHandler(mma.ProtocolID, mmaHandler.HandleStream)
	s.logger.Info("registered MMA handler", "protocol", mma.ProtocolID)

	// SDA — Store Document Access (document path)
	sdaHandler := sda.NewHandler(s.storage, s.logger)
	s.host.SetStreamHandler(sda.ProtocolID, sdaHandler.HandleStream)
	s.logger.Info("registered SDA handler", "protocol", sda.ProtocolID)

	// SFA — Store Feed Agent (feed path)
	sfaHandler := sfa.NewHandler(s.storage, s.logger)
	s.host.SetStreamHandler(sfa.ProtocolID, sfaHandler.HandleStream)
	s.logger.Info("registered SFA handler", "protocol", sfa.ProtocolID)

	// SCA — Store Collection Agent (collection path)
	scaHandler := sca.NewHandler(s.storage, s.logger)
	s.host.SetStreamHandler(sca.ProtocolID, scaHandler.HandleStream)
	s.logger.Info("registered SCA handler", "protocol", sca.ProtocolID)
}

func (s *Server) startServices(ctx context.Context) {
	// Start service registry
	if err := s.registry.Start(ctx); err != nil {
		s.logger.Warn("failed to start service registry", "error", err)
	} else {
		s.logger.Info("service registry started")
	}

	// Start presence broadcast service
	if s.presenceService != nil {
		if err := s.presenceService.Start(ctx); err != nil {
			s.logger.Warn("failed to start presence service", "error", err)
		} else {
			s.logger.Info("presence broadcast service started")
		}
	}

	// Log host advertised addresses (confirms relay service)
	addrs := s.host.Addrs()
	s.logger.Info("host advertised addresses", "addrs", fmt.Sprintf("%v", addrs))

	// Start periodic maintenance
	go s.maintenanceLoop(ctx)
}

func (s *Server) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isRunning {
				return
			}
			if err := s.mdaSrv.PerformMaintenance(ctx); err != nil {
				s.logger.Warn("maintenance error", "error", err)
			}
			if s.node != nil {
				s.node.LogDHTStatus()
			}
		}
	}
}

func (s *Server) loadIdentity() (crypto.PrivKey, error) {
	// 1. Environment variable (highest priority)
	if seedHex := os.Getenv("RICOCHET_SEED_HEX"); seedHex != "" {
		seed, err := hex.DecodeString(seedHex)
		if err != nil {
			return nil, fmt.Errorf("parse RICOCHET_SEED_HEX: %w", err)
		}
		return p2p.LoadIdentityFromSeed(seed)
	}

	// 2. Identity file from config
	if s.config.IdentityFile != "" {
		return p2p.LoadIdentityFromFile(s.config.IdentityFile)
	}

	// 3. Auto-generate in data directory
	identityPath := s.config.DataDirectory + "/peer_identity.key"
	return p2p.LoadOrCreateIdentity(identityPath)
}
