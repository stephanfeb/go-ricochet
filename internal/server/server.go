package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	forgehost "github.com/twostack/go-p2p-forge/host"
	"github.com/twostack/go-p2p-forge/node"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/metrics"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/opsapi"
	"github.com/twostack/go-ricochet/internal/presence"
	"github.com/twostack/go-ricochet/internal/protocol/maa"
	"github.com/twostack/go-ricochet/internal/protocol/mma"
	"github.com/twostack/go-ricochet/internal/protocol/msa"
	"github.com/twostack/go-ricochet/internal/protocol/sca"
	"github.com/twostack/go-ricochet/internal/protocol/sda"
	"github.com/twostack/go-ricochet/internal/protocol/sfa"
	"github.com/twostack/go-ricochet/internal/ratelimit"
	"github.com/twostack/go-ricochet/internal/registry"
	"github.com/twostack/go-ricochet/internal/storage"
	"github.com/twostack/go-ricochet/internal/storage/postgres"
)

// Server is the main Ricochet store-and-forward server.
type Server struct {
	config *core.ServerConfig
	logger *slog.Logger

	// Core components
	forgeServer *forge.Server
	storage     storage.Storage
	mdaSrv      *mda.MailboxServer
	mtaRtr      *mta.Router
	limiters    *ratelimit.Limiters
	admission   *admission.Controller
	metrics     *metrics.Metrics
	capacity    *capacity.Sampler
	opsSrv      *opsapi.Server

	// bufferPool is shared by every pipeline. It is a field rather than a
	// local so its hit rate can be published.
	bufferPool *codec.BufferPool

	// Services
	registry        *registry.Registry
	presenceCache   *presence.Cache
	presenceMonitor *presence.Monitor
	presenceService *presence.Service

	// State
	ctx       context.Context
	cancel    context.CancelFunc
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
func (s *Server) Start(parentCtx context.Context) error {
	if s.isRunning {
		return fmt.Errorf("server already running")
	}

	s.ctx, s.cancel = context.WithCancel(parentCtx)

	s.logger.Info("starting Ricochet store-and-forward server")
	s.startTime = time.Now()

	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Initialize storage
	if err := s.initializeStorage(s.ctx); err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}

	// Build and start forge server (host + node)
	if err := s.initializeP2P(s.ctx); err != nil {
		return fmt.Errorf("initialize p2p: %w", err)
	}

	// Initialize services (MDA, MTA, registry, presence) — needs Host/Node
	s.initializeServices(s.ctx)

	// Register server-level singletons for pipeline handler DI
	s.forgeServer.Provide("storage", s.storage)
	s.forgeServer.Provide("mda", s.mdaSrv)
	s.forgeServer.Provide("mta", s.mtaRtr)
	s.forgeServer.Provide("config", s.config)
	s.forgeServer.Provide(ratelimit.RegistryKey, s.limiters)
	s.forgeServer.Provide(admission.RegistryKey, s.admission)
	s.forgeServer.Provide(metrics.RegistryKey, s.metrics)
	s.forgeServer.Provide(capacity.RegistryKey, s.capacity)

	// Register protocol handlers
	s.registerProtocolHandlers()

	// Start background services
	s.startServices(s.ctx)

	// Operator HTTP surface. Started last so /readyz only reports ready once
	// everything it vouches for is actually up.
	if err := s.startOpsAPI(); err != nil {
		return fmt.Errorf("start ops api: %w", err)
	}

	s.isRunning = true

	peerID := s.forgeServer.PeerID()
	addrs := s.forgeServer.Host().Addrs()
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

	// Withdraw from load balancing before anything is torn down, so traffic
	// stops arriving while the instance can still serve what it has. The delay
	// is what gives a load balancer time to observe the 503; without it the
	// instance vanishes in the same instant it announces its withdrawal.
	if s.opsSrv != nil {
		s.opsSrv.Drain()
		if delay := s.config.Ops.DrainDelay; delay > 0 {
			s.logger.Info("draining before shutdown", "delay", delay)
			time.Sleep(delay)
		}
	}

	// Cancel context first so background goroutines exit promptly
	if s.cancel != nil {
		s.cancel()
	}

	// Stop services
	if s.presenceService != nil {
		if err := s.presenceService.Stop(); err != nil {
			s.logger.Warn("error stopping presence service", "error", err)
		}
	}
	if s.presenceMonitor != nil {
		s.presenceMonitor.StopMonitoring()
	}
	if s.registry != nil {
		if err := s.registry.Stop(); err != nil {
			s.logger.Warn("error stopping registry", "error", err)
		}
	}

	// Stop forge server (closes node + host)
	if s.forgeServer != nil {
		s.forgeServer.Stop()
	}

	// Stop rate limiter eviction goroutines
	if s.limiters != nil {
		s.limiters.Close()
	}

	// Stop the operator surface before storage closes, so /readyz never
	// answers with a pool that has already gone away.
	if s.opsSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.config.Ops.EffectiveShutdownTimeout())
		if err := s.opsSrv.Shutdown(ctx); err != nil {
			s.logger.Warn("error stopping ops server", "error", err)
		}
		cancel()
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
	if s.forgeServer == nil {
		return ""
	}
	return s.forgeServer.PeerID()
}

// IsRunning returns whether the server is running.
func (s *Server) IsRunning() bool {
	return s.isRunning
}

// ForgeServer returns the underlying forge server (available after Start).
func (s *Server) ForgeServer() *forge.Server {
	return s.forgeServer
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

	peerID, err := forgehost.PeerIDFromIdentity(priv)
	if err != nil {
		return fmt.Errorf("derive peer id: %w", err)
	}
	s.logger.Info("loaded identity", "peer_id", peerID.String())

	// Build forge config from ricochet config
	forgeCfg := s.buildForgeConfig()

	// Create forge server
	s.forgeServer = forge.NewServer(
		forge.WithConfig(forgeCfg),
		forge.WithIdentity(priv),
		forge.WithLogger(s.logger),
		forge.WithTransport(libp2p.Transport(udxtransport.NewTransport)),
	)

	// Start forge server (creates host + node)
	if err := s.forgeServer.Start(ctx); err != nil {
		return fmt.Errorf("start forge server: %w", err)
	}

	s.logger.Info("P2P stack initialized")
	return nil
}

func (s *Server) buildForgeConfig() *forge.Config {
	cfg := forge.DefaultConfig()

	// Network
	cfg.Host.Port = s.config.Port
	cfg.Host.ListenAddresses = s.config.ListenAddresses
	cfg.Host.ExternalAddresses = s.config.ExternalAddresses
	cfg.Host.BootstrapPeers = s.config.BootstrapPeers

	// Yamux tuning for mobile clients.
	// KeepAlive: how often to ping idle connections. Lower = faster dead-peer detection
	// but more bandwidth. 15s is aggressive enough for mobile while still battery-friendly.
	// WriteTimeout: max time for a single write (including yamux frames). The sendLoop
	// blocks on writes, preventing ANY new stream opens until the write completes or
	// times out. 10s ensures dead connections unblock the sendLoop quickly.
	// Total dead-connection detection: KeepAlive + WriteTimeout = 25s worst case.
	cfg.Host.YamuxKeepAlive = 15 * time.Second
	cfg.Host.YamuxWriteTimeout = 10 * time.Second

	// Relay
	cfg.Host.EnableRelay = s.config.EnableRelay
	cfg.Host.EnableRelayService = s.config.EnableRelayService
	cfg.Host.EnableAutoRelay = s.config.EnableAutoRelay
	cfg.Host.EnableHolePunching = s.config.EnableHolePunching
	cfg.Host.EnableAutoNAT = s.config.EnableAutoNAT

	// Relay limits
	rl := s.config.RelayLimits
	cfg.Host.RelayLimits = forgehost.RelayLimits{
		MaxReservations:        rl.MaxReservations,
		MaxCircuits:            rl.MaxCircuits,
		BufferSize:             rl.BufferSize,
		MaxReservationsPerPeer: rl.MaxReservationsPerPeer,
		MaxReservationsPerIP:   rl.MaxReservationsPerIP,
		MaxReservationsPerASN:  rl.MaxReservationsPerASN,
		ReservationTTL:         rl.ReservationTTL,
		ConnectionDuration:     rl.ConnectionDuration,
		ConnectionData:         rl.ConnectionData,
	}

	// Node
	cfg.Node.DHTMode = node.DHTModeServer
	cfg.Node.BootstrapPeers = s.config.BootstrapPeers
	cfg.Node.EnablePubSub = true

	// Identity / Data
	cfg.IdentityFile = s.config.IdentityFile
	cfg.DataDirectory = s.config.DataDirectory

	return cfg
}

func (s *Server) initializeServices(ctx context.Context) {
	// Metrics and the shared buffer pool come first: the pipelines read the
	// metrics out of the forge registry when they are built, and a nil there
	// would silently produce a server that publishes no request series.
	s.bufferPool = codec.NewBufferPool()
	if s.config.EnableMetrics {
		s.metrics = metrics.New()
	}

	// Build the per-protocol rate limiters before anything that uses them.
	s.limiters = ratelimit.New(s.config.RateLimits)
	s.logger.Info("rate limiters initialized",
		"window", s.config.RateLimits.EffectiveWindow(),
	)

	// Admission control governs throughput; the rate limiters above are an
	// optional per-tenant cap and are off unless configured.
	poolSize := 0
	if s.config.Storage.Postgres != nil {
		poolSize = s.config.Storage.Postgres.PoolSize
	}
	s.admission = admission.New(s.config.Admission, poolSize)
	if s.admission != nil {
		s.logger.Info("admission control initialized",
			"maxInFlight", s.config.Admission.EffectiveMaxInFlight(poolSize),
			"maxInFlightPerPeer", s.config.Admission.MaxInFlightPerPeer,
			"acquireTimeout", s.config.Admission.EffectiveAcquireTimeout(),
		)
	} else {
		s.logger.Warn("admission control disabled — throughput is unbounded and the database is unprotected")
	}

	// The aggregate sampler answers capacity questions. It scans, so it is
	// refreshed on the maintenance tick rather than per request.
	s.capacity = capacity.New(s.storage, s.config.MaxStorageBytes,
		postgres.DefaultNearCapacityRatio, s.logger)

	// Publish the live counters admission control, the connection pool and
	// the buffer pool already keep. Registered here, after the controller
	// exists, so a scrape never reads a half-built server.
	s.registerCollectors()

	// Create MDA
	s.mdaSrv = mda.NewMailboxServer(s.storage, s.logger)
	s.logger.Info("MDA initialized")

	// Create MTA
	s.mtaRtr = mta.NewRouter(s.mdaSrv, s.limiters.MTA, s.logger)
	s.logger.Info("MTA initialized")

	// Create push notifier
	if s.config.EnablePushDelivery {
		notifier := mda.NewNotifier(s.forgeServer.Host(), s.forgeServer.Node(), s.presenceMonitor, s.logger)
		s.mdaSrv.SetNotifier(notifier)
		s.logger.Info("push notifier initialized")
	}

	// Create service registry
	s.registry = registry.NewRegistry(s.forgeServer.Node(), s.config, s.forgeServer.PeerID(), s.logger)
	s.logger.Info("service registry initialized")

	// Create presence monitor
	if s.config.EnablePresenceMonitoring {
		s.presenceCache = presence.NewCache(30 * time.Second)
		s.presenceMonitor = presence.NewMonitor(s.presenceCache, s.forgeServer.Host(), s.logger)
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
		s.presenceService = presence.NewService(s.forgeServer.Host(), s.forgeServer.Node(), s.presenceCache, presCfg, s.logger)
		s.logger.Info("presence broadcast service initialized")
	}
}

func (s *Server) registerProtocolHandlers() {
	pool := s.bufferPool
	reg := s.forgeServer.Registry()
	h := s.forgeServer.Host()

	// NOTE: Handlers are registered directly on the host because
	// forge.Server.Start() has already been called (we need Host/Node
	// available for service initialization before handler registration).
	// forge.Server.Handle() only stores handlers for Start() to register,
	// so post-Start registration must go through the host directly.

	// MSA — Mail Submission Agent (write path)
	msaPipeline := msa.NewPipeline(s.logger, pool, reg)
	h.SetStreamHandler(msa.ProtocolID, msaPipeline.StreamHandler())
	s.logger.Info("registered MSA handler", "protocol", msa.ProtocolID)

	// MSA batch — many submissions in one request
	msaBatchPipeline := msa.NewBatchPipeline(s.logger, pool, reg)
	h.SetStreamHandler(msa.BatchProtocolID, msaBatchPipeline.StreamHandler())
	s.logger.Info("registered MSA batch handler", "protocol", msa.BatchProtocolID)

	// MAA — Mail Access Agent (read path)
	maaPipeline := maa.NewPipeline(s.logger, pool, reg)
	h.SetStreamHandler(maa.ProtocolID, maaPipeline.StreamHandler())
	s.logger.Info("registered MAA handler", "protocol", maa.ProtocolID)

	// MMA — Mailbox Management Agent (admin path)
	mmaPipeline := mma.NewPipeline(s.logger, pool, reg)
	h.SetStreamHandler(mma.ProtocolID, mmaPipeline.StreamHandler())
	s.logger.Info("registered MMA handler", "protocol", mma.ProtocolID)

	// SDA — Store Document Access (document path)
	sdaPipeline := sda.NewPipeline(s.logger, pool, reg)
	h.SetStreamHandler(sda.ProtocolID, sdaPipeline.StreamHandler())
	s.logger.Info("registered SDA handler", "protocol", sda.ProtocolID)

	// SFA — Store Feed Agent (feed path)
	sfaPipeline := sfa.NewPipeline(s.logger, pool, reg)
	h.SetStreamHandler(sfa.ProtocolID, sfaPipeline.StreamHandler())
	s.logger.Info("registered SFA handler", "protocol", sfa.ProtocolID)

	// SCA — Store Collection Agent (collection path)
	scaPipeline := sca.NewPipeline(s.logger, pool, reg)
	h.SetStreamHandler(sca.ProtocolID, scaPipeline.StreamHandler())
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
	addrs := s.forgeServer.Host().Addrs()
	s.logger.Info("host advertised addresses", "addrs", fmt.Sprintf("%v", addrs))

	// Start periodic maintenance
	go s.maintenanceLoop(ctx)

	// Take the first capacity sample immediately, in the background. Until it
	// lands, queryCapacity reports that it has nothing rather than returning
	// zeroes; waiting a whole cleanup interval for that to clear would be a
	// long time to look empty. It runs off the startup path because on a large
	// database the scan is not instant.
	go s.sampleCapacity(ctx)
}

// sampleCapacity refreshes the aggregate storage sample. A failure is logged
// and the previous sample kept: stale figures carry their age, so a widening
// gap is visible, whereas discarding them would leave the operator with
// nothing at the moment the database is unhappy.
func (s *Server) sampleCapacity(ctx context.Context) {
	if s.capacity == nil {
		return
	}

	stats, err := s.capacity.Sample(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("capacity sample failed", "error", err)
		}
		return
	}

	s.logger.Debug("capacity sampled",
		"mailboxes", stats.Mailboxes,
		"messages", stats.Messages,
		"databaseBytes", stats.DatabaseBytes,
		"nearCapacity", stats.MailboxesNearCapacity,
	)
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
			s.sampleCapacity(ctx)
			if s.forgeServer.Node() != nil {
				s.forgeServer.Node().LogDHTStatus()
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
		return forgehost.LoadIdentityFromSeed(seed)
	}

	// 2. Identity file from config
	if s.config.IdentityFile != "" {
		return forgehost.LoadIdentityFromFile(s.config.IdentityFile)
	}

	// 3. Auto-generate in data directory
	identityPath := s.config.DataDirectory + "/peer_identity.key"
	return forgehost.LoadOrCreateIdentity(identityPath)
}
