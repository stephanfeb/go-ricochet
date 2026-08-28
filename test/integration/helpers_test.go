package integration_test

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/metrics"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/protocol/maa"
	"github.com/twostack/go-ricochet/internal/protocol/mma"
	"github.com/twostack/go-ricochet/internal/protocol/msa"
	"github.com/twostack/go-ricochet/internal/protocol/sca"
	"github.com/twostack/go-ricochet/internal/protocol/sda"
	"github.com/twostack/go-ricochet/internal/protocol/sfa"
	"github.com/twostack/go-ricochet/internal/ratelimit"
	"github.com/twostack/go-ricochet/internal/storage"
	"github.com/twostack/go-ricochet/internal/storage/postgres"
	client "github.com/twostack/go-ricochet/pkg/client"
)

// testServer wraps a server-side libp2p host with all protocol handlers registered.
type testServer struct {
	Host     host.Host
	MDA      *mda.MailboxServer
	MTA      *mta.Router
	PeerID   peer.ID
	Config   *core.ServerConfig
	Metrics  *metrics.Metrics
	Capacity *capacity.Sampler
	Storage  *postgres.PostgresStorage
}

// newTestServer creates a fully wired server with PostgreSQL storage and all
// 4 protocol handlers (MSA, MAA, MMA, SDA). Requires RICOCHET_TEST_POSTGRES_DSN.
//
// Optional configure functions run after the defaults are set and before any
// service reads them, so a test can tighten rate limits or raise a cap.
func newTestServer(t *testing.T, configure ...func(*core.ServerConfig)) *testServer {
	t.Helper()

	dsn := os.Getenv("RICOCHET_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("RICOCHET_TEST_POSTGRES_DSN not set — skipping integration test")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Generate server identity.
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	h := createHost(t, priv)

	// Parse DSN into PostgresConfig.
	pgCfg := parsePostgresDSN(t, dsn)

	store, err := postgres.NewPostgresStorage(pgCfg, logger)
	if err != nil {
		h.Close()
		t.Fatalf("create storage: %v", err)
	}
	if err := store.InitializeWithConfig(ctx, pgCfg); err != nil {
		h.Close()
		t.Fatalf("initialize storage: %v", err)
	}

	// Server config. Rate limits are left at their defaults so the tests
	// exercise the limits a deployment actually runs with.
	cfg := core.DevelopmentConfig()
	cfg.MaxMessagesPerMailbox = 10000
	for _, apply := range configure {
		apply(cfg)
	}

	limiters := ratelimit.New(cfg.RateLimits)
	t.Cleanup(limiters.Close)
	admissionCtl := admission.New(cfg.Admission, pgCfg.PoolSize)

	// The aggregate sampler, wired as server.go wires it. Sampled on demand
	// by the tests rather than on a timer.
	sampler := capacity.New(store, cfg.MaxStorageBytes,
		postgres.DefaultNearCapacityRatio, logger)

	// Metrics are wired the way server.go wires them, so the integration
	// tests exercise the middleware rather than a pipeline that quietly has
	// none.
	met := metrics.New()
	if err := met.Register(metrics.NewCapacityCollector(sampler)); err != nil {
		t.Fatalf("register capacity collector: %v", err)
	}

	// Take the first sample, as Server.startServices does. Without it the
	// capacity handler correctly reports that it has nothing to report, which
	// is not the state a test wants to start from.
	if _, err := sampler.Sample(ctx); err != nil {
		t.Fatalf("initial capacity sample: %v", err)
	}
	if err := met.Register(metrics.NewAdmissionCollector(admissionCtl)); err != nil {
		t.Fatalf("register admission collector: %v", err)
	}
	if err := met.Register(metrics.NewPoolCollector(store.Pool())); err != nil {
		t.Fatalf("register pool collector: %v", err)
	}

	mdaSrv := mda.NewMailboxServer(store, logger)
	mtaRtr := mta.NewRouter(mdaSrv, limiters.MTA, logger)

	// Register protocol handlers — mirrors server.go registerProtocolHandlers.
	reg := forge.NewRegistry()
	reg.Provide("storage", storage.Storage(store))
	reg.Provide("mta", mtaRtr)
	reg.Provide("mda", mdaSrv)
	reg.Provide("config", cfg)
	reg.Provide(ratelimit.RegistryKey, limiters)
	reg.Provide(admission.RegistryKey, admissionCtl)
	reg.Provide(metrics.RegistryKey, met)
	reg.Provide(capacity.RegistryKey, sampler)
	pool := codec.NewBufferPool()
	if err := met.Register(metrics.NewBufferPoolCollector(pool)); err != nil {
		t.Fatalf("register buffer pool collector: %v", err)
	}

	msaPipeline := msa.NewPipeline(logger, pool, reg)
	h.SetStreamHandler(msa.ProtocolID, msaPipeline.StreamHandler())

	msaBatchPipeline := msa.NewBatchPipeline(logger, pool, reg)
	h.SetStreamHandler(msa.BatchProtocolID, msaBatchPipeline.StreamHandler())

	maaPipeline := maa.NewPipeline(logger, pool, reg)
	h.SetStreamHandler(maa.ProtocolID, maaPipeline.StreamHandler())

	mmaPipeline := mma.NewPipeline(logger, pool, reg)
	h.SetStreamHandler(mma.ProtocolID, mmaPipeline.StreamHandler())

	sdaPipeline := sda.NewPipeline(logger, pool, reg)
	h.SetStreamHandler(sda.ProtocolID, sdaPipeline.StreamHandler())

	sfaPipeline := sfa.NewPipeline(logger, pool, reg)
	h.SetStreamHandler(sfa.ProtocolID, sfaPipeline.StreamHandler())

	scaPipeline := sca.NewPipeline(logger, pool, reg)
	h.SetStreamHandler(sca.ProtocolID, scaPipeline.StreamHandler())

	ts := &testServer{
		Host:     h,
		MDA:      mdaSrv,
		MTA:      mtaRtr,
		PeerID:   h.ID(),
		Config:   cfg,
		Metrics:  met,
		Capacity: sampler,
		Storage:  store,
	}

	t.Cleanup(func() {
		mdaSrv.Close()
		h.Close()
	})

	return ts
}

// newTestClient creates a client connected to the given test server.
func newTestClient(t *testing.T, server *testServer) *client.Client {
	t.Helper()

	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	h := createHost(t, priv)

	// Add server addresses to the peerstore so the client can dial it.
	h.Peerstore().AddAddrs(server.PeerID, server.Host.Addrs(), time.Hour)

	cfg := client.Config{
		PreferredServers: []client.ServerPreference{
			{PeerID: server.PeerID, Priority: 1, Weight: 1},
		},
		ConnectionTimeout: 10 * time.Second,
		MessageTimeout:    30 * time.Second,
	}

	t.Cleanup(func() {
		h.Close()
	})

	return client.New(h, cfg)
}

// generatePeerID generates a random peer ID for use as a recipient.
func generatePeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer id from key: %v", err)
	}
	return id
}

// createHost builds a libp2p host with UDX transport on a random port.
func createHost(t *testing.T, priv crypto.PrivKey) host.Host {
	t.Helper()

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/udx"),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.ResourceManager(&network.NullResourceManager{}),
		libp2p.DisableRelay(),
	)
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	return h
}

// parsePostgresDSN parses a DSN URL into a PostgresConfig.
// Supports format: postgres://user:pass@host:port/dbname?sslmode=disable
func parsePostgresDSN(t *testing.T, dsn string) *core.PostgresConfig {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse postgres DSN: %v", err)
	}

	port := 5432
	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			t.Fatalf("parse postgres port: %v", err)
		}
	}

	password, _ := u.User.Password()
	dbName := ""
	if len(u.Path) > 1 {
		dbName = u.Path[1:] // strip leading /
	}

	sslMode := "disable"
	if sm := u.Query().Get("sslmode"); sm != "" {
		sslMode = sm
	}

	return &core.PostgresConfig{
		Host:           u.Hostname(),
		Port:           port,
		Database:       dbName,
		Username:       u.User.Username(),
		Password:       password,
		PoolSize:       5,
		SSLMode:        sslMode,
		ConnectTimeout: 10 * time.Second,
	}
}
