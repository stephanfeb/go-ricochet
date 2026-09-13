package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	"github.com/twostack/go-ricochet/internal/core"
	client "github.com/twostack/go-ricochet/pkg/client"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testServerConfig is a development config pointed at the test database,
// listening on an ephemeral loopback port with every optional network
// service off, so the test exercises the request path and nothing else.
func testServerConfig(t *testing.T) *core.ServerConfig {
	t.Helper()
	dsn := os.Getenv("RICOCHET_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("RICOCHET_TEST_POSTGRES_DSN not set — skipping server test")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	port := 5432
	if u.Port() != "" {
		if port, err = strconv.Atoi(u.Port()); err != nil {
			t.Fatalf("parse port: %v", err)
		}
	}
	password, _ := u.User.Password()

	cfg := core.DevelopmentConfig()
	cfg.ListenAddresses = []string{"/ip4/127.0.0.1/udp/0/udx"}
	cfg.DataDirectory = t.TempDir()
	cfg.Storage.Postgres.Host = u.Hostname()
	cfg.Storage.Postgres.Port = port
	cfg.Storage.Postgres.Database = strings.TrimPrefix(u.Path, "/")
	cfg.Storage.Postgres.Username = u.User.Username()
	cfg.Storage.Postgres.Password = password
	cfg.Storage.Postgres.SSLMode = "disable"
	cfg.Ops.Enabled = false
	cfg.EnableRelay = false
	cfg.EnableRelayService = false
	cfg.EnableAutoRelay = false
	cfg.EnableHolePunching = false
	cfg.EnableAutoNAT = false
	cfg.EnablePresenceMonitoring = false
	cfg.EnablePresenceBroadcast = false
	cfg.EnablePushDelivery = false
	cfg.MaxMessagesPerMailbox = 100000
	return cfg
}

func testClient(t *testing.T, srv *Server) *client.Client {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
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
		t.Fatalf("create client host: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	host := srv.forgeServer.Host()
	h.Peerstore().AddAddrs(host.ID(), host.Addrs(), time.Hour)
	return client.New(h, client.Config{
		PreferredServers:  []client.ServerPreference{{PeerID: host.ID(), Priority: 1, Weight: 1}},
		ConnectionTimeout: 10 * time.Second,
		MessageTimeout:    30 * time.Second,
	})
}

// A request in flight when Stop is called completes, or is refused with a
// 503, and never sees storage disappear underneath it. Stop used to cancel
// the context and close the pool with requests mid-query: a client got an
// internal error or a reset stream, and the server logged pool-closed errors
// for work it had already accepted.
func TestStopDrainsInFlightRequests(t *testing.T) {
	cfg := testServerConfig(t)
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	srv := NewServer(cfg, logger)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			srv.Stop()
		}
	})

	cl := testClient(t, srv)
	self := cl.PeerID()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// One request on its own proves the server is up before the burst.
	if res, err := cl.SendMessage(ctx, self, []byte("hello")); err != nil || !res.Success {
		t.Fatalf("first send: err=%v success=%v", err, res != nil && res.Success)
	}

	const senders = 8
	var (
		wg        sync.WaitGroup
		succeeded atomic.Int64
		refused   atomic.Int64
		transport atomic.Int64
		mu        sync.Mutex
		bad       []error
		halt      atomic.Bool
	)
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !halt.Load() {
				res, err := cl.SendMessage(ctx, self, []byte("in flight"))
				switch {
				case err != nil:
					// The host is gone: the stream could not be opened or
					// was reset. That is the end of the server, not a
					// request it accepted and then abandoned.
					transport.Add(1)
					return
				case res.Success:
					succeeded.Add(1)
				case errors.Is(res.Err(), client.ErrOverloaded):
					refused.Add(1)
				default:
					mu.Lock()
					bad = append(bad, res.Err())
					mu.Unlock()
					return
				}
			}
		}()
	}

	// Let the burst reach a steady state, then stop with requests in flight.
	time.Sleep(200 * time.Millisecond)
	if err := srv.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	stopped = true
	halt.Store(true)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("senders did not return after Stop; a request was left hanging")
	}

	t.Logf("succeeded=%d refused(503)=%d transport=%d bad=%d",
		succeeded.Load(), refused.Load(), transport.Load(), len(bad))
	if len(bad) > 0 {
		t.Errorf("requests failed with something other than success or 503: %v", bad)
	}
	if succeeded.Load() == 0 {
		t.Error("no request succeeded during the burst")
	}
	if refused.Load() == 0 {
		t.Error("no request was refused with a 503 while draining; Stop did not drain")
	}

	out := logs.String()
	if !strings.Contains(out, "in-flight requests drained") {
		t.Error("server never reported draining its in-flight requests")
	}
	for _, line := range strings.Split(out, "\n") {
		for _, needle := range []string{"closed pool", "conn closed"} {
			if strings.Contains(line, needle) {
				t.Errorf("server log mentions %q: storage was closed under work it had accepted:\n%s", needle, line)
			}
		}
	}
}
