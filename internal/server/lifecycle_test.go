package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/storage/postgres"
)

// freeUDPPort returns a port nothing is bound to right now. UDX binds a
// plain UDP socket with no port reuse, so a server that leaked its host
// keeps the port and the next server on it fails to bind.
func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("pick udp port: %v", err)
	}
	port := c.LocalAddr().(*net.UDPAddr).Port
	c.Close()
	return port
}

// A Start that fails after storage is up releases the pool, the host and
// everything else it built. The old Start returned the error and walked
// away: a bad listen address left a live pool behind, and a busy operator
// port left the pool, the host and the maintenance goroutine behind.
func TestStartFailureReleasesWhatItBuilt(t *testing.T) {
	// Held for the whole test so the ops case fails to bind deterministically.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy ops port: %v", err)
	}
	defer busy.Close()
	busyPort := busy.Addr().(*net.TCPAddr).Port

	for _, tc := range []struct {
		name string
		fail func(cfg *core.ServerConfig, udxPort int)
	}{
		{
			// TEST-NET-1 is not a local address, so the host cannot bind
			// and Start fails between storage and the services.
			name: "p2p listen",
			fail: func(cfg *core.ServerConfig, _ int) {
				cfg.ListenAddresses = []string{"/ip4/192.0.2.1/udp/0/udx"}
			},
		},
		{
			// The operator surface is the last thing Start brings up, so
			// this failure has the most to unwind.
			name: "ops port in use",
			fail: func(cfg *core.ServerConfig, udxPort int) {
				cfg.ListenAddresses = []string{fmt.Sprintf("/ip4/127.0.0.1/udp/%d/udx", udxPort)}
				cfg.Ops.Enabled = true
				cfg.Ops.Bind = "127.0.0.1"
				cfg.Ops.Port = busyPort
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testServerConfig(t)
			udxPort := freeUDPPort(t)
			tc.fail(cfg, udxPort)

			srv := NewServer(cfg, slog.New(slog.NewTextHandler(&syncBuffer{}, nil)))
			if err := srv.Start(context.Background()); err == nil {
				srv.Stop()
				t.Fatal("Start succeeded; the test needs it to fail after storage is up")
			}
			if srv.IsRunning() {
				t.Fatal("a server whose Start failed reports running")
			}
			if srv.storage == nil {
				t.Fatal("Start failed before storage was built; the test proves nothing")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pool := srv.storage.(*postgres.PostgresStorage).Pool()
			if err := pool.Ping(ctx); err == nil {
				t.Fatal("the connection pool is still open after Start failed")
			}

			// Stop on a server that never ran is a no-op, not a second close.
			if err := srv.Stop(); err != nil {
				t.Fatalf("Stop after a failed Start: %v", err)
			}

			// A fresh server on the same UDX port proves the host let go of it.
			good := testServerConfig(t)
			good.ListenAddresses = []string{fmt.Sprintf("/ip4/127.0.0.1/udp/%d/udx", udxPort)}
			next := NewServer(good, slog.New(slog.NewTextHandler(&syncBuffer{}, nil)))
			if err := next.Start(context.Background()); err != nil {
				t.Fatalf("a fresh server could not start after the failed one: %v", err)
			}
			if err := next.Stop(); err != nil {
				t.Fatalf("stop fresh server: %v", err)
			}
		})
	}
}

// Stop and the maintenance goroutine share the running flag. Run under the
// race detector: with a plain bool this is a write in Stop racing a read on
// the maintenance tick.
func TestStopDoesNotRaceTheMaintenanceLoop(t *testing.T) {
	cfg := testServerConfig(t)
	cfg.CleanupInterval = 2 * time.Millisecond

	srv := NewServer(cfg, slog.New(slog.NewTextHandler(&syncBuffer{}, nil)))
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := srv.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if srv.IsRunning() {
		t.Fatal("server reports running after Stop")
	}
	// A second Stop does nothing and does not close anything twice.
	if err := srv.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}
