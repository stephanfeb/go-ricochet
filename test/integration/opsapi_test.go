package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/opsapi"
	"github.com/stephanfeb/go-ricochet/internal/storage/postgres"
)

// newOpsSurface builds the operator surface over a real PostgreSQL pool, wired
// the same way internal/server does: one readiness check that pings the pool.
//
// The unit tests in internal/opsapi cover the HTTP contract with stub checks.
// This exists to verify the part a stub cannot: that a genuinely unreachable
// database actually makes pgx's Ping fail, rather than succeeding from a
// cached connection and leaving /readyz reporting a server that cannot serve.
func newOpsSurface(t *testing.T) (*opsapi.Server, *postgres.PostgresStorage) {
	t.Helper()

	dsn := os.Getenv("RICOCHET_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("RICOCHET_TEST_POSTGRES_DSN not set — skipping integration test")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pgCfg := parsePostgresDSN(t, dsn)

	store, err := postgres.NewPostgresStorage(pgCfg, logger)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	if err := store.InitializeWithConfig(context.Background(), pgCfg); err != nil {
		t.Fatalf("initialize storage: %v", err)
	}

	srv := opsapi.New(opsapi.Options{
		Bind:             "127.0.0.1",
		Port:             0, // ephemeral, so parallel tests do not clash
		ReadinessTimeout: 2 * time.Second,
		Logger:           logger,
		Checks: []opsapi.Check{{
			Name: "postgres",
			Func: func(ctx context.Context) error { return store.Pool().Ping(ctx) },
		}},
		Details: func() map[string]any {
			stat := store.Pool().Stat()
			return map[string]any{"postgresPool": map[string]any{
				"totalConns": stat.TotalConns(),
				"maxConns":   stat.MaxConns(),
			}}
		},
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("start ops server: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = store.Close()
	})

	return srv, store
}

func opsGet(t *testing.T, srv *opsapi.Server, path string) (int, map[string]any) {
	t.Helper()

	resp, err := http.Get("http://" + srv.Addr() + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()

	body := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return resp.StatusCode, body
}

// The acceptance criterion for B0: the endpoints answer over a real socket,
// and losing PostgreSQL flips readiness without touching liveness or the
// process.
func TestOpsSurfaceReportsDatabaseLoss(t *testing.T) {
	srv, store := newOpsSurface(t)

	code, ready := opsGet(t, srv, "/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d with a healthy database, want 200 (body %v)", code, ready)
	}
	if ready["ready"] != true {
		t.Errorf("ready = %v, want true", ready["ready"])
	}
	if _, ok := ready["details"].(map[string]any)["postgresPool"]; !ok {
		t.Errorf("readiness body carries no pool figures: %v", ready)
	}

	code, health := opsGet(t, srv, "/healthz")
	if code != http.StatusOK || health["status"] != "ok" {
		t.Fatalf("/healthz = %d %v, want 200 ok", code, health)
	}

	// Take the database away. Closing the pool is the in-process equivalent of
	// the server going down: every subsequent acquire fails.
	store.Pool().Close()

	code, ready = opsGet(t, srv, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d with the pool closed, want 503 (body %v)", code, ready)
	}
	if ready["ready"] != false {
		t.Errorf("ready = %v, want false", ready["ready"])
	}
	if check, ok := ready["checks"].(map[string]any)["postgres"].(map[string]any); !ok || check["ok"] != false {
		t.Errorf("postgres check not reported as failing: %v", ready["checks"])
	}

	// Liveness must survive it. A liveness probe that fails on a database
	// outage gets the process restarted for a fault a restart cannot fix.
	code, health = opsGet(t, srv, "/healthz")
	if code != http.StatusOK {
		t.Errorf("/healthz = %d after the database went away, want 200 (body %v)", code, health)
	}
}

// Draining must be visible over the wire while the listener stays up: a load
// balancer needs a 503 it can act on, not a refused connection.
func TestOpsSurfaceDrains(t *testing.T) {
	srv, _ := newOpsSurface(t)

	if code, body := opsGet(t, srv, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz = %d before drain, want 200 (body %v)", code, body)
	}

	srv.Drain()

	code, body := opsGet(t, srv, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d after drain, want 503", code)
	}
	if body["draining"] != true {
		t.Errorf("draining = %v, want true", body["draining"])
	}
	if check, ok := body["checks"].(map[string]any)["postgres"].(map[string]any); !ok || check["ok"] != true {
		t.Errorf("a healthy database must still read as healthy while draining: %v", body["checks"])
	}

	if code, _ := opsGet(t, srv, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d during drain, want 200", code)
	}
}

// Shutdown must actually stop serving, and must be safe to call while the
// surface is up.
func TestOpsSurfaceShutdownStopsServing(t *testing.T) {
	srv, _ := newOpsSurface(t)
	addr := srv.Addr()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
		resp.Body.Close()
		t.Fatalf("still serving on %s after shutdown", addr)
	}
}
