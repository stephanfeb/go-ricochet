package opsapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// get issues a GET against the surface's mux without binding a socket.
func get(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func decodeReady(t *testing.T, rec *httptest.ResponseRecorder) readyResponse {
	t.Helper()
	var body readyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /readyz body: %v (body %q)", err, rec.Body.String())
	}
	return body
}

func TestHealthzIgnoresDependencies(t *testing.T) {
	s := New(Options{Checks: []Check{{
		Name: "postgres",
		Func: func(context.Context) error { return errors.New("connection refused") },
	}}})

	rec := get(t, s, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("liveness must stay 200 while a dependency is down, got %d", rec.Code)
	}

	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want ok", body.Status)
	}
	if body.UptimeSeconds < 0 {
		t.Errorf("uptime = %v, want non-negative", body.UptimeSeconds)
	}
}

func TestReadyzPassesWhenChecksPass(t *testing.T) {
	s := New(Options{Checks: []Check{{
		Name: "postgres",
		Func: func(context.Context) error { return nil },
	}}})

	rec := get(t, s, "/readyz")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200; body %s", rec.Code, rec.Body.String())
	}

	body := decodeReady(t, rec)
	if !body.Ready {
		t.Error("ready = false, want true")
	}
	if !body.Checks["postgres"].OK {
		t.Error("postgres check not reported as ok")
	}
}

// A failing dependency must produce a 503, not a 200 with a sad payload: a
// load balancer reads the status code and nothing else.
func TestReadyzFailsWhenACheckFails(t *testing.T) {
	s := New(Options{Checks: []Check{
		{Name: "postgres", Func: func(context.Context) error { return errors.New("connection refused") }},
		{Name: "other", Func: func(context.Context) error { return nil }},
	}})

	rec := get(t, s, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rec.Code)
	}

	body := decodeReady(t, rec)
	if body.Ready {
		t.Error("ready = true with a failing check")
	}
	if got := body.Checks["postgres"]; got.OK || !strings.Contains(got.Error, "connection refused") {
		t.Errorf("postgres check = %+v, want the underlying error surfaced", got)
	}
	if !body.Checks["other"].OK {
		t.Error("a passing check must still be reported as passing")
	}
}

// Draining is how an instance leaves rotation deliberately. It must flip
// readiness while liveness stays up, or the orchestrator kills the process
// mid-shutdown instead of letting it finish.
func TestDrainFlipsReadinessButNotLiveness(t *testing.T) {
	s := New(Options{Checks: []Check{{
		Name: "postgres",
		Func: func(context.Context) error { return nil },
	}}})

	if rec := get(t, s, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("pre-drain code = %d, want 200", rec.Code)
	}

	s.Drain()

	rec := get(t, s, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-drain code = %d, want 503", rec.Code)
	}
	body := decodeReady(t, rec)
	if !body.Draining {
		t.Error("draining flag not reported")
	}
	if !body.Checks["postgres"].OK {
		t.Error("a healthy dependency must still read as healthy while draining")
	}

	if rec := get(t, s, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("liveness code = %d during drain, want 200", rec.Code)
	}
}

// A hanging dependency must not hang the probe: the orchestrator's own timeout
// would then decide the verdict, which is a restart rather than a withdrawal.
func TestReadyzBoundsSlowChecks(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	s := New(Options{
		ReadinessTimeout: 50 * time.Millisecond,
		Checks: []Check{{
			Name: "postgres",
			Func: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-release:
					return nil
				}
			},
		}},
	})

	start := time.Now()
	rec := get(t, s, "/readyz")
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rec.Code)
	}
	if elapsed > time.Second {
		t.Fatalf("probe took %v, want it bounded near the 50ms readiness timeout", elapsed)
	}
}

func TestReadyzIncludesDetails(t *testing.T) {
	s := New(Options{
		Checks:  []Check{{Name: "postgres", Func: func(context.Context) error { return nil }}},
		Details: func() map[string]any { return map[string]any{"inFlight": 7} },
	})

	body := decodeReady(t, get(t, s, "/readyz"))
	if got := body.Details["inFlight"]; got != float64(7) {
		t.Errorf("details[inFlight] = %v (%T), want 7", got, got)
	}
}

// Details are diagnostic. They must never be able to mark a working instance
// unready, because saturation is the case they exist to describe.
func TestSaturationIsNotUnreadiness(t *testing.T) {
	s := New(Options{
		Checks: []Check{{Name: "postgres", Func: func(context.Context) error { return nil }}},
		Details: func() map[string]any {
			return map[string]any{"admission": map[string]any{"inFlight": 100, "maxInFlight": 100}}
		},
	})

	if rec := get(t, s, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200: a fully loaded instance is still serving", rec.Code)
	}
}

func TestReadyzWithNoChecksIsReady(t *testing.T) {
	if rec := get(t, New(Options{}), "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
}

func TestPprofOffByDefault(t *testing.T) {
	s := New(Options{})
	if rec := get(t, s, "/debug/pprof/"); rec.Code != http.StatusNotFound {
		t.Errorf("code = %d, want 404 when pprof is not enabled", rec.Code)
	}

	withPprof := New(Options{EnablePprof: true})
	if rec := get(t, withPprof, "/debug/pprof/"); rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200 when pprof is enabled", rec.Code)
	}
}

func TestMetricsMountedOnlyWhenHandlerProvided(t *testing.T) {
	if rec := get(t, New(Options{}), "/metrics"); rec.Code != http.StatusNotFound {
		t.Errorf("code = %d, want 404 with no metrics handler", rec.Code)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "# HELP up\n")
	})
	rec := get(t, New(Options{MetricsHandler: handler}), "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "# HELP up") {
		t.Errorf("body = %q, want the provided handler's output", rec.Body.String())
	}
}

func TestIndexListsMountedRoutes(t *testing.T) {
	s := New(Options{EnablePprof: true, MetricsHandler: http.NotFoundHandler()})
	rec := get(t, s, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	for _, want := range []string{"/healthz", "/readyz", "/metrics", "/debug/pprof/"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("index missing %s; body:\n%s", want, rec.Body.String())
		}
	}
}

func TestUnknownPathIs404(t *testing.T) {
	if rec := get(t, New(Options{}), "/nope"); rec.Code != http.StatusNotFound {
		t.Errorf("code = %d, want 404", rec.Code)
	}
}

func TestWritesAreRejected(t *testing.T) {
	s := New(Options{})
	for _, path := range []string{"/healthz", "/readyz", "/"} {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d, want 405", path, rec.Code)
		}
	}
}

// Start must bind before returning, and Addr must report the port actually
// chosen — otherwise an ephemeral port is unusable.
func TestStartBindsAndServes(t *testing.T) {
	var probes atomic.Int64
	s := New(Options{
		Bind: "127.0.0.1",
		Port: 0,
		Checks: []Check{{Name: "stub", Func: func(context.Context) error {
			probes.Add(1)
			return nil
		}}},
	})

	if err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	addr := s.Addr()
	if addr == "" || strings.HasSuffix(addr, ":0") {
		t.Fatalf("Addr() = %q, want the concrete bound address", addr)
	}

	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("code = %d, want 200", resp.StatusCode)
	}
	if probes.Load() != 1 {
		t.Errorf("checks ran %d times, want 1", probes.Load())
	}
}

func TestStartReportsBindFailure(t *testing.T) {
	first := New(Options{Bind: "127.0.0.1", Port: 0})
	if err := first.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = first.Shutdown(context.Background()) })

	_, port, _ := strings.Cut(first.Addr(), ":")
	portNum, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse port %q: %v", port, err)
	}

	second := New(Options{Bind: "127.0.0.1", Port: portNum})
	if err := second.Start(); err == nil {
		_ = second.Shutdown(context.Background())
		t.Fatal("start on an occupied port returned nil; a silent absence of health checks is worse than a startup failure")
	}
}

func TestShutdownBeforeStartIsSafe(t *testing.T) {
	if err := New(Options{}).Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown before start: %v", err)
	}
}

func TestDrainOnNilServerIsSafe(t *testing.T) {
	var s *Server
	s.Drain()
}

// ---------------------------------------------------------------------------
// Supplied routes
// ---------------------------------------------------------------------------

func echoHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	})
}

func TestSuppliedRoutesAreServedAndListed(t *testing.T) {
	s := New(Options{Routes: map[string]http.Handler{
		"/ops/storage": echoHandler("owners"),
		"/ops/limits":  echoHandler("knobs"),
	}})

	if rec := get(t, s, "/ops/storage"); rec.Code != http.StatusOK || rec.Body.String() != "owners" {
		t.Errorf("GET /ops/storage = %d %q", rec.Code, rec.Body.String())
	}
	if rec := get(t, s, "/ops/limits"); rec.Code != http.StatusOK || rec.Body.String() != "knobs" {
		t.Errorf("GET /ops/limits = %d %q", rec.Code, rec.Body.String())
	}

	// An endpoint nobody can discover is one nobody uses.
	index := get(t, s, "/").Body.String()
	for _, want := range []string{"/ops/storage", "/ops/limits"} {
		if !strings.Contains(index, want) {
			t.Errorf("index missing %s; body:\n%s", want, index)
		}
	}
}

// The surface's no-mutation rule has to hold for routes supplied from
// elsewhere too, or it is a convention rather than a property.
func TestSuppliedRoutesRejectWrites(t *testing.T) {
	var served atomic.Int64
	s := New(Options{Routes: map[string]http.Handler{
		"/ops/storage": http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			served.Add(1)
		}),
	}})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ops/storage", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /ops/storage = %d, want 405", rec.Code)
	}
	if served.Load() != 0 {
		t.Error("the handler ran on a POST; the wrapper is not applied")
	}
}

// A supplied route must never displace a built-in. Losing /healthz to a typo
// would look like a healthy server to every probe that could still reach it.
func TestSuppliedRoutesCannotShadowBuiltins(t *testing.T) {
	s := New(Options{
		MetricsHandler: echoHandler("real metrics"),
		Routes: map[string]http.Handler{
			"/healthz": echoHandler("hijacked"),
			"/metrics": echoHandler("hijacked"),
			"/":        echoHandler("hijacked"),
		},
	})

	rec := get(t, s, "/healthz")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "hijacked") {
		t.Errorf("/healthz was displaced: %d %q", rec.Code, rec.Body.String())
	}
	if body := get(t, s, "/metrics").Body.String(); body != "real metrics" {
		t.Errorf("/metrics = %q, want the supplied metrics handler", body)
	}
	if body := get(t, s, "/").Body.String(); strings.Contains(body, "hijacked") {
		t.Errorf("index was displaced: %q", body)
	}
}

func TestNoRoutesIsStillAValidSurface(t *testing.T) {
	s := New(Options{Routes: nil})
	if rec := get(t, s, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", rec.Code)
	}
}
