// Package opsapi serves the operator HTTP surface.
//
// It exists because nothing in this server could previously be observed from
// outside it: there was no health check, no readiness signal, and no way for a
// load balancer to tell a draining instance from a healthy one. The surface is
// deliberately separate from the libp2p protocols — it needs no peer identity,
// no handshake, and no client library, so it works with curl and with every
// orchestrator's existing probe configuration.
//
// It binds to loopback unless told otherwise. Readiness details, pprof
// profiles and metrics all disclose internal state, so exposing them beyond
// the host is an explicit choice.
package opsapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"
)

// Check is one named readiness dependency. Func returns nil when the
// dependency is usable.
type Check struct {
	Name string
	Func func(context.Context) error
}

// Options configures the operator surface. The zero value is not useful;
// build one from core.OpsConfig.
type Options struct {
	// Bind and Port are the listen address. Port 0 binds an ephemeral port.
	Bind string
	Port int

	// Checks are consulted by /readyz, in order. All must pass.
	Checks []Check

	// Details, when set, contributes free-form diagnostic state to the
	// /readyz body. It must not block and does not affect the verdict.
	Details func() map[string]any

	// MetricsHandler is mounted at /metrics when non-nil.
	MetricsHandler http.Handler

	// EnablePprof mounts /debug/pprof.
	EnablePprof bool

	// ReadinessTimeout bounds the whole readiness evaluation.
	ReadinessTimeout time.Duration

	Logger *slog.Logger
}

// Server is the operator HTTP surface.
type Server struct {
	opts    Options
	logger  *slog.Logger
	httpSrv *http.Server
	ln      net.Listener
	started time.Time

	// draining flips /readyz to unready without taking the listener down, so
	// a load balancer sees a deliberate withdrawal rather than a refused
	// connection.
	draining atomic.Bool
}

// New builds the operator surface without binding a socket. Call Start to
// listen.
func New(opts Options) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.ReadinessTimeout <= 0 {
		opts.ReadinessTimeout = 2 * time.Second
	}

	s := &Server{
		opts:    opts,
		logger:  logger,
		started: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/", s.handleIndex)

	if opts.MetricsHandler != nil {
		mux.Handle("/metrics", opts.MetricsHandler)
	}
	if opts.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	s.httpSrv = &http.Server{
		Handler: mux,
		// Generous enough for a 30s CPU profile, which is the longest thing
		// this surface legitimately serves.
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}

	return s
}

// Handler exposes the routing table for tests that do not want a socket.
func (s *Server) Handler() http.Handler {
	return s.httpSrv.Handler
}

// Start binds the listener and serves in the background. It returns the bind
// error synchronously, so a port clash fails startup rather than surfacing
// later as a silent absence of health checks.
func (s *Server) Start() error {
	addr := net.JoinHostPort(s.opts.Bind, fmt.Sprintf("%d", s.opts.Port))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	s.ln = ln

	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("ops server stopped unexpectedly", "error", err)
		}
	}()

	return nil
}

// Addr reports the address actually bound, which is the only way to learn the
// port when one was requested ephemerally. It is empty before Start.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Drain marks the instance unready while continuing to serve. Call it at the
// start of shutdown so a load balancer removes this instance before its
// dependencies go away, rather than discovering the fact through failed
// requests.
func (s *Server) Drain() {
	if s == nil {
		return
	}
	s.draining.Store(true)
	s.logger.Info("ops surface draining — /readyz now reports unready")
}

// Shutdown stops serving, waiting up to the context deadline for in-flight
// requests.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.ln == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

// healthResponse is the /healthz body.
type healthResponse struct {
	Status        string  `json:"status"`
	UptimeSeconds float64 `json:"uptimeSeconds"`
}

// checkResult is one dependency's outcome in the /readyz body.
type checkResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// readyResponse is the /readyz body.
type readyResponse struct {
	Ready    bool                   `json:"ready"`
	Draining bool                   `json:"draining,omitempty"`
	Checks   map[string]checkResult `json:"checks"`
	Details  map[string]any         `json:"details,omitempty"`
}

// handleHealth answers liveness. It deliberately touches no dependency: a
// liveness probe that fails when the database is down gets the process killed
// and restarted for a fault a restart cannot fix.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !allowRead(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, healthResponse{
		Status:        "ok",
		UptimeSeconds: time.Since(s.started).Seconds(),
	})
}

// handleReady answers readiness: whether this instance should receive traffic.
//
// It reports unready when a dependency is unusable or when the instance is
// draining, and 503 is the signal a load balancer acts on. Saturation is not
// unreadiness — a busy instance is working, and withdrawing it would move its
// load onto the instances that are already busiest.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !allowRead(w, r) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.opts.ReadinessTimeout)
	defer cancel()

	resp := readyResponse{
		Ready:    true,
		Draining: s.draining.Load(),
		Checks:   make(map[string]checkResult, len(s.opts.Checks)),
	}
	if resp.Draining {
		resp.Ready = false
	}

	for _, c := range s.opts.Checks {
		if c.Func == nil {
			continue
		}
		if err := c.Func(ctx); err != nil {
			resp.Ready = false
			resp.Checks[c.Name] = checkResult{OK: false, Error: err.Error()}
			continue
		}
		resp.Checks[c.Name] = checkResult{OK: true}
	}

	if s.opts.Details != nil {
		resp.Details = s.opts.Details()
	}

	status := http.StatusOK
	if !resp.Ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

// handleIndex lists what is mounted, so an operator who reaches the port can
// discover the surface without consulting the source.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if !allowRead(w, r) {
		return
	}

	routes := []string{"/healthz", "/readyz"}
	if s.opts.MetricsHandler != nil {
		routes = append(routes, "/metrics")
	}
	if s.opts.EnablePprof {
		routes = append(routes, "/debug/pprof/")
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "ricochet operator surface")
	for _, route := range routes {
		fmt.Fprintln(w, route)
	}
}

// allowRead rejects anything but a read. Nothing on this surface mutates
// state, so a POST is a misdirected request rather than a valid one.
func allowRead(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
