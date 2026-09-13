package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/twostack/go-ricochet/internal/opsapi"
	"github.com/twostack/go-ricochet/internal/opsview"
)

// poolProvider is satisfied by the PostgreSQL storage backend. It is declared
// here rather than on the Storage interface because connection pooling is a
// property of one backend, not of storage in general.
type poolProvider interface {
	Pool() *pgxpool.Pool
}

// startOpsAPI brings up the operator HTTP surface.
//
// It is not fatal for the rest of the server if this cannot bind, but it is
// reported as an error from Start: an instance with no health check will be
// treated as healthy by every load balancer that cannot reach it, which is
// worse than failing to start.
func (s *Server) startOpsAPI() error {
	if !s.config.Ops.Enabled {
		s.logger.Warn("operator HTTP surface disabled — no health check, readiness or metrics endpoint")
		return nil
	}

	opts := opsapi.Options{
		Bind:             s.config.Ops.EffectiveBind(),
		Port:             s.config.Ops.Port,
		EnablePprof:      s.config.Ops.EnablePprof,
		ReadinessTimeout: s.config.Ops.EffectiveReadinessTimeout(),
		Checks:           s.readinessChecks(),
		Details:          s.opsDetails,
		Routes:           s.opsRoutes(),
		Logger:           s.logger,
	}

	// Serve this server's own registry rather than Prometheus's global
	// default. libp2p registers a large number of collectors into the default
	// registry simply by being imported, and the point of the exposition is to
	// be a deliberate list of what this server publishes.
	if reg := s.metrics.Registry(); reg != nil {
		opts.MetricsHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
		})
	}

	srv := opsapi.New(opts)
	if err := srv.Start(); err != nil {
		return err
	}
	s.opsSrv = srv

	s.logger.Info("operator HTTP surface listening",
		"addr", srv.Addr(),
		"metrics", s.config.EnableMetrics,
		"pprof", s.config.Ops.EnablePprof,
	)
	return nil
}

// opsRoutes builds the operator's view of stored data.
//
// It is skipped when there is no storage, which is the only state in which
// the endpoints would have nothing to read; mounting them anyway would answer
// every question with a 500.
func (s *Server) opsRoutes() map[string]http.Handler {
	if s.storage == nil {
		return nil
	}

	// The same derivation initializeServices uses, so /ops/limits reports the
	// bound the admission controller was actually built with.
	poolSize := 0
	if s.config.Storage.Postgres != nil {
		poolSize = s.config.Storage.Postgres.PoolSize
	}

	return opsview.Routes(opsview.Options{
		Storage:  s.storage,
		Capacity: s.capacity,
		Config:   s.config,
		PoolSize: poolSize,
		Timeout:  s.config.Ops.EffectiveQueryTimeout(),
		Logger:   s.logger,
	})
}

// readinessChecks returns the dependencies /readyz verifies. Today that is
// the database: it is the only thing whose loss makes this instance unable to
// serve a request it would otherwise accept.
func (s *Server) readinessChecks() []opsapi.Check {
	provider, ok := s.storage.(poolProvider)
	if !ok {
		return nil
	}

	return []opsapi.Check{{
		Name: "postgres",
		Func: func(ctx context.Context) error {
			pool := provider.Pool()
			if pool == nil {
				return fmt.Errorf("connection pool not initialized")
			}
			return pool.Ping(ctx)
		},
	}}
}

// opsDetails contributes diagnostic state to the /readyz body: how loaded the
// instance is, and how close the database pool is to exhaustion.
//
// None of it affects the readiness verdict. A saturated instance is working,
// and removing it from rotation would push its load onto whichever instances
// are already busiest. These numbers are here to answer "why is it slow"
// without a debugger attached.
func (s *Server) opsDetails() map[string]any {
	details := map[string]any{
		"uptimeSeconds": time.Since(s.startTime).Seconds(),
		"admission":     s.admission.Stats(),
	}
	if s.mdaSrv != nil {
		details["mailboxCache"] = s.mdaSrv.CacheStats()
	}

	if provider, ok := s.storage.(poolProvider); ok {
		if pool := provider.Pool(); pool != nil {
			stat := pool.Stat()
			details["postgresPool"] = map[string]any{
				"totalConns":        stat.TotalConns(),
				"idleConns":         stat.IdleConns(),
				"acquiredConns":     stat.AcquiredConns(),
				"maxConns":          stat.MaxConns(),
				"emptyAcquireCount": stat.EmptyAcquireCount(),
				"canceledAcquire":   stat.CanceledAcquireCount(),
			}
		}
	}

	return details
}
