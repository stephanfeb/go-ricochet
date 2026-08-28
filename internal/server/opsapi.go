package server

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/twostack/go-ricochet/internal/opsapi"
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
		Logger:           s.logger,
	}

	if s.config.EnableMetrics {
		opts.MetricsHandler = promhttp.Handler()
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
