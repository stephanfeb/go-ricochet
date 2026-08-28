package server

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/twostack/go-ricochet/internal/metrics"
)

// registerCollectors attaches the runtime collectors to the metrics registry.
//
// Each reads a counter something else already maintains — the admission
// controller, the pgx pool, forge's buffer pool — so there is nothing to keep
// in step and no cost between scrapes.
func (s *Server) registerCollectors() {
	if s.metrics == nil {
		return
	}

	register := func(name string, c prometheus.Collector) {
		if err := s.metrics.Register(c); err != nil {
			// A duplicate registration is a programming error, not an
			// operational one, and it must not stop the server from serving.
			s.logger.Error("failed to register metrics collector", "collector", name, "error", err)
		}
	}

	register("admission", metrics.NewAdmissionCollector(s.admission))
	register("buffer_pool", metrics.NewBufferPoolCollector(s.bufferPool))

	if provider, ok := s.storage.(poolProvider); ok {
		register("db_pool", metrics.NewPoolCollector(provider.Pool()))
	}
}
