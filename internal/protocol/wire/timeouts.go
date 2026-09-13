package wire

import (
	forge "github.com/twostack/go-p2p-forge"

	"github.com/twostack/go-ricochet/internal/core"
)

// Bounded applies the server's edge timeouts to a pipeline.
//
// connection_timeout is the idle budget: how long a frame read or write may
// wait for the peer to make progress before the stream is dropped. It is
// refreshed as bytes move, so it bounds stalls rather than transfers. The
// forge default request timeout stays in force as the ceiling on a stream's
// whole life. Without a config in the registry the forge defaults apply.
func Bounded(p *forge.Pipeline, reg *forge.Registry) *forge.Pipeline {
	cfg, ok := forge.Service[*core.ServerConfig](reg, "config")
	if !ok || cfg == nil {
		return p
	}
	if cfg.ConnectionTimeout > 0 {
		p.WithIdleTimeout(cfg.ConnectionTimeout)
	}
	return p
}
