package wire

import (
	"context"

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

// RequestDeadline bounds the work a request may do once it has been read:
// message_timeout is the deadline on sc.Ctx from here to the response.
//
// It sits after frame decoding so that reading a large request over a slow
// link, which the idle timeout already governs, does not eat into the
// budget for the storage work. Handlers reach storage through sc.Ctx, so a
// database that stops answering releases the admission slot and the stream
// at the deadline instead of holding both until the request timeout.
func RequestDeadline(reg *forge.Registry) forge.Middleware {
	cfg, ok := forge.Service[*core.ServerConfig](reg, "config")
	if !ok || cfg == nil || cfg.MessageTimeout <= 0 {
		return func(sc *forge.StreamContext, next func()) { next() }
	}
	timeout := cfg.MessageTimeout
	return func(sc *forge.StreamContext, next func()) {
		ctx, cancel := context.WithTimeout(sc.Ctx, timeout)
		defer cancel()
		sc.Ctx = ctx
		next()
	}
}
