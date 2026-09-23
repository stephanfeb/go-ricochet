// Package ratelimit builds the per-protocol rate limiters from configuration
// and makes them available to the protocol handlers.
//
// Limiters live here rather than inside each handler so that a single place
// owns the mapping from configuration to enforcement. Before this, every
// handler constructed its own limiter from a hardcoded literal, which left an
// operator with no way to raise a limit that was cutting them off.
package ratelimit

import (
	"sync"

	forge "github.com/stephanfeb/go-p2p-forge"
	"github.com/stephanfeb/go-p2p-forge/middleware"

	"github.com/stephanfeb/go-ricochet/internal/core"
)

// RegistryKey is the forge registry key under which the limiters are provided.
const RegistryKey = "ratelimits"

// Limiters holds one limiter per protocol. Each is shared by every stream on
// that protocol and budgets per peer ID, so a peer's allowance follows it
// across connections rather than resetting with each new stream.
type Limiters struct {
	MSA      *middleware.TokenBucket
	MSABatch *middleware.TokenBucket
	MAA      *middleware.TokenBucket
	MMA      *middleware.TokenBucket
	MTA      *middleware.TokenBucket

	SDA *middleware.DualTokenBucket
	SFA *middleware.DualTokenBucket
	SCA *middleware.DualTokenBucket
}

// New builds the limiters described by cfg.
func New(cfg core.RateLimits) *Limiters {
	window := cfg.EffectiveWindow()

	single := func(protocol string) *middleware.TokenBucket {
		l := cfg.For(protocol).Requests
		return middleware.NewTokenBucket(window, l.Rate, l.Burst)
	}
	dual := func(protocol string) *middleware.DualTokenBucket {
		l := cfg.For(protocol)
		return middleware.NewDualTokenBucket(window,
			l.Read.Rate, l.Read.Burst,
			l.Write.Rate, l.Write.Burst,
		)
	}

	return &Limiters{
		MSA:      single(core.RateLimitMSA),
		MSABatch: single(core.RateLimitMSABatch),
		MAA:      single(core.RateLimitMAA),
		MMA:      single(core.RateLimitMMA),
		MTA:      single(core.RateLimitMTA),
		SDA:      dual(core.RateLimitSDA),
		SFA:      dual(core.RateLimitSFA),
		SCA:      dual(core.RateLimitSCA),
	}
}

// Close stops the background eviction goroutine of every limiter.
func (l *Limiters) Close() {
	l.MSA.Close()
	l.MSABatch.Close()
	l.MAA.Close()
	l.MMA.Close()
	l.MTA.Close()
	l.SDA.Close()
	l.SFA.Close()
	l.SCA.Close()
}

var (
	defaultOnce     sync.Once
	defaultLimiters *Limiters
)

// Default returns a lazily built set of limiters using the built-in defaults.
// It exists so that FromRegistry can fall back to enforcing something rather
// than to enforcing nothing.
func Default() *Limiters {
	defaultOnce.Do(func() {
		defaultLimiters = New(core.DefaultRateLimits())
	})
	return defaultLimiters
}

// FromRegistry retrieves the configured limiters, falling back to the defaults
// when a pipeline is built without them.
//
// Falling back rather than returning nil is deliberate: a nil limiter would
// have to be treated as "allow everything", so a wiring mistake would silently
// remove rate limiting from a protocol instead of failing visibly.
func FromRegistry(reg *forge.Registry) *Limiters {
	if reg != nil {
		if l, ok := forge.Service[*Limiters](reg, RegistryKey); ok && l != nil {
			return l
		}
	}
	return Default()
}
