package admission

import (
	forge "github.com/twostack/go-p2p-forge"
)

// RegistryKey is the forge registry key under which the controller is provided.
const RegistryKey = "admission"

// Middleware admits a request before the handler runs and releases its slot
// once the handler returns.
//
// It belongs after frame decoding and rate limiting — a request that is going
// to be rejected outright should not occupy capacity while it waits — and
// before anything that touches storage.
func Middleware(c *Controller) forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		release, err := c.Acquire(sc.Ctx, sc.PeerID)
		if err != nil {
			sc.Err = err
			sc.Logger.Warn("request not admitted",
				"peer", sc.PeerID,
				"error", err,
				"inFlight", c.InFlight(),
			)
			return
		}
		defer release()
		next()
	}
}

// FromRegistry retrieves the configured controller. A missing entry yields nil,
// which admits everything — the safe direction here, since the alternative
// would be a wiring mistake that silently refuses traffic.
func FromRegistry(reg *forge.Registry) *Controller {
	if reg == nil {
		return nil
	}
	c, _ := forge.Service[*Controller](reg, RegistryKey)
	return c
}
