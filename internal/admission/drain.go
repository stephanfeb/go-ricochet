package admission

import (
	"errors"
	"sync/atomic"

	forge "github.com/stephanfeb/go-p2p-forge"
)

// ErrDraining is returned to a request that arrives while the server is
// shutting down. It is a 503 like ErrOverloaded: the peer did nothing wrong
// and should try another server, or this one once it is back.
var ErrDraining = errors.New("server shutting down")

// DrainKey is the forge registry key under which the drain switch is provided.
const DrainKey = "drain"

// Drain is the shutdown switch every pipeline consults before admitting a
// request. It is separate from the Controller because the controller is nil
// when admission control is disabled, and shutdown has to work regardless.
type Drain struct {
	on atomic.Bool
}

// Begin flips the switch: every request from now on is refused with
// ErrDraining. It is idempotent.
func (d *Drain) Begin() { d.on.Store(true) }

// Active reports whether the server is draining.
func (d *Drain) Active() bool { return d != nil && d.on.Load() }

// DrainGate refuses requests once the drain has begun. It sits just before
// the admission middleware, after decoding and rate limiting, so a refused
// request is answered in the protocol's own error shape by the response
// writer above it. A nil Drain admits everything.
func DrainGate(d *Drain) forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		if d.Active() {
			sc.Err = ErrDraining
			return
		}
		next()
	}
}

// DrainFromRegistry retrieves the drain switch, or nil when none is wired,
// which admits everything.
func DrainFromRegistry(reg *forge.Registry) *Drain {
	if reg == nil {
		return nil
	}
	d, _ := forge.Service[*Drain](reg, DrainKey)
	return d
}
