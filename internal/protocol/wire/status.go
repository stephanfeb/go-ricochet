// Package wire maps server-side errors onto the status codes and retry hints
// that travel to a client.
//
// It exists because the same three conditions — the peer is being throttled,
// the server is saturated, the mailbox is full — arise in six protocols that
// each shape their response differently, and until now each flattened them
// into free-form English. A client could not tell a throttle from a bug, so
// the only safe response to any failure was to slow down and stay slow. That
// is precisely what the sumi team ended up doing.
//
// The classification lives in one place so the protocols cannot disagree about
// what a 429 means, and so adding a condition does not mean editing six
// response writers and forgetting one.
package wire

import (
	"errors"
	"time"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/mda/mailboxes"
	"github.com/twostack/go-ricochet/internal/mta"
)

// Status codes. They are HTTP's, because the semantics line up and every
// client library and operator already knows them; SDA, SFA and SCA were
// already using this vocabulary before this package existed.
const (
	StatusOK                 = 200
	StatusBadRequest         = 400
	StatusForbidden          = 403
	StatusNotFound           = 404
	StatusConflict           = 409
	StatusTooManyRequests    = 429
	StatusInternalError      = 500
	StatusServiceUnavailable = 503

	// StatusInsufficientStorage is a full mailbox. It is distinct from a 503:
	// the server is fine and retrying will not help until the owner drains
	// the mailbox, so a client that treats it as backpressure would retry
	// forever against a condition only the recipient can clear.
	StatusInsufficientStorage = 507
)

// Classify maps a pipeline or handler error onto the status a client should
// see and how long it should wait before retrying.
//
// A zero duration means no useful hint: either the condition is not one that
// time resolves, or the source could not say. Callers must omit the field
// rather than send a zero, since "retry after 0s" reads as an invitation to
// spin.
func Classify(err error) (status int, retryAfter time.Duration) {
	if err == nil {
		return StatusOK, 0
	}

	// Ordered most specific first. A rate-limit rejection and an admission
	// shed are both "come back later", but they mean opposite things about
	// whose fault it is, and a client that conflates them either backs off
	// when it did nothing wrong or hammers a server that is already full.
	var rateLimited *forge.RateLimitedError
	if errors.As(err, &rateLimited) {
		return StatusTooManyRequests, rateLimited.RetryAfter
	}
	if errors.Is(err, forge.ErrRateLimited) {
		return StatusTooManyRequests, 0
	}

	var overloaded *admission.OverloadedError
	if errors.As(err, &overloaded) {
		return StatusServiceUnavailable, overloaded.RetryAfter
	}
	if errors.Is(err, admission.ErrOverloaded) {
		return StatusServiceUnavailable, 0
	}

	var full *mailboxes.MailboxFullError
	if errors.As(err, &full) {
		return StatusInsufficientStorage, 0
	}
	// A quota and a server over its storage budget are the same shape as a
	// full mailbox: retrying does nothing until something is deleted.
	var quota *mailboxes.QuotaExceededError
	if errors.As(err, &quota) {
		return StatusInsufficientStorage, 0
	}
	if errors.Is(err, capacity.ErrStorageFull) {
		return StatusInsufficientStorage, 0
	}
	var badPath *mailboxes.InvalidPathError
	if errors.As(err, &badPath) {
		return StatusBadRequest, 0
	}

	// Access-control refusals from the mailbox types. Before these were
	// classified they fell through to 500, and a client reading another
	// peer's private inbox saw "internal error" for what was a correct
	// refusal.
	var unauthorized *mailboxes.UnauthorizedError
	if errors.As(err, &unauthorized) {
		return StatusForbidden, 0
	}
	var forwarding *mta.ForwardingRefusedError
	if errors.As(err, &forwarding) {
		return StatusForbidden, 0
	}
	var notFound *mailboxes.NotFoundError
	if errors.As(err, &notFound) {
		return StatusNotFound, 0
	}

	// A request naming an operation the server does not route is the client's
	// mistake, not the server's. Reporting it as a 500 sends an operator
	// looking for a fault that is not there.
	if errors.Is(err, middleware.ErrUnknownOperation) || errors.Is(err, middleware.ErrMissingOperation) {
		return StatusBadRequest, 0
	}

	return StatusInternalError, 0
}

// RetryAfterMs renders a hint for a JSON response. It returns zero when there
// is nothing to say, so the field can be tagged omitempty and simply not
// appear rather than appearing as a zero a client has to interpret.
//
// Milliseconds rather than HTTP's seconds: the useful hints here are well
// under a second — a token bucket refilling, a fleet being desynchronised —
// and rounding those to a whole second would turn a 40ms wait into a pause
// twenty-five times longer than necessary.
func RetryAfterMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := d.Milliseconds()
	if ms < 1 {
		return 1
	}
	return ms
}

// RetryAfterHeaderKey is where SDA, SFA and SCA carry the hint. Those
// protocols already have a headers map and no room for a new top-level field
// without changing three response structs, so the hint travels as a header;
// the mailbox protocols, whose responses are flat, carry it as a field.
const RetryAfterHeaderKey = "Retry-After-Ms"
