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
	"encoding/json"
	"errors"
	"time"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/capacity"
	"github.com/twostack/go-ricochet/internal/mda/mailboxes"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/storage"
	public "github.com/twostack/go-ricochet/pkg/wire"
)

// Status codes are defined in pkg/wire, where clients can name them; they
// are aliased here so the handlers keep reading wire.StatusX.
const (
	StatusOK                  = public.StatusOK
	StatusBadRequest          = public.StatusBadRequest
	StatusForbidden           = public.StatusForbidden
	StatusNotFound            = public.StatusNotFound
	StatusConflict            = public.StatusConflict
	StatusTooManyRequests     = public.StatusTooManyRequests
	StatusInternalError       = public.StatusInternalError
	StatusServiceUnavailable  = public.StatusServiceUnavailable
	StatusInsufficientStorage = public.StatusInsufficientStorage
)

// Classify maps a pipeline or handler error onto the status a client should
// see and how long it should wait before retrying.
//
// A zero duration means no useful hint: either the condition is not one that
// time resolves, or the source could not say. Callers must omit the field
// rather than send a zero, since "retry after 0s" reads as an invitation to
// spin.
func Classify(err error) (status int, retryAfter time.Duration) {
	status, retryAfter, _ = classify(err)
	return status, retryAfter
}

// ClientMessage is the text a client may see for err.
//
// An error the server constructed for the client -- a full mailbox, a
// refused forwarder, a malformed request -- carries its own wording, which
// is part of the contract. Anything else is replaced by a fixed phrase for
// its status: a driver or database error names constraints, tables and
// SQL state, which is an information leak and a wire format nobody chose.
// The real error belongs in the server log next to the peer that sent the
// request.
func ClientMessage(err error) string {
	status, _, msg := classify(err)
	if msg != "" {
		return msg
	}
	switch status {
	case StatusOK:
		return ""
	case StatusBadRequest:
		return "bad request"
	case StatusForbidden:
		return "forbidden"
	case StatusNotFound:
		return "not found"
	case StatusConflict:
		return "conflict"
	case StatusTooManyRequests:
		return "rate limited"
	case StatusServiceUnavailable:
		return "server overloaded"
	case StatusInsufficientStorage:
		return "insufficient storage"
	default:
		return "internal error"
	}
}

// classify is the one table of conditions a client is told about. The
// message is the wording of the recognised error itself, without any
// context a caller wrapped around it, and empty when the error is not one a
// client should read.
func classify(err error) (status int, retryAfter time.Duration, msg string) {
	if err == nil {
		return StatusOK, 0, ""
	}

	// Ordered most specific first. A rate-limit rejection and an admission
	// shed are both "come back later", but they mean opposite things about
	// whose fault it is, and a client that conflates them either backs off
	// when it did nothing wrong or hammers a server that is already full.
	var rateLimited *forge.RateLimitedError
	if errors.As(err, &rateLimited) {
		return StatusTooManyRequests, rateLimited.RetryAfter, "rate limit exceeded"
	}
	if errors.Is(err, forge.ErrRateLimited) {
		return StatusTooManyRequests, 0, "rate limit exceeded"
	}
	var mtaLimited *mta.RateLimitError
	if errors.As(err, &mtaLimited) {
		return StatusTooManyRequests, 0, mtaLimited.Error()
	}

	var overloaded *admission.OverloadedError
	if errors.As(err, &overloaded) {
		return StatusServiceUnavailable, overloaded.RetryAfter, overloaded.Error()
	}
	if errors.Is(err, admission.ErrOverloaded) {
		return StatusServiceUnavailable, 0, admission.ErrOverloaded.Error()
	}

	var full *mailboxes.MailboxFullError
	if errors.As(err, &full) {
		return StatusInsufficientStorage, 0, full.Error()
	}
	// A quota and a server over its storage budget are the same shape as a
	// full mailbox: retrying does nothing until something is deleted.
	var quota *mailboxes.QuotaExceededError
	if errors.As(err, &quota) {
		return StatusInsufficientStorage, 0, quota.Error()
	}
	if errors.Is(err, capacity.ErrStorageFull) {
		return StatusInsufficientStorage, 0, capacity.ErrStorageFull.Error()
	}
	var badPath *mailboxes.InvalidPathError
	if errors.As(err, &badPath) {
		return StatusBadRequest, 0, badPath.Error()
	}
	var invalid *mta.ValidationError
	if errors.As(err, &invalid) {
		return StatusBadRequest, 0, invalid.Error()
	}
	var tooBig *storage.DocumentSizeExceededError
	if errors.As(err, &tooBig) {
		return StatusBadRequest, 0, tooBig.Error()
	}
	if errors.Is(err, storage.ErrInvalidCursor) {
		return StatusBadRequest, 0, storage.ErrInvalidCursor.Error()
	}
	// A request the server could not parse is the client's to fix, and the
	// decoder's wording says where.
	var syntax *json.SyntaxError
	if errors.As(err, &syntax) {
		return StatusBadRequest, 0, syntax.Error()
	}
	var badType *json.UnmarshalTypeError
	if errors.As(err, &badType) {
		return StatusBadRequest, 0, badType.Error()
	}

	// Access-control refusals from the mailbox types. Before these were
	// classified they fell through to 500, and a client reading another
	// peer's private inbox saw "internal error" for what was a correct
	// refusal.
	var unauthorized *mailboxes.UnauthorizedError
	if errors.As(err, &unauthorized) {
		return StatusForbidden, 0, unauthorized.Error()
	}
	var forwarding *mta.ForwardingRefusedError
	if errors.As(err, &forwarding) {
		return StatusForbidden, 0, forwarding.Error()
	}
	var notFound *mailboxes.NotFoundError
	if errors.As(err, &notFound) {
		return StatusNotFound, 0, notFound.Error()
	}
	for _, sentinel := range []error{
		storage.ErrMailboxNotFound, storage.ErrDocumentNotFound, storage.ErrDirectoryEntryNotFound,
		storage.ErrFeedNotFound, storage.ErrCollectionNotFound,
	} {
		if errors.Is(err, sentinel) {
			return StatusNotFound, 0, sentinel.Error()
		}
	}
	var docConflict *storage.DocumentConflictError
	if errors.As(err, &docConflict) {
		return StatusConflict, 0, docConflict.Error()
	}
	var itemConflict *storage.CollectionItemConflictError
	if errors.As(err, &itemConflict) {
		return StatusConflict, 0, itemConflict.Error()
	}

	// A request naming an operation the server does not route is the client's
	// mistake, not the server's. Reporting it as a 500 sends an operator
	// looking for a fault that is not there.
	if errors.Is(err, middleware.ErrUnknownOperation) {
		return StatusBadRequest, 0, middleware.ErrUnknownOperation.Error()
	}
	if errors.Is(err, middleware.ErrMissingOperation) {
		return StatusBadRequest, 0, middleware.ErrMissingOperation.Error()
	}
	if errors.Is(err, capacity.ErrNotSampled) {
		return StatusServiceUnavailable, 0, capacity.ErrNotSampled.Error()
	}

	return StatusInternalError, 0, ""
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
const RetryAfterHeaderKey = public.RetryAfterHeaderKey
