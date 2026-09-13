package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twostack/go-ricochet/internal/protocol/maa"
	"github.com/twostack/go-ricochet/pkg/wire"
)

// Errors from this package carry the server's classification instead of
// collapsing it into a sentence.
//
// Every failure used to arrive as fmt.Errorf text, so a caller could not tell
// "you are being throttled" from "the server is full" from "that mailbox
// cannot accept more messages" — three conditions with three different correct
// responses. With only prose to go on, the safe move was to slow down and stay
// slow, which is how a 500-document sync ended up pacing itself at eighteen
// documents a minute against a server that could have taken all of them at
// once.
//
// Branch with errors.As for the specific type, or errors.Is against the
// sentinels for a coarser check:
//
//	var limited *client.RateLimitedError
//	if errors.As(err, &limited) {
//	    time.Sleep(limited.RetryAfter)  // the server said how long
//	}

// Sentinels for errors.Is. Each typed error matches its own sentinel and no
// other, so a check for one condition never quietly catches a different one.
var (
	// ErrRateLimited means this peer exceeded its own allowance. The server is
	// healthy; the client is being paced.
	ErrRateLimited = errors.New("rate limited")

	// ErrOverloaded means the server could not admit the request before its
	// acquire timeout. Nothing is wrong with the request, and the condition
	// clears on its own.
	ErrOverloaded = errors.New("server at capacity")

	// ErrConflict means the precondition failed — the document changed under
	// the caller. Retrying the same write unchanged will fail identically.
	ErrConflict = errors.New("conflict")

	// ErrMailboxFull means the recipient's mailbox is at its cap. This is not
	// backpressure: only the recipient can clear it, so retrying is futile
	// until they do.
	ErrMailboxFull = errors.New("mailbox full")

	// ErrForbidden means the server refused the caller access to something
	// that exists. Retrying as the same identity will not change the answer.
	ErrForbidden = errors.New("forbidden")

	// ErrNotFound means the addressed mailbox, document or other resource
	// does not exist on this server.
	ErrNotFound = errors.New("not found")
)

// ProtocolError is a failure the server classified with a status code.
//
// It is embedded by the specific types rather than used directly for the
// conditions those cover, but it is returned on its own for statuses with no
// dedicated type — a 400, a 500 — so a caller always has the status available
// even when it has no special handling for it.
type ProtocolError struct {
	// Op names the operation, so the message reads the way the old fmt.Errorf
	// text did.
	Op string

	// Status is the server's status code, using HTTP's vocabulary.
	Status int

	// Message is the server's explanation, which may be empty.
	Message string

	// RetryAfter is how long the server asked the caller to wait. Zero means
	// it did not say — not that retrying immediately is free.
	RetryAfter time.Duration
}

func (e *ProtocolError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = fmt.Sprintf("status %d", e.Status)
	}
	if e.Op == "" {
		return msg
	}
	return e.Op + ": " + msg
}

// RateLimitedError reports that this peer exceeded its allowance (429).
//
// RetryAfter is exact when present: it comes from the token bucket, which
// knows when the next token arrives. Waiting it out is the correct response;
// backing off further paces the client below what the server would allow.
type RateLimitedError struct{ ProtocolError }

func (e *RateLimitedError) Is(target error) bool { return target == ErrRateLimited }

// OverloadedError reports that the server was saturated (503).
//
// This says nothing about the request. RetryAfter here is a floor to jitter
// around rather than a schedule — the server bounds concurrency rather than
// rate, so capacity frees continuously and a long uniform backoff across a
// fleet wastes it.
type OverloadedError struct{ ProtocolError }

func (e *OverloadedError) Is(target error) bool { return target == ErrOverloaded }

// ConflictError reports a failed precondition (409): the document changed
// since the ETag the caller sent. ActualETag is the server's current value
// when it supplied one, which is what a caller needs to merge and retry.
type ConflictError struct {
	ProtocolError
	ActualETag string
}

func (e *ConflictError) Is(target error) bool { return target == ErrConflict }

// MailboxFullError reports that the recipient's mailbox is at its cap (507).
//
// It carries no retry hint on purpose. Time does not clear this condition —
// only the recipient does — so a client that treats it as backpressure retries
// against something that will not change.
type MailboxFullError struct{ ProtocolError }

func (e *MailboxFullError) Is(target error) bool { return target == ErrMailboxFull }

// ForbiddenError reports an access-control refusal (403).
type ForbiddenError struct{ ProtocolError }

func (e *ForbiddenError) Is(target error) bool { return target == ErrForbidden }

// NotFoundError reports that the addressed resource does not exist (404).
type NotFoundError struct{ ProtocolError }

func (e *NotFoundError) Is(target error) bool { return target == ErrNotFound }

// RetryAfter reports how long the server asked the caller to wait, and whether
// it said anything at all. A caller that gets false should back off on its own
// terms rather than treating zero as "immediately".
func RetryAfter(err error) (time.Duration, bool) {
	var pe *ProtocolError
	if asProtocolError(err, &pe) && pe.RetryAfter > 0 {
		return pe.RetryAfter, true
	}
	return 0, false
}

// Status reports the server's status code for err, or zero when err did not
// come from a classified server response.
func Status(err error) int {
	var pe *ProtocolError
	if asProtocolError(err, &pe) {
		return pe.Status
	}
	return 0
}

// IsRetryable reports whether waiting and trying again could succeed.
//
// True for throttling and saturation, which time resolves. Explicitly false
// for a full mailbox and for a conflict: both need someone to do something —
// the recipient to drain, or the caller to re-read and merge — and retrying
// them on a timer is an infinite loop that looks like patience.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRateLimited) || errors.Is(err, ErrOverloaded)
}

// asProtocolError finds the embedded ProtocolError in any of the typed errors.
// The embedded struct is not itself in the Unwrap chain, so errors.As on
// *ProtocolError would not find it inside a *RateLimitedError; this walks the
// concrete types instead.
func asProtocolError(err error, out **ProtocolError) bool {
	var (
		rl   *RateLimitedError
		ov   *OverloadedError
		cf   *ConflictError
		full *MailboxFullError
		fb   *ForbiddenError
		nf   *NotFoundError
		pe   *ProtocolError
	)
	switch {
	case errors.As(err, &rl):
		*out = &rl.ProtocolError
	case errors.As(err, &ov):
		*out = &ov.ProtocolError
	case errors.As(err, &cf):
		*out = &cf.ProtocolError
	case errors.As(err, &full):
		*out = &full.ProtocolError
	case errors.As(err, &fb):
		*out = &fb.ProtocolError
	case errors.As(err, &nf):
		*out = &nf.ProtocolError
	case errors.As(err, &pe):
		*out = pe
	default:
		return false
	}
	return true
}

// statusError builds the typed error for a classified failure. It is the one
// place that maps a status onto a type, so the protocols cannot drift apart in
// what they return for the same code.
//
// A status below 400 is not a failure and returns nil, which lets a caller
// hand it every response without checking first.
func statusError(op string, status int, message string, retryAfterMs int64) error {
	if status < 400 {
		return nil
	}

	base := ProtocolError{
		Op:         op,
		Status:     status,
		Message:    message,
		RetryAfter: time.Duration(retryAfterMs) * time.Millisecond,
	}
	if base.Message == "" {
		base.Message = fmt.Sprintf("status %d", status)
	}

	switch status {
	case wire.StatusTooManyRequests:
		return &RateLimitedError{base}
	case wire.StatusServiceUnavailable:
		return &OverloadedError{base}
	case wire.StatusConflict:
		return &ConflictError{ProtocolError: base}
	case wire.StatusInsufficientStorage:
		// A full mailbox carries no hint even if one arrived: waiting does not
		// clear it, and reporting a wait would invite exactly the retry loop
		// that never terminates.
		base.RetryAfter = 0
		return &MailboxFullError{base}
	case wire.StatusForbidden:
		return &ForbiddenError{base}
	case wire.StatusNotFound:
		return &NotFoundError{base}
	default:
		return &ProtocolError{Op: base.Op, Status: base.Status, Message: base.Message, RetryAfter: base.RetryAfter}
	}
}

// headerError builds a typed error from a document-style response, where the
// message and the retry hint travel in the headers map.
func headerError(op string, status int, headers map[string]any) error {
	message, _ := headers["Error"].(string)
	err := statusError(op, status, message, headerToInt64(headers[wire.RetryAfterHeaderKey]))

	// The server's current ETag is what makes a conflict actionable, so it is
	// carried through rather than left for the caller to fetch again. The
	// document protocol names it Actual-ETag, to distinguish it from the ETag
	// header a successful write returns.
	if conflict, ok := err.(*ConflictError); ok {
		for _, key := range []string{"Actual-ETag", "ETag"} {
			if etag, ok := headers[key].(string); ok && etag != "" {
				conflict.ActualETag = etag
				break
			}
		}
	}
	return err
}

// responseError is headerError for a call site that has already decided the
// response is a failure.
//
// It never returns nil. A status the classifier does not treat as an error —
// a 3xx arriving where the caller required a 200 — still has to surface as
// something, or the caller gets a nil error alongside a nil result and reports
// success it did not have.
// deleteOutcome reads a delete reply. Absent is false with no error, gone is
// true, and anything else is the typed error for its status: a 403 or a 500
// used to read as "deleted", which is the one thing a delete must not lie
// about.
func deleteOutcome(op string, status int, headers map[string]any) (bool, error) {
	switch {
	case status == wire.StatusNotFound:
		return false, nil
	case status < 300:
		return true, nil
	default:
		return false, responseError(op, status, headers)
	}
}

func responseError(op string, status int, headers map[string]any) error {
	if err := headerError(op, status, headers); err != nil {
		return err
	}
	message, _ := headers["Error"].(string)
	if message == "" {
		message = fmt.Sprintf("unexpected status %d", status)
	}
	return &ProtocolError{Op: op, Status: status, Message: message}
}

// failureStatus supplies a status for a failure the server did not classify.
//
// A server predating typed statuses reports success=false with no status at
// all, and a zero would fall through statusError's "not a failure" guard and
// return nil — turning a rejection into a silent success. Treating an
// unclassified failure as a 500 keeps it a failure, which is the safe reading
// of "something went wrong and I cannot tell you what".
func failureStatus(status int) int {
	if status < 400 {
		return wire.StatusInternalError
	}
	return status
}

// maaError reports the mailbox-access server's failure envelope when a frame
// is one.
//
// The retrieve response and the various acks have no "error" field, so an
// envelope is unambiguous. Checking for it matters most on retrieve, where the
// compound frame decoder cannot make sense of an error envelope at all: a
// throttled retrieve used to surface as "retrieve response truncated at
// metadata", which reads like a codec bug and sends the reader looking at the
// framing instead of at the limiter that actually rejected them.
func maaError(op string, data []byte) error {
	var envelope maa.ErrorResponse
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Error == "" {
		return nil
	}
	return statusError(op, failureStatus(envelope.Status), envelope.Error, envelope.RetryAfterMs)
}
