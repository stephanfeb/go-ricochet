package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/protocol/maa"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
)

// The point of the whole file: a caller can tell the four conditions apart
// without reading English.
func TestStatusMapsToTheRightType(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{wire.StatusTooManyRequests, ErrRateLimited},
		{wire.StatusServiceUnavailable, ErrOverloaded},
		{wire.StatusConflict, ErrConflict},
		{wire.StatusInsufficientStorage, ErrMailboxFull},
	}

	for _, tc := range cases {
		err := statusError("op", tc.status, "server said so", 0)
		if err == nil {
			t.Fatalf("status %d produced no error", tc.status)
		}
		if !errors.Is(err, tc.want) {
			t.Errorf("status %d = %v, want it to match %v", tc.status, err, tc.want)
		}

		// And must not match any of the others. A check for one condition
		// quietly catching a different one is worse than no check.
		for _, other := range []error{ErrRateLimited, ErrOverloaded, ErrConflict, ErrMailboxFull} {
			if other == tc.want {
				continue
			}
			if errors.Is(err, other) {
				t.Errorf("status %d also matches %v", tc.status, other)
			}
		}
	}
}

func TestErrorsAsExposesTheDetail(t *testing.T) {
	err := statusError("document put", wire.StatusTooManyRequests, "rate limit exceeded", 250)

	var limited *RateLimitedError
	if !errors.As(err, &limited) {
		t.Fatalf("err = %v, want *RateLimitedError", err)
	}
	if limited.Status != wire.StatusTooManyRequests {
		t.Errorf("Status = %d, want 429", limited.Status)
	}
	if limited.RetryAfter != 250*time.Millisecond {
		t.Errorf("RetryAfter = %v, want 250ms", limited.RetryAfter)
	}
	if limited.Op != "document put" {
		t.Errorf("Op = %q", limited.Op)
	}
}

// An unclassified status still has to reach the caller as an error carrying
// the number, or the caller is back to reading prose.
func TestUnclassifiedStatusIsStillTyped(t *testing.T) {
	err := statusError("op", 400, "bad path", 0)

	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *ProtocolError", err)
	}
	if Status(err) != 400 {
		t.Errorf("Status = %d, want 400", Status(err))
	}
	if IsRetryable(err) {
		t.Error("a 400 is reported as retryable")
	}
}

func TestSuccessStatusIsNotAnError(t *testing.T) {
	for _, status := range []int{200, 201, 204, 304} {
		if err := statusError("op", status, "", 0); err != nil {
			t.Errorf("status %d produced %v, want nil", status, err)
		}
	}
}

// Retrying is right for throttling and saturation and wrong for the other two,
// and the difference has to be visible without the caller knowing the codes.
func TestRetryabilityMatchesWhatTimeCanFix(t *testing.T) {
	retryable := []int{wire.StatusTooManyRequests, wire.StatusServiceUnavailable}
	for _, status := range retryable {
		if !IsRetryable(statusError("op", status, "", 0)) {
			t.Errorf("status %d is not reported as retryable", status)
		}
	}

	// A full mailbox clears only when the recipient drains it, and a conflict
	// only when the caller re-reads. Retrying either on a timer never ends.
	for _, status := range []int{wire.StatusInsufficientStorage, wire.StatusConflict, 500, 403} {
		if IsRetryable(statusError("op", status, "", 0)) {
			t.Errorf("status %d is reported as retryable", status)
		}
	}
}

func TestRetryAfterIsReportedOnlyWhenTheServerSaidSomething(t *testing.T) {
	withHint := statusError("op", wire.StatusTooManyRequests, "", 400)
	if d, ok := RetryAfter(withHint); !ok || d != 400*time.Millisecond {
		t.Errorf("RetryAfter = %v, %v; want 400ms, true", d, ok)
	}

	// Absent means the server could not say — which is not the same as "retry
	// immediately", so the caller must be able to tell.
	without := statusError("op", wire.StatusTooManyRequests, "", 0)
	if d, ok := RetryAfter(without); ok {
		t.Errorf("RetryAfter = %v, true with no hint sent; want false", d)
	}

	if _, ok := RetryAfter(errors.New("some other failure")); ok {
		t.Error("a plain error reported a retry hint")
	}
}

// A full mailbox must never carry a hint, however the server phrased it.
// Waiting does not clear it, and a hint invites a loop that never ends.
func TestMailboxFullCarriesNoRetryHint(t *testing.T) {
	err := statusError("submit message", wire.StatusInsufficientStorage, "mailbox full", 5000)

	var full *MailboxFullError
	if !errors.As(err, &full) {
		t.Fatalf("err = %v, want *MailboxFullError", err)
	}
	if full.RetryAfter != 0 {
		t.Errorf("RetryAfter = %v, want none", full.RetryAfter)
	}
	if _, ok := RetryAfter(err); ok {
		t.Error("a full mailbox offered a retry hint")
	}
}

// A conflict is only actionable with the server's current ETag.
func TestConflictCarriesTheServerETag(t *testing.T) {
	err := headerError("document put", wire.StatusConflict, map[string]any{
		"Error": "etag mismatch",
		"ETag":  `"v7"`,
	})

	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v, want *ConflictError", err)
	}
	if conflict.ActualETag != `"v7"` {
		t.Errorf("ActualETag = %q, want the server's current value", conflict.ActualETag)
	}
}

func TestHeaderErrorReadsTheRetryHint(t *testing.T) {
	// JSON numbers arrive as float64, which is how this actually reaches a
	// client. A version that only handled int would silently drop every hint.
	err := headerError("document put", wire.StatusTooManyRequests, map[string]any{
		"Error":                  "rate limit exceeded",
		wire.RetryAfterHeaderKey: float64(120),
	})

	if d, ok := RetryAfter(err); !ok || d != 120*time.Millisecond {
		t.Errorf("RetryAfter = %v, %v; want 120ms, true", d, ok)
	}
}

// A response the caller rejected but the classifier would not must still
// surface. Returning nil alongside a nil result reports success that did not
// happen.
func TestResponseErrorNeverReturnsNil(t *testing.T) {
	err := responseError("get feed", 204, map[string]any{})
	if err == nil {
		t.Fatal("a status the caller rejected produced no error")
	}
	if Status(err) != 204 {
		t.Errorf("Status = %d, want 204", Status(err))
	}
}

// A server too old to classify reports success=false and nothing else. That
// has to stay a failure rather than falling through the "not an error" guard.
func TestUnclassifiedFailureStaysAFailure(t *testing.T) {
	res := &SendResult{Success: false, ErrorMessage: "something went wrong"}

	err := res.Err()
	if err == nil {
		t.Fatal("a failed submission with no status produced no error")
	}
	if Status(err) != wire.StatusInternalError {
		t.Errorf("Status = %d, want 500", Status(err))
	}
}

func TestSendResultErrIsNilOnSuccess(t *testing.T) {
	if err := (&SendResult{Success: true}).Err(); err != nil {
		t.Errorf("a successful submission produced %v", err)
	}
	if err := (&BatchSendResult{Success: true}).Err(); err != nil {
		t.Errorf("a successful batch entry produced %v", err)
	}
}

func TestSendResultErrCarriesTheClassification(t *testing.T) {
	res := &SendResult{
		Success:      false,
		ErrorMessage: "mailbox full: 100/100 messages",
		Status:       wire.StatusInsufficientStorage,
	}

	if !errors.Is(res.Err(), ErrMailboxFull) {
		t.Errorf("err = %v, want ErrMailboxFull", res.Err())
	}
}

// The mailbox-access protocol answers a rejected retrieve with an error
// envelope that decodes into an empty result. Missing it turns "you are
// throttled" into "you have no mail".
func TestMAAErrorEnvelopeIsDetected(t *testing.T) {
	data, err := json.Marshal(maa.ErrorResponse{
		Error:        "rate limit exceeded",
		Status:       wire.StatusTooManyRequests,
		RetryAfterMs: 90,
	})
	if err != nil {
		t.Fatal(err)
	}

	got := maaError("retrieve messages", data)
	if !errors.Is(got, ErrRateLimited) {
		t.Fatalf("err = %v, want ErrRateLimited", got)
	}
	if d, ok := RetryAfter(got); !ok || d != 90*time.Millisecond {
		t.Errorf("RetryAfter = %v, %v; want 90ms, true", d, ok)
	}
}

// A real retrieve response has no "error" field and must pass through.
func TestMAAErrorIgnoresARealResponse(t *testing.T) {
	for _, body := range []string{
		`{"messages":[],"hasMore":false}`,
		`{"success":true,"updatedCount":3}`,
		`not json at all`,
	} {
		if err := maaError("retrieve messages", []byte(body)); err != nil {
			t.Errorf("%s produced %v, want nil", body, err)
		}
	}
}

func TestErrorMessageReadsLikeTheOldOne(t *testing.T) {
	err := statusError("document put", 500, "storage unavailable", 0)
	if got, want := err.Error(), "document put: storage unavailable"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	// With nothing from the server, the status still has to be legible.
	bare := statusError("document put", 503, "", 0)
	if got := bare.Error(); got != fmt.Sprintf("document put: status %d", 503) {
		t.Errorf("Error() = %q", got)
	}
}
