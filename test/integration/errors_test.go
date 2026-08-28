package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	client "github.com/twostack/go-ricochet/pkg/client"
)

// B4's acceptance criterion, over the wire: a client branches on the error
// type, and the tests stop matching on strings.
//
// The conditions being separated here are the ones a caller must respond to
// differently — throttled, saturated, mailbox full, conflict. Told only
// "failed", the safe response to all four is to slow down and stay slow, which
// is exactly the pacing the sumi team settled on.

// withRequestLimit caps one protocol's single request bucket.
func withRequestLimit(protocol string, rate, burst int) func(*core.ServerConfig) {
	return func(cfg *core.ServerConfig) {
		limits := cfg.RateLimits.For(protocol)
		limits.Requests = core.Limit{Rate: rate, Burst: burst}
		cfg.RateLimits.Protocols[protocol] = limits
	}
}

// A 429 must arrive as a rate-limit error and nothing else, and the wait it
// reports has to be long enough to work. A hint that is a fraction short
// teaches a client to stop believing hints and start guessing.
func TestRateLimitedErrorCarriesAWorkingRetryHint(t *testing.T) {
	// The hint is only as short as the window: at the default one-minute
	// window a rate of 1 means a truthful sixty-second wait, which is correct
	// but not something a test can sit through. A two-second window keeps the
	// arithmetic the same and the wait observable.
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.RateLimits.Window = 2 * time.Second
		withSDAWriteLimit(1, 1)(cfg)
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	owner := cl.PeerID()

	if _, err := cl.PutDocument(ctx, owner, "retry/first", []byte("x")); err != nil {
		t.Fatalf("first write rejected: %v", err)
	}

	_, err := cl.PutDocument(ctx, owner, "retry/second", []byte("x"))
	if err == nil {
		t.Fatal("second write was admitted; the limit did not apply")
	}

	var limited *client.RateLimitedError
	if !errors.As(err, &limited) {
		t.Fatalf("err = %v (%T), want *client.RateLimitedError", err, err)
	}
	if limited.Status != 429 {
		t.Errorf("status = %d, want 429", limited.Status)
	}

	// Being throttled is not being overloaded. A client that conflates them
	// backs off when it did nothing wrong, or hammers a server that is full.
	if errors.Is(err, client.ErrOverloaded) {
		t.Error("a throttled request also reports as overloaded")
	}
	if !client.IsRetryable(err) {
		t.Error("throttling is not reported as retryable")
	}

	wait, ok := client.RetryAfter(err)
	if !ok || wait <= 0 {
		t.Fatalf("RetryAfter = %v, %v; want a positive hint", wait, ok)
	}
	if wait > 4*time.Second {
		t.Errorf("RetryAfter = %v, want roughly the two-second window", wait)
	}

	// The hint has to be true.
	time.Sleep(wait)
	if _, err := cl.PutDocument(ctx, owner, "retry/third", []byte("x")); err != nil {
		t.Errorf("still rejected after waiting the reported %v: %v", wait, err)
	}
}

// A 503 is the server admitting it is full, not a verdict on the client, and
// it must be distinguishable from a 429 without reading the message.
func TestOverloadedErrorIsDistinctFromRateLimited(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		// The shape proven to shed in admission_test: a bound of one, no
		// per-peer cap, and no patience.
		cfg.Admission.MaxInFlight = 1
		cfg.Admission.MaxInFlightPerPeer = 0
		cfg.Admission.AcquireTimeout = time.Millisecond
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	owner := cl.PeerID()

	var shed error
	const callers = 16
	errCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			docs := make([]client.BatchDocumentPut, 100)
			for j := range docs {
				docs[j] = client.BatchDocumentPut{
					Path: fmt.Sprintf("shed/%d-%d", i, j),
					// The payload size is what makes the callers contend;
					// admission_test proves this shape sheds.
					Content: []byte(strings.Repeat("x", 4096)),
				}
			}
			_, err := cl.PutDocuments(ctx, owner, docs)
			errCh <- err
		}(i)
	}
	for i := 0; i < callers; i++ {
		if err := <-errCh; err != nil && errors.Is(err, client.ErrOverloaded) {
			shed = err
		}
	}

	if shed == nil {
		t.Fatal("nothing was shed against a bound of one with a 1ms timeout; " +
			"the test proves nothing if the server never saturates")
	}

	var overloaded *client.OverloadedError
	if !errors.As(shed, &overloaded) {
		t.Fatalf("err = %v (%T), want *client.OverloadedError", shed, shed)
	}
	if overloaded.Status != 503 {
		t.Errorf("status = %d, want 503", overloaded.Status)
	}
	if errors.Is(shed, client.ErrRateLimited) {
		t.Error("a shed request also reports as rate limited")
	}
	if !client.IsRetryable(shed) {
		t.Error("saturation is not reported as retryable")
	}

	// The hint exists to desynchronise a fleet, not to pace it. A large one
	// would re-create the per-minute ceiling admission control removes.
	if wait, ok := client.RetryAfter(shed); ok && wait > 2*time.Second {
		t.Errorf("RetryAfter = %v; a shed request must not be told to wait that long", wait)
	}
}

// A conflict is only actionable with the server's current ETag, and it must
// not read as backpressure: retrying the same write on a timer fails forever.
func TestConflictIsTypedAndCarriesTheServerETag(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	owner := cl.PeerID()

	first, err := cl.PutDocument(ctx, owner, "conflict/doc", []byte(`{"v":1}`))
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}

	_, err = cl.PutDocument(ctx, owner, "conflict/doc", []byte(`{"v":2}`),
		client.WithIfMatch(`"stale-etag"`))
	if err == nil {
		t.Fatal("a write against a stale ETag succeeded")
	}

	var conflict *client.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v (%T), want *client.ConflictError", err, err)
	}
	if conflict.Status != 409 {
		t.Errorf("status = %d, want 409", conflict.Status)
	}
	if client.IsRetryable(err) {
		t.Error("a conflict is reported as retryable; retrying it unchanged never succeeds")
	}
	if conflict.ActualETag == "" {
		t.Error("no current ETag reported; a caller cannot merge without it")
	}
	if first.ETag != "" && conflict.ActualETag != first.ETag {
		t.Errorf("ActualETag = %q, want the server's current %q", conflict.ActualETag, first.ETag)
	}
}

// A full mailbox is not backpressure. Only the recipient can clear it, so a
// client that reads it as "retry later" retries against something that will
// not change until somebody else acts.
func TestMailboxFullIsTypedAndNotRetryable(t *testing.T) {
	const (
		cap    = 2
		folder = "full/inbox"
	)

	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	self := cl.PeerID()

	// The cap is set on the mailbox rather than through
	// cfg.MaxMessagesPerMailbox, because a mailbox created on the delivery
	// path does not read that setting — see mda/delivery.go, which passes a
	// hardcoded 1000. That is a separate bug; this test is about the error a
	// full mailbox produces, so it creates one with a cap it will actually
	// honour.
	if err := cl.CreateMailbox(ctx, folder, core.MailboxPrivate,
		client.WithMailboxMaxMessages(cap)); err != nil {
		t.Fatalf("create capped mailbox: %v", err)
	}

	for i := 0; i < cap; i++ {
		res, err := cl.SendMessage(ctx, self, []byte("filling"),
			client.WithFolderPath(folder))
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if !res.Success {
			t.Fatalf("send %d rejected below the cap: %s", i, res.ErrorMessage)
		}
		if res.Err() != nil {
			t.Fatalf("a successful send reported %v", res.Err())
		}
	}

	res, err := cl.SendMessage(ctx, self, []byte("one too many"),
		client.WithFolderPath(folder))
	if err != nil {
		t.Fatalf("send past the cap: %v", err)
	}
	if res.Success {
		t.Fatal("a message past the cap was accepted")
	}

	full := res.Err()
	var typed *client.MailboxFullError
	if !errors.As(full, &typed) {
		t.Fatalf("err = %v (%T), want *client.MailboxFullError", full, full)
	}
	if typed.Status != 507 {
		t.Errorf("status = %d, want 507", typed.Status)
	}
	if client.IsRetryable(full) {
		t.Error("a full mailbox is reported as retryable")
	}
	if wait, ok := client.RetryAfter(full); ok {
		t.Errorf("a full mailbox offered a retry hint of %v", wait)
	}

	// And it must not be mistaken for either of the transient conditions.
	if errors.Is(full, client.ErrRateLimited) || errors.Is(full, client.ErrOverloaded) {
		t.Error("a full mailbox reports as throttling or saturation")
	}
}

// A throttled retrieve used to fail inside the frame decoder — "retrieve
// response truncated at metadata" — because the compound decoder cannot read
// an error envelope. That reads like a codec bug and sends whoever hits it
// looking at the framing rather than at the limiter that rejected them.
func TestThrottledRetrieveIsNotAnEmptyInbox(t *testing.T) {
	server := newTestServer(t, withRequestLimit(core.RateLimitMAA, 1, 1))
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := cl.RetrieveMessages(ctx); err != nil {
		t.Fatalf("first retrieve: %v", err)
	}

	msgs, err := cl.RetrieveMessages(ctx)
	if err == nil {
		t.Fatalf("a throttled retrieve returned %d messages and no error", len(msgs))
	}
	// The error must name the cause, not the framing.
	if strings.Contains(err.Error(), "truncated") {
		t.Errorf("err = %v; a throttled retrieve is being reported as a decode failure", err)
	}
	if !errors.Is(err, client.ErrRateLimited) {
		t.Errorf("err = %v, want ErrRateLimited", err)
	}
	if msgs != nil {
		t.Errorf("a rejected retrieve returned %d messages", len(msgs))
	}
}

// The admin protocol reports failure in a body rather than a status, so its
// classification travels in a different field. It has to end up the same type.
func TestAdminFailuresAreTyped(t *testing.T) {
	server := newTestServer(t, withRequestLimit(core.RateLimitMMA, 1, 1))
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := cl.QueryCapacity(ctx); err != nil {
		t.Fatalf("first admin call: %v", err)
	}

	_, err := cl.QueryCapacity(ctx)
	if err == nil {
		t.Fatal("second admin call was admitted; the limit did not apply")
	}
	if !errors.Is(err, client.ErrRateLimited) {
		t.Fatalf("err = %v (%T), want ErrRateLimited", err, err)
	}
	if client.Status(err) != 429 {
		t.Errorf("status = %d, want 429", client.Status(err))
	}
}
