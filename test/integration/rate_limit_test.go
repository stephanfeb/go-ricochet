package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
)

// withSDAWriteLimit returns a config hook that replaces the SDA write bucket.
func withSDAWriteLimit(rate, burst int) func(*core.ServerConfig) {
	return func(cfg *core.ServerConfig) {
		limits := cfg.RateLimits.For(core.RateLimitSDA)
		limits.Write = core.Limit{Rate: rate, Burst: burst}
		cfg.RateLimits.Protocols[core.RateLimitSDA] = limits
	}
}

// isRateLimited reports whether err is the server's 429 response.
//
// It matches on text because pkg/client collapses a document error into a
// string, discarding the status code. The server does distinguish 429 from
// every other failure, so a typed client error would be the better surface.
func isRateLimited(err error) bool {
	return err != nil && strings.Contains(err.Error(), "rate limit")
}

// TestRateLimitIsConfigurable is the property A4 exists for: a write limit set
// in configuration must actually reach the SDA handler. Before this, every
// handler's limit was a hardcoded literal, so an operator could raise the one
// documented knob and see no change at all.
func TestRateLimitIsConfigurable(t *testing.T) {
	const rate = 3
	server := newTestServer(t, withSDAWriteLimit(rate, rate))
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ownerID := cl.PeerID()

	for i := 0; i < rate; i++ {
		_, err := cl.PutDocument(ctx, ownerID, fmt.Sprintf("limited/doc-%d", i), []byte("x"))
		if err != nil {
			t.Fatalf("write %d should be within the configured limit of %d: %v", i, rate, err)
		}
	}

	_, err := cl.PutDocument(ctx, ownerID, "limited/overflow", []byte("x"))
	if err == nil {
		t.Fatalf("write %d should have been rejected: the configured limit of %d is not being enforced", rate+1, rate)
	}
	if !isRateLimited(err) {
		t.Fatalf("expected a 429 rate limit error, got: %v", err)
	}
}

// TestRateLimitBurstIsSeparateFromRate checks that burst is its own knob. A
// sliding window cannot express this: with the same sustained rate, a larger
// burst must allow more writes in one go.
func TestRateLimitBurstIsSeparateFromRate(t *testing.T) {
	const rate = 3
	const burst = 12
	server := newTestServer(t, withSDAWriteLimit(rate, burst))
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ownerID := cl.PeerID()

	// The previous test showed rate=3, burst=3 cuts off after 3. With the
	// same rate and a burst of 12, the same client must get 12 through.
	for i := 0; i < burst; i++ {
		_, err := cl.PutDocument(ctx, ownerID, fmt.Sprintf("burst/doc-%d", i), []byte("x"))
		if err != nil {
			t.Fatalf("write %d should be within the burst of %d: %v", i, burst, err)
		}
	}

	_, err := cl.PutDocument(ctx, ownerID, "burst/overflow", []byte("x"))
	if err == nil {
		t.Fatal("a write past the burst capacity should have been rejected")
	}
	if !isRateLimited(err) {
		t.Fatalf("expected a 429 rate limit error, got: %v", err)
	}
}

// TestRateLimitReadsAreNotChargedToWrites checks the read and write buckets
// stay independent once configured, so tightening writes does not throttle
// a client's ability to read back what it stored.
func TestRateLimitReadsAreNotChargedToWrites(t *testing.T) {
	server := newTestServer(t, withSDAWriteLimit(2, 2))
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ownerID := cl.PeerID()

	if _, err := cl.PutDocument(ctx, ownerID, "split/doc", []byte("hello")); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Exhaust the write bucket.
	_, _ = cl.PutDocument(ctx, ownerID, "split/doc-2", []byte("x"))
	if _, err := cl.PutDocument(ctx, ownerID, "split/doc-3", []byte("x")); !isRateLimited(err) {
		t.Fatalf("expected the write bucket to be exhausted, got: %v", err)
	}

	// Reads must be unaffected.
	for i := 0; i < 10; i++ {
		doc, err := cl.GetDocument(ctx, ownerID, "split/doc")
		if err != nil {
			t.Fatalf("read %d should not be charged to the write bucket: %v", i, err)
		}
		if string(doc.Content) != "hello" {
			t.Fatalf("read %d returned %q, want %q", i, doc.Content, "hello")
		}
	}
}
