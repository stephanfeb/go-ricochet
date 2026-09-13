package mta_test

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/storage/storagetest"
)

// ---------------------------------------------------------------------------

// generatePeerID creates a random peer.ID for testing.
func generatePeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer ID from key: %v", err)
	}
	return id
}

// newTestRouter creates a Router backed by a mockStorage.
func newTestRouter(t *testing.T, maxRequests int) (*mta.Router, *storagetest.Fake) {
	t.Helper()
	store := storagetest.New()
	logger := slog.Default()
	mailboxServer := mda.NewMailboxServer(store, mda.MailboxDefaults{}, logger)
	limiter := middleware.NewTokenBucket(1*time.Minute, maxRequests, maxRequests)
	t.Cleanup(limiter.Close)
	router := mta.NewRouter(mailboxServer, limiter, logger)
	return router, store
}

// validMessage creates a valid message from sender to recipient with a 7-day expiry.
func validMessage(senderID, recipientID peer.ID) *core.Message {
	return core.NewMessageWithDefaultExpiry(senderID, recipientID, []byte("hello"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestAcceptMessage_Success(t *testing.T) {
	router, store := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	ctx := context.Background()

	id, err := router.AcceptMessage(ctx, msg, sender)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if id != msg.MessageID {
		t.Fatalf("expected message ID %q, got %q", msg.MessageID, id)
	}
	if len(store.AllMessages()) != 1 {
		t.Fatalf("expected 1 stored message, got %d", len(store.AllMessages()))
	}
	if store.AllMessages()[0].MessageID != msg.MessageID {
		t.Fatalf("stored message ID mismatch: %q vs %q", store.AllMessages()[0].MessageID, msg.MessageID)
	}
}

func TestAcceptMessage_SenderMismatch(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)
	imposter := generatePeerID(t)

	msg := validMessage(sender, recipient)
	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, imposter)
	if err == nil {
		t.Fatal("expected error for sender mismatch, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_ExpiredMessage(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	// Set expiry to 1 second in the past
	msg.ExpiryTimestamp = time.Now().Add(-1 * time.Second).UnixMilli()

	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected error for expired message, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_HopCountExceeded(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	msg.HopCount = core.MaxHopCount + 1

	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected error for hop count exceeded, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_PayloadTooLarge(t *testing.T) {
	router, _ := newTestRouter(t, 100)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	msg := validMessage(sender, recipient)
	msg.Payload = make([]byte, core.MaxPayloadSize+1)

	ctx := context.Background()

	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected error for payload too large, got nil")
	}

	var valErr *mta.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestAcceptMessage_RateLimit(t *testing.T) {
	maxRequests := 3
	router, _ := newTestRouter(t, maxRequests)
	sender := generatePeerID(t)
	recipient := generatePeerID(t)

	ctx := context.Background()

	// Send maxRequests messages successfully
	for i := 0; i < maxRequests; i++ {
		msg := validMessage(sender, recipient)
		_, err := router.AcceptMessage(ctx, msg, sender)
		if err != nil {
			t.Fatalf("message %d: expected no error, got: %v", i+1, err)
		}
	}

	// The next message should be rate limited
	msg := validMessage(sender, recipient)
	_, err := router.AcceptMessage(ctx, msg, sender)
	if err == nil {
		t.Fatal("expected rate limit error on 4th message, got nil")
	}

	var rlErr *mta.RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatalf("expected RateLimitError, got %T: %v", err, err)
	}
}
