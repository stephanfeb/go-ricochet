package mta

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
)

// Router is the Mail Transfer Agent — handles routing, validation, and rate limiting.
type Router struct {
	mda              *mda.MailboxServer
	rateLimitWindow  time.Duration
	maxRequests      int
	rateLimitHistory map[string][]time.Time
	mu               sync.Mutex
	logger           *slog.Logger
}

// NewRouter creates a new MTA router.
func NewRouter(mailboxServer *mda.MailboxServer, rateLimitWindow time.Duration, maxRequests int, logger *slog.Logger) *Router {
	return &Router{
		mda:              mailboxServer,
		rateLimitWindow:  rateLimitWindow,
		maxRequests:      maxRequests,
		rateLimitHistory: make(map[string][]time.Time),
		logger:           logger,
	}
}

// RetrieveOpts holds options for message retrieval.
type RetrieveOpts struct {
	FromSequence *int
	MaxMessages  *int
	MinPriority  *core.MessagePriority
}

// AcceptMessage validates and delivers a message to the local MDA.
func (r *Router) AcceptMessage(ctx context.Context, msg *core.Message, senderID peer.ID) (string, error) {
	// Validate message
	if err := r.validateMessage(msg, senderID); err != nil {
		return "", err
	}

	// Check rate limit
	if !r.checkRateLimit(senderID) {
		return "", &RateLimitError{PeerID: senderID.String()}
	}

	// Deliver to local MDA
	if _, err := r.mda.DeliverLocal(ctx, msg); err != nil {
		return "", fmt.Errorf("deliver message: %w", err)
	}

	r.logger.Info("message routed to local MDA",
		"message_id", msg.MessageID,
		"recipient", msg.RecipientPeerID,
	)

	return msg.MessageID, nil
}

// HandleRetrieve forwards a retrieve request to the MDA.
func (r *Router) HandleRetrieve(ctx context.Context, addr *core.MailboxAddress, callerID peer.ID, opts RetrieveOpts) ([]*core.Message, error) {
	// Check rate limit
	if !r.checkRateLimit(callerID) {
		return nil, &RateLimitError{PeerID: callerID.String()}
	}

	messages, err := r.mda.Retrieve(ctx, addr, callerID, mda.RetrieveOpts{
		FromSequence: opts.FromSequence,
		MaxMessages:  opts.MaxMessages,
		MinPriority:  opts.MinPriority,
	})
	if err != nil {
		return nil, err
	}

	r.logger.Info("retrieved messages",
		"count", len(messages),
		"caller", callerID.String(),
		"mailbox", addr.FullPath(),
	)

	return messages, nil
}

func (r *Router) validateMessage(msg *core.Message, senderID peer.ID) error {
	// Verify sender matches
	if msg.SenderPeerID != senderID.String() {
		return &ValidationError{Message: fmt.Sprintf("sender mismatch: expected %s, got %s", senderID, msg.SenderPeerID)}
	}

	// Check if message already expired
	if msg.IsExpired() {
		return &ValidationError{Message: fmt.Sprintf("message already expired: %s", msg.MessageID)}
	}

	// Check hop count
	if msg.HopCount > core.MaxHopCount {
		return &ValidationError{Message: fmt.Sprintf("hop count exceeded: %d > %d", msg.HopCount, core.MaxHopCount)}
	}

	// Check payload size
	if len(msg.Payload) > core.MaxPayloadSize {
		return &ValidationError{Message: fmt.Sprintf("payload too large: %d bytes", len(msg.Payload))}
	}

	return nil
}

func (r *Router) checkRateLimit(peerID peer.ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	key := peerID.String()
	cutoff := now.Add(-r.rateLimitWindow)

	history := r.rateLimitHistory[key]

	// Remove old entries
	filtered := history[:0]
	for _, ts := range history {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}

	if len(filtered) >= r.maxRequests {
		r.logger.Warn("rate limit exceeded", "peer", key[:12])
		r.rateLimitHistory[key] = filtered
		return false
	}

	r.rateLimitHistory[key] = append(filtered, now)
	return true
}

// GetStats returns router statistics.
func (r *Router) GetStats() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return map[string]any{
		"rateLimitedPeers":     len(r.rateLimitHistory),
		"rateLimitWindow":      r.rateLimitWindow.Seconds(),
		"maxRequestsPerWindow": r.maxRequests,
	}
}

// RateLimitError indicates a rate limit was exceeded.
type RateLimitError struct {
	PeerID string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limit exceeded for peer %s", e.PeerID)
}

// ValidationError indicates a message validation failure.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: %s", e.Message)
}
