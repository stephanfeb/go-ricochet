package mta

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/trust"
)

// Router is the Mail Transfer Agent — handles routing, validation, and rate limiting.
type Router struct {
	mda     *mda.MailboxServer
	limiter middleware.Limiter

	// forwarding is on when enable_forwarding is set; forwarders is the
	// trusted set that may use it.
	forwarding bool
	forwarders *trust.Peers
	logger     *slog.Logger
}

// NewRouter creates a new MTA router.
//
// The limiter is a backstop charged once per message, sitting behind the
// per-request limits the protocol handlers apply. A batch submission therefore
// costs one request at the MSA edge but one unit here per message it carries.
// Passing nil disables it, leaving the protocol handlers as the only limit.
func NewRouter(mailboxServer *mda.MailboxServer, limiter middleware.Limiter, logger *slog.Logger) *Router {
	return &Router{
		mda:     mailboxServer,
		limiter: limiter,
		logger:  logger,
	}
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

	r.logger.Debug("message routed to local MDA",
		"message_id", msg.MessageID,
		"recipient", msg.RecipientPeerID,
	)

	return msg.MessageID, nil
}

// WithForwarding enables forwarded submissions from the trusted peers.
func (r *Router) WithForwarding(enabled bool, forwarders *trust.Peers) *Router {
	r.forwarding = enabled
	r.forwarders = forwarders
	return r
}

// ForwardingEnabled reports whether this server accepts forwarded mail.
func (r *Router) ForwardingEnabled() bool { return r.forwarding }

// AcceptForwarded delivers a message submitted by a peer other than its
// sender: a forwarder relaying on the sender's behalf. It is the
// enable_forwarding feature, and the only way a submission whose sender is
// not the connection's peer is accepted.
//
// Only trusted peers may forward, since a forwarded message carries a
// sender the server cannot verify. The message is marked forwarded and its
// hop count advanced, so a loop between forwarders runs out at MaxHopCount
// rather than forever.
func (r *Router) AcceptForwarded(ctx context.Context, msg *core.Message, forwarderID peer.ID) (string, error) {
	if !r.forwarding {
		return "", &ForwardingRefusedError{Reason: "forwarding is not enabled on this server"}
	}
	if !r.forwarders.Contains(forwarderID) {
		return "", &ForwardingRefusedError{Reason: "forwarder is not a trusted peer"}
	}
	if _, err := peer.Decode(msg.SenderPeerID); err != nil {
		return "", &ValidationError{Message: fmt.Sprintf("invalid sender peer ID: %v", err)}
	}

	forwarded, err := msg.WithIncrementedHopCount()
	if err != nil {
		return "", &ValidationError{Message: err.Error()}
	}
	forwarded.Flags |= core.FlagForwarded

	// The forwarder, not the original sender, is charged for the submission:
	// it is the peer on the wire, and the one whose behaviour a limit can
	// change.
	if !r.checkRateLimit(forwarderID) {
		return "", &RateLimitError{PeerID: forwarderID.String()}
	}
	if err := r.validateForwarded(forwarded); err != nil {
		return "", err
	}
	if _, err := r.mda.DeliverLocal(ctx, forwarded); err != nil {
		return "", fmt.Errorf("deliver forwarded message: %w", err)
	}

	r.logger.Debug("forwarded message routed to local MDA",
		"message_id", forwarded.MessageID,
		"forwarder", forwarderID.String(),
		"sender", forwarded.SenderPeerID,
		"recipient", forwarded.RecipientPeerID,
		"hops", forwarded.HopCount,
	)
	return forwarded.MessageID, nil
}

// validateForwarded is validateMessage without the sender-matches-caller
// rule, which forwarding exists to relax.
func (r *Router) validateForwarded(msg *core.Message) error {
	if msg.IsExpired() {
		return &ValidationError{Message: fmt.Sprintf("message already expired: %s", msg.MessageID)}
	}
	if msg.HopCount > core.MaxHopCount {
		return &ValidationError{Message: fmt.Sprintf("hop count exceeded: %d > %d", msg.HopCount, core.MaxHopCount)}
	}
	if len(msg.Payload) > core.MaxPayloadSize {
		return &ValidationError{Message: fmt.Sprintf("payload too large: %d bytes", len(msg.Payload))}
	}
	return nil
}

// ForwardingRefusedError is a submission on someone else's behalf that this
// server will not take: forwarding is off, or the forwarder is not trusted.
// It is a 403.
type ForwardingRefusedError struct {
	Reason string
}

func (e *ForwardingRefusedError) Error() string {
	return "forwarding refused: " + e.Reason
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
	if r.limiter == nil {
		return true
	}
	return r.limiter.Allow(peerID)
}

// GetStats returns router statistics.
func (r *Router) GetStats() map[string]any {
	stats := map[string]any{
		"rateLimitingEnabled": r.limiter != nil,
	}
	if tracker, ok := r.limiter.(interface{ TrackedPeers() int }); ok {
		stats["rateLimitedPeers"] = tracker.TrackedPeers()
	}
	return stats
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
