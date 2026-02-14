package msa

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/protocol/frame"
)

// ProtocolID is the MSA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/submit/1.0.0")

// Handler is the Mail Submission Agent protocol handler.
type Handler struct {
	router           *mta.Router
	logger           *slog.Logger
	rateLimitMu      sync.Mutex
	requestHistory   map[string][]time.Time
	rateLimitWindow  time.Duration
	maxRequests      int
}

// NewHandler creates a new MSA handler.
func NewHandler(router *mta.Router, logger *slog.Logger) *Handler {
	return &Handler{
		router:          router,
		logger:          logger,
		requestHistory:  make(map[string][]time.Time),
		rateLimitWindow: time.Minute,
		maxRequests:     100,
	}
}

// HandleStream handles an incoming submission stream.
func (h *Handler) HandleStream(s network.Stream) {
	callerID := s.Conn().RemotePeer()
	defer s.Close()

	h.logger.Debug("handling submission", "caller", callerID.String())

	// Check rate limit
	if !h.checkRateLimit(callerID) {
		h.sendError(s, "rate limit exceeded")
		return
	}

	// Read length-prefixed frame
	data, err := frame.ReadFrame(s)
	if err != nil {
		h.logger.Error("failed to read frame", "error", err)
		h.sendError(s, "failed to read message")
		return
	}

	// Decode message
	msg, err := frame.DecodeMessage(data)
	if err != nil {
		h.logger.Error("failed to decode message", "error", err)
		h.sendError(s, "invalid message format")
		return
	}

	// Validate sender matches caller
	if msg.SenderPeerID != callerID.String() {
		h.sendError(s, "unauthorized: sender does not match connection")
		return
	}

	h.logger.Info("submitting message",
		"message_id", msg.MessageID,
		"from", msg.SenderPeerID,
		"to", msg.RecipientPeerID,
	)

	// Hand off to MTA for routing and delivery
	ctx := context.Background()
	messageID, err := h.router.AcceptMessage(ctx, msg, callerID)

	// Send acknowledgment
	ack := &core.StoreAck{
		MessageID: messageID,
		Success:   err == nil,
	}
	if err != nil {
		ack.ErrorMessage = err.Error()
	}

	ackData, encErr := frame.EncodeStoreAck(ack)
	if encErr != nil {
		h.logger.Error("failed to encode ack", "error", encErr)
		return
	}

	if writeErr := frame.WriteFrame(s, ackData); writeErr != nil {
		h.logger.Error("failed to write ack", "error", writeErr)
	}
}

func (h *Handler) checkRateLimit(peerID peer.ID) bool {
	h.rateLimitMu.Lock()
	defer h.rateLimitMu.Unlock()

	now := time.Now()
	key := peerID.String()
	cutoff := now.Add(-h.rateLimitWindow)

	history := h.requestHistory[key]
	filtered := history[:0]
	for _, ts := range history {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}

	if len(filtered) >= h.maxRequests {
		h.requestHistory[key] = filtered
		return false
	}

	h.requestHistory[key] = append(filtered, now)
	return true
}

func (h *Handler) sendError(s network.Stream, message string) {
	ack := &core.StoreAck{
		Success:      false,
		ErrorMessage: message,
	}

	data, err := frame.EncodeStoreAck(ack)
	if err != nil {
		return
	}
	_ = frame.WriteFrame(s, data)
}
