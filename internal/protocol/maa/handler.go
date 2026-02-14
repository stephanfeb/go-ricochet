package maa

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/protocol/frame"
)

// ProtocolID is the MAA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/access/1.0.0")

// Handler is the Mail Access Agent protocol handler.
type Handler struct {
	mda              *mda.MailboxServer
	logger           *slog.Logger
	rateLimitMu      sync.Mutex
	requestHistory   map[string][]time.Time
	rateLimitWindow  time.Duration
	maxRequests      int
}

// NewHandler creates a new MAA handler.
func NewHandler(mailboxServer *mda.MailboxServer, logger *slog.Logger) *Handler {
	return &Handler{
		mda:             mailboxServer,
		logger:          logger,
		requestHistory:  make(map[string][]time.Time),
		rateLimitWindow: time.Minute,
		maxRequests:     100,
	}
}

// HandleStream handles an incoming access stream.
func (h *Handler) HandleStream(s network.Stream) {
	callerID := s.Conn().RemotePeer()
	defer s.CloseWrite()

	// Read length-prefixed frame
	data, err := frame.ReadFrame(s)
	if err != nil {
		h.logger.Error("failed to read frame", "error", err)
		h.sendError(s, "failed to read request")
		return
	}

	// Determine operation type
	opType, err := frame.GetOperationType(data)
	if err != nil {
		h.logger.Error("failed to get operation type", "error", err)
		h.sendError(s, "invalid request format")
		return
	}

	h.logger.Debug("handling access request", "operation", opType, "caller", callerID.String())

	// Check rate limit
	if !h.checkRateLimit(callerID) {
		h.sendError(s, "rate limit exceeded")
		return
	}

	ctx := context.Background()

	switch opType {
	case "retrieve":
		h.handleRetrieve(ctx, s, data, callerID)
	case "markDelivered":
		h.handleMarkDelivered(ctx, s, data)
	case "updateFlags":
		h.handleUpdateFlags(ctx, s, data)
	case "expunge":
		h.handleExpunge(ctx, s, data, callerID)
	case "deleteMessages":
		h.handleDeleteMessages(ctx, s, data)
	default:
		h.sendError(s, "unknown operation type: "+opType)
	}
}

func (h *Handler) handleRetrieve(ctx context.Context, s network.Stream, data []byte, callerID peer.ID) {
	req, err := frame.DecodeRetrieveRequest(data)
	if err != nil {
		h.sendError(s, "invalid retrieve request")
		return
	}

	// Check if requesting own mailbox or cross-peer read
	isOwnMailbox := req.PeerID == callerID.String()
	mailboxType := core.MailboxPrivate
	if !isOwnMailbox {
		mailboxType = core.MailboxPublic
	}

	folderPath := "inbox"
	if req.FolderPath != "" {
		folderPath = req.FolderPath
	}

	ownerPeerID, err := peer.Decode(req.PeerID)
	if err != nil {
		h.sendError(s, "invalid peer ID")
		return
	}

	addr := &core.MailboxAddress{
		OwnerID:    ownerPeerID,
		FolderPath: folderPath,
		Type:       mailboxType,
	}

	// Convert FromSequence from *uint64 to *int
	var fromSeq *int
	if req.FromSequence != nil {
		v := int(*req.FromSequence)
		fromSeq = &v
	}

	messages, err := h.mda.Retrieve(ctx, addr, callerID, mda.RetrieveOpts{
		FromSequence: fromSeq,
		MaxMessages:  req.MaxMessages,
		MinPriority:  req.MinPriority,
	})
	if err != nil {
		h.logger.Warn("retrieve failed", "error", err)
		messages = nil
	}

	resp := &core.RetrieveResponse{
		Messages: messages,
		HasMore:  false,
	}

	respData, err := frame.EncodeRetrieveResponse(resp)
	if err != nil {
		h.sendError(s, "failed to encode response")
		return
	}

	if err := frame.WriteFrame(s, respData); err != nil {
		h.logger.Error("failed to write response", "error", err)
	}

	h.logger.Info("retrieved messages", "count", len(messages))
}

func (h *Handler) handleMarkDelivered(ctx context.Context, s network.Stream, data []byte) {
	req, err := frame.DecodeMarkDelivered(data)
	if err != nil {
		h.sendError(s, "invalid mark delivered request")
		return
	}

	updatedCount, err := h.mda.Storage.MarkMessagesDelivered(ctx, req.MessageIDs)
	if err != nil {
		h.logger.Error("failed to mark messages delivered", "error", err)
	}

	ack := &core.MarkDeliveredAck{
		Success:      err == nil,
		UpdatedCount: updatedCount,
	}

	ackData, err := frame.EncodeMarkDeliveredAck(ack)
	if err != nil {
		return
	}
	_ = frame.WriteFrame(s, ackData)

	h.logger.Info("marked messages delivered", "count", updatedCount)
}

func (h *Handler) handleUpdateFlags(ctx context.Context, s network.Stream, data []byte) {
	req, err := frame.DecodeUpdateFlags(data)
	if err != nil {
		h.sendError(s, "invalid update flags request")
		return
	}

	success, err := h.mda.Storage.UpdateMessageFlags(ctx, req.MessageID, req.AddFlags, req.RemoveFlags)
	if err != nil {
		h.logger.Error("failed to update flags", "error", err)
	}

	var newFlags *uint32
	if success {
		newFlags, _ = h.mda.Storage.GetMessageFlags(ctx, req.MessageID)
	}

	ack := &core.UpdateFlagsAck{
		Success:  success,
		NewFlags: newFlags,
	}
	if !success {
		ack.ErrorMessage = "message not found"
	}

	ackData, err := frame.EncodeUpdateFlagsAck(ack)
	if err != nil {
		return
	}
	_ = frame.WriteFrame(s, ackData)
}

func (h *Handler) handleExpunge(ctx context.Context, s network.Stream, data []byte, callerID peer.ID) {
	req, err := frame.DecodeExpunge(data)
	if err != nil {
		h.sendError(s, "invalid expunge request")
		return
	}

	// Verify requesting peer matches
	if req.PeerID != callerID.String() {
		h.sendError(s, "unauthorized: can only expunge own mailboxes")
		return
	}

	var deletedCount int
	if req.FolderPath != "" {
		record, err := h.mda.Storage.FindMailbox(ctx, callerID, req.FolderPath)
		if err == nil && record != nil {
			deletedCount, _ = h.mda.Storage.ExpungeMailbox(ctx, record.ID)
		}
	} else {
		deletedCount, _ = h.mda.Storage.ExpungeAllMailboxes(ctx, callerID)
	}

	ack := &core.ExpungeAck{
		Success:      true,
		DeletedCount: deletedCount,
	}

	ackData, err := frame.EncodeExpungeAck(ack)
	if err != nil {
		return
	}
	_ = frame.WriteFrame(s, ackData)

	h.logger.Info("expunged messages", "count", deletedCount)
}

func (h *Handler) handleDeleteMessages(ctx context.Context, s network.Stream, data []byte) {
	req, err := frame.DecodeDeleteMessages(data)
	if err != nil {
		h.sendError(s, "invalid delete messages request")
		return
	}

	err = h.mda.Storage.DeleteMessages(ctx, req.MessageIDs)

	ack := &core.DeleteMessagesAck{
		Success:      err == nil,
		DeletedCount: len(req.MessageIDs),
	}

	ackData, encErr := frame.EncodeDeleteMessagesAck(ack)
	if encErr != nil {
		return
	}
	_ = frame.WriteFrame(s, ackData)

	h.logger.Info("deleted messages", "count", len(req.MessageIDs))
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
	data, err := frame.EncodeError(message)
	if err != nil {
		return
	}
	_ = frame.WriteFrame(s, data)
}
