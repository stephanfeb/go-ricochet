package maa

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
	"github.com/twostack/go-ricochet/internal/metrics"
	"github.com/twostack/go-ricochet/internal/protocol/wire"
	"github.com/twostack/go-ricochet/internal/ratelimit"
)

// ProtocolID is the MAA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/access/1.0.0")

// retrievePageBudget is how many bytes of encoded messages one retrieve
// response may carry. The whole page travels as a single frame, and a frame
// over codec.MaxFrameSize is refused by the writer — which used to happen
// after the messages had already been deleted. The margin covers the
// metadata header and the per-message length prefixes.
const retrievePageBudget = codec.MaxFrameSize - 64*1024

// NewPipeline creates a forge pipeline for the Mail Access Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := ratelimit.FromRegistry(reg).MAA

	routes := map[string]forge.Middleware{
		"retrieve":       middleware.Chain(deserializeRetrieve(), handleRetrieve),
		"markDelivered":  middleware.Chain(forge.JSONDeserialize[core.MarkDeliveredRequest](), handleMarkDelivered),
		"updateFlags":    middleware.Chain(forge.JSONDeserialize[core.UpdateFlagsRequest](), handleUpdateFlags),
		"expunge":        middleware.Chain(forge.JSONDeserialize[core.ExpungeRequest](), handleExpunge),
		"deleteMessages": middleware.Chain(forge.JSONDeserialize[core.DeleteMessagesRequest](), handleDeleteMessages),
	}

	return wire.Bounded(forge.NewPipeline(logger,
		metrics.Middleware(metrics.FromRegistry(reg), "maa", ""),
		wire.AccessLog("maa", ""),
		middleware.Recovery(),
		maaResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		wire.RequestDeadline(reg),
		middleware.RateLimitMiddleware(limiter),
		admission.Middleware(admission.FromRegistry(reg)),
		defaultOperationType(),
		middleware.OperationRouter("operationType", routes),
	).WithRegistry(reg), reg)
}

// ErrorResponse is the MAA failure envelope.
//
// It replaces a bare map[string]string{"error": ...}, which gave a client
// nothing but prose: a retrieve rejected for throttling and one rejected
// because the server was full were the same shape, and the caller's only
// option was to slow down for both. The "error" key is unchanged, so a client
// reading only that keeps working.
type ErrorResponse struct {
	Error        string `json:"error"`
	Status       int    `json:"status,omitempty"`
	RetryAfterMs int64  `json:"retryAfterMs,omitempty"`
}

// StatusCode reports the status so the metrics middleware classifies a failed
// retrieve as the kind of failure it was.
func (r *ErrorResponse) StatusCode() int { return r.Status }

// maaResponseWriter writes the response after downstream handlers complete.
// For retrieve operations, sc.Response is a FrameIterator that produces
// multiple length-prefixed frames (metadata + N messages). For all other
// operations, sc.Response is a single JSON-serializable struct.
//
// On pipeline error with no response set, it writes a JSON error frame.
func maaResponseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()

		// Convert pipeline errors into JSON error responses.
		if sc.Err != nil && sc.Response == nil {
			status, retryAfter := wire.Classify(sc.Err)
			wire.LogRejection(sc, status, sc.Err)
			sc.Response = &ErrorResponse{
				Error:        wire.ClientMessage(sc.Err),
				Status:       status,
				RetryAfterMs: wire.RetryAfterMs(retryAfter),
			}
		}

		if sc.Response == nil {
			return
		}

		// Check for pre-encoded raw bytes (compound retrieve response).
		if raw, ok := sc.Response.(rawResponse); ok {
			if err := sc.WriteFrame([]byte(raw)); err != nil {
				sc.Logger.Error("failed to write raw response", "error", err)
			}
			return
		}

		// Single-frame JSON path.
		data, err := json.Marshal(sc.Response)
		if err != nil {
			sc.Logger.Error("failed to marshal response", "error", err)
			return
		}
		if err := sc.WriteFrame(data); err != nil {
			sc.Logger.Error("failed to write response", "error", err)
		}
	}
}

// rawResponse wraps pre-encoded bytes that should be written directly
// as a frame without JSON marshaling.
type rawResponse []byte

// defaultOperationType injects "operationType":"retrieve" into RawBytes when
// the field is missing, preserving backwards compatibility with clients that
// omit the field for retrieve requests.
func defaultOperationType() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		var envelope struct {
			OperationType string `json:"operationType"`
		}
		if err := json.Unmarshal(sc.RawBytes, &envelope); err == nil && envelope.OperationType == "" {
			// Inject the default operationType into the raw JSON.
			var m map[string]json.RawMessage
			if err := json.Unmarshal(sc.RawBytes, &m); err == nil {
				m["operationType"] = json.RawMessage(`"retrieve"`)
				if patched, err := json.Marshal(m); err == nil {
					sc.RawBytes = patched
				}
			}
		}
		next()
	}
}

// deserializeRetrieve decodes a core.RetrieveRequest from sc.RawBytes.
// RetrieveRequest does not carry an operationType field, so we use plain
// JSON unmarshal rather than the generic JSONDeserialize (which would also
// work, but this mirrors the original frame.DecodeRetrieveRequest).
func deserializeRetrieve() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		var req core.RetrieveRequest
		if err := json.Unmarshal(sc.RawBytes, &req); err != nil {
			sc.Err = fmt.Errorf("invalid retrieve request: %w", err)
			return
		}
		sc.Request = &req
		next()
	}
}

// encodeCompoundRetrieveResponse builds the compound retrieve response in the
// wire format expected by the Dart client:
//
//	[4-byte metadata-length][metadata JSON][4-byte msg1-length][msg1 JSON]...
//
// This is a single compound blob written as one length-prefixed frame.
func encodeCompoundRetrieveResponse(messages []*core.Message, hasMore bool) ([]byte, error) {
	meta := map[string]any{
		"messageCount": len(messages),
		"hasMore":      hasMore,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}

	var encodedMsgs [][]byte
	totalSize := 4 + len(metaBytes)
	for _, msg := range messages {
		msgBytes, err := msg.ToJSON()
		if err != nil {
			return nil, err
		}
		encodedMsgs = append(encodedMsgs, msgBytes)
		totalSize += 4 + len(msgBytes)
	}

	buf := make([]byte, 0, totalSize)
	lenBuf := make([]byte, 4)

	binary.BigEndian.PutUint32(lenBuf, uint32(len(metaBytes)))
	buf = append(buf, lenBuf...)
	buf = append(buf, metaBytes...)

	for _, msgBytes := range encodedMsgs {
		binary.BigEndian.PutUint32(lenBuf, uint32(len(msgBytes)))
		buf = append(buf, lenBuf...)
		buf = append(buf, msgBytes...)
	}

	return buf, nil
}

// handleRetrieve retrieves messages from the mailbox.
func handleRetrieve(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.RetrieveRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	folderPath := "inbox"
	if req.FolderPath != "" {
		folderPath = req.FolderPath
	}

	ownerPeerID, err := peer.Decode(req.PeerID)
	if err != nil {
		sc.Response = map[string]string{"error": "invalid peer ID"}
		return
	}

	// The type is whatever the stored record says. It is deliberately not
	// inferred from the caller: an earlier version guessed "public" for any
	// cross-peer read and then auto-created the mailbox with that guess,
	// which let any peer squat another peer's inbox as world-readable.
	addr := &core.MailboxAddress{
		OwnerID:    ownerPeerID,
		FolderPath: folderPath,
	}

	// Convert FromSequence from *uint64 to *int
	var fromSeq *int
	if req.FromSequence != nil {
		v := int(*req.FromSequence)
		fromSeq = &v
	}

	ctx := sc.Ctx
	messages, hasMore, err := mailbox.Retrieve(ctx, addr, callerID, mda.RetrieveOpts{
		FromSequence: fromSeq,
		MaxMessages:  req.MaxMessages,
		MinPriority:  req.MinPriority,
		MaxBytes:     retrievePageBudget,
	})
	if err != nil {
		// Surface the refusal as a classified error envelope rather than an
		// empty inbox. An empty list told a refused reader nothing, and told
		// an owner whose retrieve hit a storage fault that they had no mail.
		sc.Logger.Warn("retrieve failed", "error", err)
		sc.Err = err
		return
	}

	compoundBytes, err := encodeCompoundRetrieveResponse(messages, hasMore)
	if err != nil {
		sc.Logger.Error("failed to encode retrieve response", "error", err)
		sc.Response = map[string]string{"error": "internal encoding error"}
		return
	}

	// Write the compound response as raw bytes. The response writer middleware
	// will wrap this in a length-prefixed frame. We use rawResponse to signal
	// that this is pre-encoded bytes, not a JSON object to marshal.
	sc.Response = rawResponse(compoundBytes)
	sc.Logger.Debug("retrieved messages", "count", len(messages))
}

// handleMarkDelivered marks messages as delivered.
func handleMarkDelivered(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.MarkDeliveredRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")

	ctx := sc.Ctx
	updatedCount, err := mailbox.MarkDelivered(ctx, sc.PeerID, req.MessageIDs)
	if err != nil {
		sc.Logger.Error("failed to mark messages delivered", "error", err)
	}

	sc.Response = &core.MarkDeliveredAck{
		Success:      err == nil,
		UpdatedCount: updatedCount,
	}

	sc.Logger.Debug("marked messages delivered", "count", updatedCount)
}

// handleUpdateFlags updates IMAP-style flags on a message.
func handleUpdateFlags(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.UpdateFlagsRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")

	ctx := sc.Ctx
	newFlags, err := mailbox.UpdateFlags(ctx, sc.PeerID, req.MessageID, req.AddFlags, req.RemoveFlags)
	if err != nil {
		sc.Logger.Error("failed to update flags", "error", err)
	}

	ack := &core.UpdateFlagsAck{
		Success:  err == nil && newFlags != nil,
		NewFlags: newFlags,
	}
	if err != nil {
		ack.ErrorMessage = "failed to update flags"
	} else if newFlags == nil {
		// Also the answer for a message the caller does not own: a refusal
		// that said "exists but not yours" would confirm a guessed ID.
		ack.ErrorMessage = "message not found"
	}

	sc.Response = ack
}

// handleExpunge deletes messages marked with the \Deleted flag.
func handleExpunge(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.ExpungeRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

	// Verify requesting peer matches
	if req.PeerID != callerID.String() {
		sc.Response = map[string]string{"error": "unauthorized: can only expunge own mailboxes"}
		return
	}

	ctx := sc.Ctx
	var deletedCount int
	if req.FolderPath != "" {
		record, err := mailbox.Storage.FindMailbox(ctx, callerID, req.FolderPath)
		if err == nil && record != nil {
			deletedCount, _ = mailbox.Storage.ExpungeMailbox(ctx, record.ID)
		}
	} else {
		deletedCount, _ = mailbox.Storage.ExpungeAllMailboxes(ctx, callerID)
	}

	sc.Response = &core.ExpungeAck{
		Success:      true,
		DeletedCount: deletedCount,
	}

	sc.Logger.Debug("expunged messages", "count", deletedCount)
}

// handleDeleteMessages immediately deletes messages by ID.
func handleDeleteMessages(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.DeleteMessagesRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")

	ctx := sc.Ctx
	deleted, err := mailbox.DeleteMessages(ctx, sc.PeerID, req.MessageIDs)
	if err != nil {
		sc.Logger.Error("failed to delete messages", "error", err)
	}

	// DeletedCount is what was removed, not what was asked. It used to echo
	// the request length, so a caller deleting IDs it did not own — or that
	// never existed — was told they were all gone.
	ack := &core.DeleteMessagesAck{
		Success:      err == nil,
		DeletedCount: deleted,
	}
	if err != nil {
		ack.ErrorMessage = "failed to delete messages"
	}
	sc.Response = ack

	sc.Logger.Debug("deleted messages", "requested", len(req.MessageIDs), "deleted", deleted)
}
