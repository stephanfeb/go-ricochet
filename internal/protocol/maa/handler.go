package maa

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda"
)

// ProtocolID is the MAA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/access/1.0.0")

// NewPipeline creates a forge pipeline for the Mail Access Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := middleware.NewSingleBucket(time.Minute, 100)

	routes := map[string]forge.Middleware{
		"retrieve":       middleware.Chain(deserializeRetrieve(), handleRetrieve),
		"markDelivered":  middleware.Chain(forge.JSONDeserialize[core.MarkDeliveredRequest](), handleMarkDelivered),
		"updateFlags":    middleware.Chain(forge.JSONDeserialize[core.UpdateFlagsRequest](), handleUpdateFlags),
		"expunge":        middleware.Chain(forge.JSONDeserialize[core.ExpungeRequest](), handleExpunge),
		"deleteMessages": middleware.Chain(forge.JSONDeserialize[core.DeleteMessagesRequest](), handleDeleteMessages),
	}

	return forge.NewPipeline(logger,
		middleware.Recovery(),
		maaResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		middleware.RateLimitMiddleware(limiter),
		defaultOperationType(),
		middleware.OperationRouter("operationType", routes),
	).WithRegistry(reg)
}

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
			sc.Response = map[string]string{"error": sc.Err.Error()}
		}

		if sc.Response == nil {
			return
		}

		// Multi-frame path: retrieve response implements FrameIterator.
		if iter, ok := sc.Response.(forge.FrameIterator); ok {
			for {
				data, err := iter.Next()
				if err == io.EOF {
					return
				}
				if err != nil {
					sc.Logger.Error("failed to produce frame", "error", err)
					return
				}
				if err := codec.WriteFrame(sc.Stream, data); err != nil {
					sc.Logger.Error("failed to write frame", "error", err)
					return
				}
			}
		}

		// Single-frame path.
		data, err := json.Marshal(sc.Response)
		if err != nil {
			sc.Logger.Error("failed to marshal response", "error", err)
			return
		}
		if err := codec.WriteFrame(sc.Stream, data); err != nil {
			sc.Logger.Error("failed to write response", "error", err)
		}
	}
}

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

// retrieveIterator implements forge.FrameIterator for the compound retrieve
// response: one metadata frame followed by N message frames.
//
// The metadata frame is JSON: {"messageCount": N, "hasMore": bool}
// Each message frame is produced by Message.ToJSON() (handles base64 payload).
type retrieveIterator struct {
	metadata *retrieveMetadata
	messages []*core.Message
	idx      int // -1 = metadata not yet sent, 0..N-1 = message index
}

type retrieveMetadata struct {
	MessageCount int  `json:"messageCount"`
	HasMore      bool `json:"hasMore"`
}

func newRetrieveIterator(messages []*core.Message, hasMore bool) *retrieveIterator {
	return &retrieveIterator{
		metadata: &retrieveMetadata{
			MessageCount: len(messages),
			HasMore:      hasMore,
		},
		messages: messages,
		idx:      -1,
	}
}

func (ri *retrieveIterator) Next() ([]byte, error) {
	if ri.idx == -1 {
		ri.idx = 0
		return json.Marshal(ri.metadata)
	}
	if ri.idx >= len(ri.messages) {
		return nil, io.EOF
	}
	msg := ri.messages[ri.idx]
	ri.idx++
	return msg.ToJSON()
}

// handleRetrieve retrieves messages from the mailbox.
func handleRetrieve(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.RetrieveRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")
	callerID := sc.PeerID

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
		sc.Response = map[string]string{"error": "invalid peer ID"}
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

	ctx := context.Background()
	messages, err := mailbox.Retrieve(ctx, addr, callerID, mda.RetrieveOpts{
		FromSequence: fromSeq,
		MaxMessages:  req.MaxMessages,
		MinPriority:  req.MinPriority,
	})
	if err != nil {
		sc.Logger.Warn("retrieve failed", "error", err)
		messages = nil
	}

	sc.Response = newRetrieveIterator(messages, false)
	sc.Logger.Info("retrieved messages", "count", len(messages))
}

// handleMarkDelivered marks messages as delivered.
func handleMarkDelivered(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.MarkDeliveredRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")

	ctx := context.Background()
	updatedCount, err := mailbox.Storage.MarkMessagesDelivered(ctx, req.MessageIDs)
	if err != nil {
		sc.Logger.Error("failed to mark messages delivered", "error", err)
	}

	sc.Response = &core.MarkDeliveredAck{
		Success:      err == nil,
		UpdatedCount: updatedCount,
	}

	sc.Logger.Info("marked messages delivered", "count", updatedCount)
}

// handleUpdateFlags updates IMAP-style flags on a message.
func handleUpdateFlags(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.UpdateFlagsRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")

	ctx := context.Background()
	success, err := mailbox.Storage.UpdateMessageFlags(ctx, req.MessageID, req.AddFlags, req.RemoveFlags)
	if err != nil {
		sc.Logger.Error("failed to update flags", "error", err)
	}

	var newFlags *uint32
	if success {
		newFlags, _ = mailbox.Storage.GetMessageFlags(ctx, req.MessageID)
	}

	ack := &core.UpdateFlagsAck{
		Success:  success,
		NewFlags: newFlags,
	}
	if !success {
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

	ctx := context.Background()
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

	sc.Logger.Info("expunged messages", "count", deletedCount)
}

// handleDeleteMessages immediately deletes messages by ID.
func handleDeleteMessages(sc *forge.StreamContext, next func()) {
	req := sc.Request.(*core.DeleteMessagesRequest)
	mailbox, _ := forge.ServiceFrom[*mda.MailboxServer](sc, "mda")

	ctx := context.Background()
	err := mailbox.Storage.DeleteMessages(ctx, req.MessageIDs)

	sc.Response = &core.DeleteMessagesAck{
		Success:      err == nil,
		DeletedCount: len(req.MessageIDs),
	}

	sc.Logger.Info("deleted messages", "count", len(req.MessageIDs))
}
