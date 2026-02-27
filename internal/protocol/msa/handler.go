package msa

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mta"
)

// ProtocolID is the MSA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/submit/1.0.0")

// NewPipeline creates a forge pipeline for the Mail Submission Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := middleware.NewSingleBucket(time.Minute, 100)

	return forge.NewPipeline(logger,
		middleware.Recovery(),
		ackResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		middleware.RateLimitMiddleware(limiter),
		deserializeMessage(),
		submitHandler,
	).WithRegistry(reg)
}

// ackResponseWriter writes a StoreAck response. On pipeline error, it
// converts the error into an error ack so the client always gets a response.
func ackResponseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()

		// Convert pipeline errors into error ack responses.
		if sc.Err != nil && sc.Response == nil {
			sc.Response = &core.StoreAck{
				Success:      false,
				ErrorMessage: sc.Err.Error(),
			}
		}

		if sc.Response == nil {
			return
		}

		data, err := json.Marshal(sc.Response)
		if err != nil {
			sc.Logger.Error("failed to marshal ack", "error", err)
			return
		}
		if err := codec.WriteFrame(sc.Stream, data); err != nil {
			sc.Logger.Error("failed to write ack", "error", err)
		}
	}
}

// deserializeMessage decodes a core.Message from sc.RawBytes using the
// custom MessageFromJSON decoder (handles base64 payload field).
func deserializeMessage() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		msg, err := core.MessageFromJSON(sc.RawBytes)
		if err != nil {
			sc.Err = err
			sc.Logger.Error("failed to decode message", "error", err)
			return
		}
		sc.Request = msg
		next()
	}
}

func submitHandler(sc *forge.StreamContext, next func()) {
	router, _ := forge.ServiceFrom[*mta.Router](sc, "mta")
	msg := sc.Request.(*core.Message)

	// Validate sender matches caller
	if msg.SenderPeerID != sc.PeerID.String() {
		sc.Response = &core.StoreAck{
			Success:      false,
			ErrorMessage: "unauthorized: sender does not match connection",
		}
		return
	}

	sc.Logger.Info("submitting message",
		"message_id", msg.MessageID,
		"from", msg.SenderPeerID,
		"to", msg.RecipientPeerID,
	)

	// Hand off to MTA for routing and delivery
	ctx := context.Background()
	messageID, err := router.AcceptMessage(ctx, msg, sc.PeerID)

	ack := &core.StoreAck{
		MessageID: messageID,
		Success:   err == nil,
	}
	if err != nil {
		ack.ErrorMessage = err.Error()
	}

	sc.Response = ack
}
