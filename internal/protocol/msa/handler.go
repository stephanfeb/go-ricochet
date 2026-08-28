package msa

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/protocol"

	forge "github.com/twostack/go-p2p-forge"
	"github.com/twostack/go-p2p-forge/codec"
	"github.com/twostack/go-p2p-forge/middleware"

	"github.com/twostack/go-ricochet/internal/admission"
	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mta"
	"github.com/twostack/go-ricochet/internal/ratelimit"
)

// ProtocolID is the MSA protocol identifier.
const ProtocolID = protocol.ID("/sf-network/submit/1.0.0")

// BatchProtocolID carries many submissions in one request.
//
// This is a separate protocol rather than an operation on ProtocolID because
// the single-submit wire format is a bare Message with no envelope to add a
// discriminator to. A new protocol keeps existing clients working untouched.
const BatchProtocolID = protocol.ID("/sf-network/submit/batch/1.0.0")

// maxBatchMessages bounds one batch submission. The request must fit in a
// single frame (codec.MaxFrameSize, 10MB) and payloads travel base64-encoded,
// so this is deliberately well below what the frame could hold.
const maxBatchMessages = 100

// NewPipeline creates a forge pipeline for the Mail Submission Agent.
func NewPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := ratelimit.FromRegistry(reg).MSA

	return forge.NewPipeline(logger,
		middleware.Recovery(),
		ackResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		middleware.RateLimitMiddleware(limiter),
		admission.Middleware(admission.FromRegistry(reg)),
		deserializeMessage(),
		submitHandler,
	).WithRegistry(reg)
}

// BatchSubmitRequest is the JSON request format for a batch submission.
type BatchSubmitRequest struct {
	Messages []json.RawMessage `json:"messages"`
}

// BatchSubmitResponse returns one ack per submitted message, in request order.
type BatchSubmitResponse struct {
	Acks     []core.StoreAck `json:"acks"`
	Accepted int             `json:"accepted"`
	Total    int             `json:"total"`
}

// NewBatchPipeline creates a forge pipeline for batch mail submission.
func NewBatchPipeline(logger *slog.Logger, pool *codec.BufferPool, reg *forge.Registry) *forge.Pipeline {
	limiter := ratelimit.FromRegistry(reg).MSABatch

	return forge.NewPipeline(logger,
		middleware.Recovery(),
		batchResponseWriter(),
		forge.FrameDecodeMiddleware(pool),
		middleware.RateLimitMiddleware(limiter),
		admission.Middleware(admission.FromRegistry(reg)),
		batchSubmitHandler,
	).WithRegistry(reg)
}

// batchResponseWriter writes a BatchSubmitResponse, converting a pipeline error
// into a response so the client always gets one.
func batchResponseWriter() forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		next()

		if sc.Err != nil && sc.Response == nil {
			sc.Response = &BatchSubmitResponse{
				Acks: []core.StoreAck{{Success: false, ErrorMessage: sc.Err.Error()}},
			}
		}
		if sc.Response == nil {
			return
		}

		data, err := json.Marshal(sc.Response)
		if err != nil {
			sc.Logger.Error("failed to marshal batch ack", "error", err)
			return
		}
		if err := codec.WriteFrame(sc.Stream, data); err != nil {
			sc.Logger.Error("failed to write batch ack", "error", err)
		}
	}
}

// batchSubmitHandler routes many messages in one request.
//
// Each message is accepted or rejected on its own and reports its own ack, so
// one undeliverable message -- a full mailbox, an unknown recipient -- does not
// sink the rest of the batch. Messages are routed sequentially: a batch
// commonly targets one mailbox, whose sequence counter serialises writes
// anyway, so fanning out would contend rather than parallelise.
func batchSubmitHandler(sc *forge.StreamContext, next func()) {
	router, _ := forge.ServiceFrom[*mta.Router](sc, "mta")

	var req BatchSubmitRequest
	if err := json.Unmarshal(sc.RawBytes, &req); err != nil {
		sc.Response = &BatchSubmitResponse{
			Acks: []core.StoreAck{{Success: false, ErrorMessage: "invalid batch request"}},
		}
		return
	}

	if len(req.Messages) == 0 {
		sc.Response = &BatchSubmitResponse{
			Acks: []core.StoreAck{{Success: false, ErrorMessage: "messages is required"}},
		}
		return
	}
	if len(req.Messages) > maxBatchMessages {
		sc.Response = &BatchSubmitResponse{
			Acks: []core.StoreAck{{Success: false, ErrorMessage: fmt.Sprintf(
				"batch holds %d messages, maximum is %d", len(req.Messages), maxBatchMessages)}},
		}
		return
	}

	ctx := context.Background()
	acks := make([]core.StoreAck, 0, len(req.Messages))
	accepted := 0

	for i, raw := range req.Messages {
		msg, err := core.MessageFromJSON(raw)
		if err != nil {
			acks = append(acks, core.StoreAck{
				Success:      false,
				ErrorMessage: fmt.Sprintf("message %d: %v", i, err),
			})
			continue
		}

		// Same authorization rule as single submit: the sender must be the peer
		// on the connection. Checked per message so a batch cannot smuggle a
		// forged sender in alongside honest ones.
		if msg.SenderPeerID != sc.PeerID.String() {
			acks = append(acks, core.StoreAck{
				MessageID:    msg.MessageID,
				Success:      false,
				ErrorMessage: "unauthorized: sender does not match connection",
			})
			continue
		}

		messageID, err := router.AcceptMessage(ctx, msg, sc.PeerID)
		ack := core.StoreAck{MessageID: messageID, Success: err == nil}
		if err != nil {
			ack.ErrorMessage = err.Error()
		} else {
			accepted++
		}
		acks = append(acks, ack)
	}

	sc.Logger.Info("batch submit complete",
		"from", sc.PeerID.String(),
		"messages", len(req.Messages),
		"accepted", accepted,
	)

	sc.Response = &BatchSubmitResponse{
		Acks:     acks,
		Accepted: accepted,
		Total:    len(req.Messages),
	}
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
