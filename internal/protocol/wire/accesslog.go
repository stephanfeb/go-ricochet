package wire

import (
	"time"

	forge "github.com/stephanfeb/go-p2p-forge"
)

// statusCoder is implemented by every response type, so the access line can
// report what the client was told without knowing the protocol.
type statusCoder interface{ StatusCode() int }

// AccessLog writes the one Info line a request produces: protocol, operation,
// status, peer, stream and duration, after the response has been written.
//
// It sits at the top of every pipeline, so it sees the routed operation and
// the final response, and it is the only Info line on the hot path; the
// handlers' own narration is Debug. A request that failed also gets a Warn or
// Error from LogRejection with the cause, tagged with the same stream id.
//
// fallbackOp names the operation for pipelines that have no router and serve
// exactly one, the way metrics.Middleware takes it.
func AccessLog(protocol, fallbackOp string) forge.Middleware {
	return func(sc *forge.StreamContext, next func()) {
		start := time.Now()
		next()

		attrs := []any{
			"protocol", protocol,
			"op", operationOf(sc, fallbackOp),
			"status", StatusOf(sc),
			"peer", sc.PeerID,
			"duration", time.Since(start),
		}
		if sc.Stream != nil {
			attrs = append(attrs, "stream", sc.Stream.ID())
		}
		sc.Logger.Info("request", attrs...)
	}
}

// StatusOf is the status the client was given: the response's own if it
// reports one, the classification of the pipeline error if the request never
// produced a response, and 200 for a response that carries no status.
func StatusOf(sc *forge.StreamContext) int {
	if r, ok := sc.Response.(statusCoder); ok && r.StatusCode() != 0 {
		return r.StatusCode()
	}
	if sc.Response == nil && sc.Err != nil {
		status, _ := Classify(sc.Err)
		return status
	}
	return StatusOK
}

func operationOf(sc *forge.StreamContext, fallback string) string {
	if op := sc.Operation(); op != "" {
		return op
	}
	if fallback != "" {
		return fallback
	}
	return "unrouted"
}
