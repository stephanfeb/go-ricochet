package wire

import (
	"log/slog"

	forge "github.com/twostack/go-p2p-forge"
)

// LogRejection records the real error behind a failed request. The client
// sees ClientMessage(err); this is where the full text goes, tagged by the
// stream context's logger with the peer and protocol and here with the
// stream id, so an operator can match a client's report to the cause.
//
// A 5xx is the server's fault and logged at Error; anything else is the
// client's and logged at Warn, since a flood of client mistakes is worth
// seeing but is not an outage.
func LogRejection(sc *forge.StreamContext, status int, err error) {
	if err == nil {
		return
	}
	attrs := []any{"status", status, "error", err}
	if sc.Stream != nil {
		attrs = append(attrs, "stream", sc.Stream.ID())
	}
	level := slog.LevelWarn
	if status >= StatusInternalError {
		level = slog.LevelError
	}
	sc.Logger.Log(sc.Ctx, level, "request rejected", attrs...)
}
