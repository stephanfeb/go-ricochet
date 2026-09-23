package wire_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	forge "github.com/stephanfeb/go-p2p-forge"

	"github.com/stephanfeb/go-ricochet/internal/protocol/wire"
)

// recordingHandler keeps every record at Info or above.
type recordingHandler struct {
	records *[]slog.Record
	attrs   []slog.Attr
}

func (h recordingHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }
func (h recordingHandler) Handle(_ context.Context, r slog.Record) error {
	r.AddAttrs(h.attrs...)
	*h.records = append(*h.records, r)
	return nil
}
func (h recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return recordingHandler{records: h.records, attrs: append(h.attrs, attrs...)}
}
func (h recordingHandler) WithGroup(string) slog.Handler { return h }

func attrsOf(r slog.Record) map[string]any {
	m := map[string]any{}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.Any(); return true })
	return m
}

type statused struct{ code int }

func (s statused) StatusCode() int { return s.code }

func TestAccessLogWritesOneInfoLine(t *testing.T) {
	cases := []struct {
		name   string
		handle func(sc *forge.StreamContext)
		op     string
		status int64
	}{
		{"routed op with a status", func(sc *forge.StreamContext) {
			sc.SetOperation("get")
			sc.Response = statused{404}
		}, "get", 404},
		{"response without a status is 200", func(sc *forge.StreamContext) {
			sc.Response = map[string]any{"success": true}
		}, "submit", 200},
		{"pipeline error without a response is classified", func(sc *forge.StreamContext) {
			sc.Err = forge.ErrRateLimited
		}, "submit", 429},
		{"handler narration at Info is not counted here", func(sc *forge.StreamContext) {
			sc.Logger.Debug("doing the thing")
			sc.Response = statused{200}
		}, "submit", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var records []slog.Record
			sc := &forge.StreamContext{
				Ctx:    context.Background(),
				Logger: slog.New(recordingHandler{records: &records}),
			}
			wire.AccessLog("sda", "submit")(sc, func() { tc.handle(sc) })

			if len(records) != 1 {
				t.Fatalf("got %d Info records, want exactly 1", len(records))
			}
			r := records[0]
			if r.Level != slog.LevelInfo || r.Message != "request" {
				t.Errorf("record = %s %q, want INFO \"request\"", r.Level, r.Message)
			}
			a := attrsOf(r)
			if a["protocol"] != "sda" || a["op"] != tc.op || a["status"] != tc.status {
				t.Errorf("attrs = %v, want protocol sda op %s status %d", a, tc.op, tc.status)
			}
			if _, ok := a["duration"].(time.Duration); !ok {
				t.Errorf("duration attr = %v (%T), want a time.Duration", a["duration"], a["duration"])
			}
		})
	}
}

func TestStatusOfPrefersTheResponse(t *testing.T) {
	sc := &forge.StreamContext{Response: statused{507}, Err: errors.New("ignored")}
	if got := wire.StatusOf(sc); got != 507 {
		t.Errorf("StatusOf = %d, want the response's 507", got)
	}
}
