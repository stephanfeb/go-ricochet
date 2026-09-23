package integration_test

import (
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/stephanfeb/go-p2p-forge/codec"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/protocol/maa"
)

// A peer that opens a stream, declares a ten-megabyte frame and then sends
// nothing used to pin a goroutine and a ten-megabyte buffer for as long as
// it liked; there was no read deadline anywhere on the server side. Now the
// declared size costs nothing until bytes arrive, and the idle timeout from
// connection_timeout closes the stream.
func TestHalfSentFrameIsReapedByTheIdleTimeout(t *testing.T) {
	const idle = 500 * time.Millisecond
	server := newTestServer(t, func(c *core.ServerConfig) { c.ConnectionTimeout = idle })

	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	h := createHost(t, priv)
	t.Cleanup(func() { h.Close() })
	h.Peerstore().AddAddrs(server.PeerID, server.Host.Addrs(), time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := h.NewStream(ctx, server.PeerID, maa.ProtocolID)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer s.Close()

	var header [codec.LengthPrefixSize]byte
	binary.BigEndian.PutUint32(header[:], codec.MaxFrameSize)
	if _, err := s.Write(header[:]); err != nil {
		t.Fatalf("write length prefix: %v", err)
	}

	// The server should give up on us after roughly one idle window: it may
	// send an error envelope first, but then it closes the stream. Give it
	// several windows before calling that a failure.
	s.SetReadDeadline(time.Now().Add(10 * idle))
	start := time.Now()
	_, err = io.ReadAll(s)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("server still holding the half-sent stream after %v (idle timeout %v): %v", elapsed, idle, err)
	}
	if elapsed >= 10*idle {
		t.Fatalf("stream took %v to end (idle timeout %v)", elapsed, idle)
	}
}

// The idle timeout is idle, not total: a normal exchange under the same
// short window still completes.
func TestNormalRequestCompletesUnderAShortIdleTimeout(t *testing.T) {
	server := newTestServer(t, func(c *core.ServerConfig) { c.ConnectionTimeout = 500 * time.Millisecond })
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("prompt enough")); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
}
