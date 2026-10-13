package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/stephanfeb/go-ricochet/internal/core"
	"github.com/stephanfeb/go-ricochet/internal/transports"
)

// One server runs every supported transport, and a client that speaks only
// one of them stores and retrieves a message over it. The handlers never
// see the transport; this is the check that nothing between them and the
// wire assumes UDX.
func TestStoreAndForwardOverEveryTransport(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.Transports = transports.Known()
	})

	for _, transport := range transports.Known() {
		t.Run(transport, func(t *testing.T) {
			newClient := func() crypto.PrivKey {
				priv, _, err := crypto.GenerateEd25519Key(nil)
				if err != nil {
					t.Fatal(err)
				}
				return priv
			}
			sender := newTestClientOver(t, server, newClient(), transport)
			recipient := newTestClientOver(t, server, newClient(), transport)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			payload := []byte("stored and forwarded over " + transport)
			result, err := sender.SendMessage(ctx, recipient.PeerID(), payload)
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			if !result.Success {
				t.Fatalf("send not successful: %s", result.ErrorMessage)
			}

			messages, err := recipient.RetrieveMessages(ctx)
			if err != nil {
				t.Fatalf("retrieve: %v", err)
			}
			found := false
			for _, msg := range messages {
				if msg.MessageID == result.MessageID {
					found = bytes.Equal(msg.Payload, payload)
				}
			}
			if !found {
				t.Fatalf("message %s not retrieved intact", result.MessageID)
			}

			conns := server.Host.Network().ConnsToPeer(recipient.PeerID())
			if len(conns) == 0 {
				t.Fatal("server holds no connection to the recipient")
			}
			for _, c := range conns {
				if got, err := transports.ForAddress(c.LocalMultiaddr()); err != nil || got != transport {
					t.Errorf("connection runs over %q (%v), want %q", got, err, transport)
				}
			}
		})
	}
}
