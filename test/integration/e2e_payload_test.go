package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

// End-to-end encryption and compression are applied by the sending client and
// undone by the receiving one, keyed on the message's protocol flags. Those
// flags used to be dropped by storage — only the IMAP flags were persisted and
// the row scanner hard-coded the protocol flags to none — so an encrypted
// message came back as ciphertext with nothing to say so, and the client never
// decrypted it. The README's headline feature did not work through the server.
//
// These tests go through a real server and a real database, which is the
// only place the bug was visible: the client's own primitives round-tripped
// fine in unit tests.
func TestEncryptedMessageRoundTripsThroughTheServer(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plaintext := []byte("for the recipient's eyes only")
	if _, err := sender.SendMessage(ctx, recipient.PeerID(), plaintext, client.WithEncryption()); err != nil {
		t.Fatalf("send encrypted: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if !bytes.Equal(msgs[0].Payload, plaintext) {
		t.Fatalf("payload did not decrypt: got %q (%d bytes), want %q", truncate(msgs[0].Payload), len(msgs[0].Payload), plaintext)
	}
	if msgs[0].Flags.IsEncrypted() {
		t.Errorf("encrypted flag still set after client-side decryption")
	}
}

// Compression is applied before encryption on send and undone in reverse on
// receive; both flags have to survive storage for either to be undone.
func TestCompressedAndEncryptedMessageRoundTrips(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Well over the compression threshold and highly compressible, so the
	// compressed flag is actually set rather than skipped as not worth it.
	plaintext := bytes.Repeat([]byte("ricochet "), 4096)
	if _, err := sender.SendMessage(ctx, recipient.PeerID(), plaintext,
		client.WithCompression(), client.WithEncryption()); err != nil {
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
	if !bytes.Equal(msgs[0].Payload, plaintext) {
		t.Fatalf("payload did not round-trip: got %d bytes, want %d", len(msgs[0].Payload), len(plaintext))
	}
	if msgs[0].Flags.IsEncrypted() || msgs[0].Flags.IsCompressed() {
		t.Errorf("flags %v still set after the client undid them", msgs[0].Flags)
	}
}

// Compression alone, so the flag survives storage independently of encryption.
func TestCompressedMessageRoundTrips(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plaintext := bytes.Repeat([]byte("compress me "), 2048)
	if _, err := sender.SendMessage(ctx, recipient.PeerID(), plaintext, client.WithCompression()); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != 1 || !bytes.Equal(msgs[0].Payload, plaintext) {
		t.Fatalf("compressed payload did not round-trip (%d messages)", len(msgs))
	}
}

func truncate(b []byte) []byte {
	if len(b) > 32 {
		return b[:32]
	}
	return b
}

// The ciphertext is bound to the folder the message was sent to, and the
// server returns the message in that folder, so the binding checks out
// through a real delivery rather than only in the client's own tests.
func TestEncryptedMessageBoundToItsFolderRoundTrips(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plaintext := []byte("filed under work")
	if _, err := sender.SendMessage(ctx, recipient.PeerID(), plaintext,
		client.WithEncryption(), client.WithFolderPath("work")); err != nil {
		t.Fatalf("send encrypted to folder: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	msgs, err := recipient.RetrieveMessages(ctx, client.WithRetrieveFolderPath("work"))
	if err != nil {
		t.Fatalf("retrieve from folder: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages in work, want 1", len(msgs))
	}
	if !bytes.Equal(msgs[0].Payload, plaintext) {
		t.Fatalf("payload did not decrypt: got %q, want %q", truncate(msgs[0].Payload), plaintext)
	}
}
