package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
)

func TestMarkDelivered(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("mark me"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	ack, err := recipient.MarkDelivered(ctx, []string{result.MessageID})
	if err != nil {
		t.Fatalf("mark delivered: %v", err)
	}
	if !ack.Success {
		t.Fatal("mark delivered not successful")
	}
	if ack.UpdatedCount != 1 {
		t.Errorf("updated count: got %d, want 1", ack.UpdatedCount)
	}
}

func TestUpdateFlags_Add(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("flag me"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	ack, err := recipient.UpdateFlags(ctx, result.MessageID, uint32(core.MsgFlagFlagged), 0)
	if err != nil {
		t.Fatalf("update flags: %v", err)
	}
	if !ack.Success {
		t.Fatal("update flags not successful")
	}
	if ack.NewFlags == nil {
		t.Fatal("expected new flags to be returned")
	}
	if *ack.NewFlags&uint32(core.MsgFlagFlagged) == 0 {
		t.Error("Flagged flag not set")
	}
}

func TestUpdateFlags_Remove(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("flag then unflag"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Add flag.
	_, err = recipient.UpdateFlags(ctx, result.MessageID, uint32(core.MsgFlagFlagged), 0)
	if err != nil {
		t.Fatalf("add flag: %v", err)
	}

	// Remove flag.
	ack, err := recipient.UpdateFlags(ctx, result.MessageID, 0, uint32(core.MsgFlagFlagged))
	if err != nil {
		t.Fatalf("remove flag: %v", err)
	}
	if !ack.Success {
		t.Fatal("remove flag not successful")
	}
	if ack.NewFlags != nil && *ack.NewFlags&uint32(core.MsgFlagFlagged) != 0 {
		t.Error("Flagged flag still set after removal")
	}
}

func TestExpunge(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("expunge me"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Mark as deleted.
	_, err = recipient.UpdateFlags(ctx, result.MessageID, uint32(core.MsgFlagDeleted), 0)
	if err != nil {
		t.Fatalf("set deleted flag: %v", err)
	}

	// Expunge.
	ack, err := recipient.Expunge(ctx)
	if err != nil {
		t.Fatalf("expunge: %v", err)
	}
	if !ack.Success {
		t.Fatal("expunge not successful")
	}
	if ack.DeletedCount < 1 {
		t.Errorf("expected at least 1 deleted, got %d", ack.DeletedCount)
	}

	// Verify removed.
	messages, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve after expunge: %v", err)
	}
	for _, msg := range messages {
		if msg.MessageID == result.MessageID {
			t.Error("expunged message still present")
		}
	}
}
