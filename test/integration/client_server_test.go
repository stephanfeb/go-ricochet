package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	client "github.com/twostack/go-ricochet/pkg/client"
)

func TestSendAndRetrieve(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	payload := []byte("hello from integration test")

	// Sender sends a message to the recipient's peer ID.
	result, err := sender.SendMessage(ctx, recipient.PeerID(), payload)
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	if !result.Success {
		t.Fatalf("send not successful: %s", result.ErrorMessage)
	}
	if result.MessageID == "" {
		t.Fatal("expected non-empty message ID")
	}

	// Give the server a moment to process.
	time.Sleep(100 * time.Millisecond)

	// Recipient retrieves messages.
	messages, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve messages: %v", err)
	}
	if len(messages) == 0 {
		t.Fatal("expected at least 1 message")
	}

	found := false
	for _, msg := range messages {
		if msg.MessageID == result.MessageID {
			found = true
			if !bytes.Equal(msg.Payload, payload) {
				t.Errorf("payload mismatch: got %q, want %q", msg.Payload, payload)
			}
			break
		}
	}
	if !found {
		t.Errorf("message %s not found in retrieved messages", result.MessageID)
	}
}

func TestSendMultipleMessages(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const count = 5
	sentIDs := make(map[string]bool, count)

	for i := 0; i < count; i++ {
		payload := []byte("message " + string(rune('A'+i)))
		result, err := sender.SendMessage(ctx, recipient.PeerID(), payload)
		if err != nil {
			t.Fatalf("send message %d: %v", i, err)
		}
		if !result.Success {
			t.Fatalf("send %d not successful: %s", i, result.ErrorMessage)
		}
		sentIDs[result.MessageID] = true
	}

	time.Sleep(100 * time.Millisecond)

	messages, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(messages) < count {
		t.Fatalf("expected at least %d messages, got %d", count, len(messages))
	}

	for id := range sentIDs {
		found := false
		for _, msg := range messages {
			if msg.MessageID == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("sent message %s not found", id)
		}
	}
}

func TestSendToFolder(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	payload := []byte("order data")
	result, err := sender.SendMessage(ctx, recipient.PeerID(), payload,
		client.WithFolderPath("orders"))
	if err != nil {
		t.Fatalf("send to folder: %v", err)
	}
	if !result.Success {
		t.Fatalf("send not successful: %s", result.ErrorMessage)
	}

	time.Sleep(100 * time.Millisecond)

	// Retrieve from the specific folder.
	messages, err := recipient.RetrieveMessages(ctx,
		client.WithRetrieveFolderPath("orders"))
	if err != nil {
		t.Fatalf("retrieve from folder: %v", err)
	}
	if len(messages) == 0 {
		t.Fatal("expected messages in orders folder")
	}

	found := false
	for _, msg := range messages {
		if msg.MessageID == result.MessageID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("message not found in orders folder")
	}
}

func TestSendWithPriority(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	payload := []byte("urgent message")
	result, err := sender.SendMessage(ctx, recipient.PeerID(), payload,
		client.WithPriority(core.PriorityUrgent))
	if err != nil {
		t.Fatalf("send with priority: %v", err)
	}
	if !result.Success {
		t.Fatalf("send not successful: %s", result.ErrorMessage)
	}

	time.Sleep(100 * time.Millisecond)

	messages, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}

	for _, msg := range messages {
		if msg.MessageID == result.MessageID {
			if msg.Priority != core.PriorityUrgent {
				t.Errorf("priority: got %d, want %d", msg.Priority, core.PriorityUrgent)
			}
			return
		}
	}
	t.Error("urgent message not found")
}

func TestRetrieveEmptyMailbox(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	messages, err := cl.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve empty: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

func TestDeleteMessages(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("to be deleted"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	ack, err := recipient.DeleteMessages(ctx, []string{result.MessageID})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !ack.Success {
		t.Fatal("delete not successful")
	}

	// Verify message is gone.
	messages, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve after delete: %v", err)
	}
	for _, msg := range messages {
		if msg.MessageID == result.MessageID {
			t.Error("deleted message still present")
		}
	}
}
