package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

func TestCreateAndListMailboxes(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create mailboxes of each type.
	err := cl.CreateMailbox(ctx, "test-private", core.MailboxPrivate)
	if err != nil {
		t.Fatalf("create private: %v", err)
	}

	err = cl.CreateMailbox(ctx, "test-shared", core.MailboxShared)
	if err != nil {
		t.Fatalf("create shared: %v", err)
	}

	err = cl.CreateMailbox(ctx, "test-public", core.MailboxPublic,
		client.WithMailboxMaxMessages(500),
		client.WithRetentionDays(7))
	if err != nil {
		t.Fatalf("create public: %v", err)
	}

	// List and verify.
	mailboxes, err := cl.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("list mailboxes: %v", err)
	}

	found := make(map[string]bool)
	for _, mb := range mailboxes {
		found[mb.FolderPath] = true
	}

	for _, name := range []string{"test-private", "test-shared", "test-public"} {
		if !found[name] {
			t.Errorf("mailbox %s not found in listing", name)
		}
	}
}

func TestDeleteMailbox(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := cl.CreateMailbox(ctx, "to-delete", core.MailboxPrivate)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	err = cl.DeleteMailbox(ctx, "to-delete")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify it's gone from the listing.
	mailboxes, err := cl.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	for _, mb := range mailboxes {
		if mb.FolderPath == "to-delete" {
			t.Error("deleted mailbox still in listing")
		}
	}
}

func TestFolderIsolation(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Send to two different folders.
	r1, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("inbox msg"))
	if err != nil {
		t.Fatalf("send to inbox: %v", err)
	}

	r2, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("orders msg"),
		client.WithFolderPath("orders"))
	if err != nil {
		t.Fatalf("send to orders: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Retrieve from inbox — should only have r1.
	inboxMsgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve inbox: %v", err)
	}
	assertContainsMessage(t, inboxMsgs, r1.MessageID)
	assertNotContainsMessage(t, inboxMsgs, r2.MessageID)

	// Retrieve from orders — should only have r2.
	ordersMsgs, err := recipient.RetrieveMessages(ctx,
		client.WithRetrieveFolderPath("orders"))
	if err != nil {
		t.Fatalf("retrieve orders: %v", err)
	}
	assertContainsMessage(t, ordersMsgs, r2.MessageID)
	assertNotContainsMessage(t, ordersMsgs, r1.MessageID)
}

func TestQueryCapacity(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	capacity, err := cl.QueryCapacity(ctx)
	if err != nil {
		t.Fatalf("query capacity: %v", err)
	}
	if capacity == nil {
		t.Fatal("expected non-nil capacity")
	}
	if capacity.TotalStorageBytes <= 0 {
		t.Errorf("expected positive total storage, got %d", capacity.TotalStorageBytes)
	}
}

// assertContainsMessage asserts that the message slice contains a message with the given ID.
func assertContainsMessage(t *testing.T, messages []*core.Message, id string) {
	t.Helper()
	for _, msg := range messages {
		if msg.MessageID == id {
			return
		}
	}
	t.Errorf("message %s not found in %d messages", id, len(messages))
}

// assertNotContainsMessage asserts that the message slice does NOT contain a message with the given ID.
func assertNotContainsMessage(t *testing.T, messages []*core.Message, id string) {
	t.Helper()
	for _, msg := range messages {
		if msg.MessageID == id {
			t.Errorf("message %s unexpectedly found", id)
			return
		}
	}
}
