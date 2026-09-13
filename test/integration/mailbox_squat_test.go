package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	client "github.com/twostack/go-ricochet/pkg/client"
)

// A cross-peer retrieve of a mailbox that does not exist must not bring it
// into existence.
//
// It used to: the access handler guessed "public" for any read where the
// caller was not the owner, and the delivery agent's get-or-create honoured
// that guess. Reading victim/inbox before the victim ever connected therefore
// created their inbox as a public mailbox — after which every one of the
// victim's senders was refused for lacking an ACL grant, and anything that did
// land was readable by anyone. This pins the fixed behaviour from both sides:
// the reader gets a 404, and the victim's inbox is still private when it is
// finally created by a real delivery.
func TestCrossPeerRetrieveDoesNotCreateTheMailbox(t *testing.T) {
	server := newTestServer(t)
	attacker := newTestClient(t, server)
	victim := newTestClient(t, server)
	sender := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The attacker reads the victim's inbox before the victim has done anything.
	msgs, err := attacker.RetrieveMessages(ctx, client.WithTargetPeer(victim.PeerID()))
	if err == nil {
		t.Fatalf("retrieve of a nonexistent foreign mailbox returned %d messages and no error", len(msgs))
	}
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}

	// Nothing was created: the owner's list is still empty.
	boxes, err := victim.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("list mailboxes: %v", err)
	}
	if len(boxes) != 0 {
		t.Fatalf("victim has %d mailboxes after a foreign read; want 0", len(boxes))
	}

	// A real delivery creates the inbox, and it is private: an unrelated
	// sender can deposit without an ACL grant, which a public mailbox would
	// refuse.
	if _, err := sender.SendMessage(ctx, victim.PeerID(), []byte("hello")); err != nil {
		t.Fatalf("send to victim after the foreign read: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	got, err := victim.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("victim retrieve: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("victim got %d messages, want 1", len(got))
	}

	// And now that it exists as private, the attacker is refused rather than
	// handed an empty list — a refusal and an empty inbox are different facts.
	msgs, err = attacker.RetrieveMessages(ctx, client.WithTargetPeer(victim.PeerID()))
	if err == nil {
		t.Fatalf("retrieve of a foreign private mailbox returned %d messages and no error", len(msgs))
	}
	if !errors.Is(err, client.ErrForbidden) {
		t.Fatalf("err = %v, want ErrForbidden", err)
	}
	if client.IsRetryable(err) {
		t.Errorf("a 403 is reported as retryable")
	}
}

// An owner reading a folder that has never received anything gets an empty
// inbox, not an error. A fresh identity's first retrieve is the common case
// and must not look like a failure.
func TestOwnerRetrieveOfMissingMailboxIsEmpty(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msgs, err := cl.RetrieveMessages(ctx, client.WithRetrieveFolderPath("never-used"))
	if err != nil {
		t.Fatalf("owner retrieve of a missing folder: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("got %d messages from a folder that was never created", len(msgs))
	}

	// And reading it did not create it either.
	boxes, err := cl.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("list mailboxes: %v", err)
	}
	for _, b := range boxes {
		if b.FolderPath == "never-used" {
			t.Fatalf("an owner's read created the folder; mailboxes = %+v", boxes)
		}
	}
}
