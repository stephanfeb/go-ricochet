package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stephanfeb/go-ricochet/internal/core"
	client "github.com/stephanfeb/go-ricochet/pkg/client"
)

// Mark-delivered, update-flags and delete are keyed by message ID, and message
// IDs are chosen by the sender and echoed in every acknowledgement. They used
// to reach storage with no owner scope, so a sender could delete a message
// after delivering it, and any reader of a shared mailbox could delete
// everyone else's. Now each mutation is scoped to mailboxes the caller owns:
// a foreign ID is not matched, the acknowledgement reports zero, and the
// message is still there for its owner.
func TestMessageMutationsAreScopedToTheOwner(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)
	bystander := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("mine to keep"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	id := result.MessageID

	// The sender knows the ID because it chose it; the bystander could have
	// guessed or been told it. Neither may act on it.
	for name, cl := range map[string]interface {
		MarkDelivered(context.Context, []string) (*core.MarkDeliveredAck, error)
		UpdateFlags(context.Context, string, uint32, uint32) (*core.UpdateFlagsAck, error)
		DeleteMessages(context.Context, []string) (*core.DeleteMessagesAck, error)
	}{"sender": sender, "bystander": bystander} {
		mark, err := cl.MarkDelivered(ctx, []string{id})
		if err != nil {
			t.Fatalf("%s mark delivered: %v", name, err)
		}
		if mark.UpdatedCount != 0 {
			t.Errorf("%s marked %d foreign messages delivered; want 0", name, mark.UpdatedCount)
		}

		flags, err := cl.UpdateFlags(ctx, id, uint32(core.MsgFlagDeleted), 0)
		if err != nil {
			t.Fatalf("%s update flags: %v", name, err)
		}
		if flags.Success || flags.NewFlags != nil {
			t.Errorf("%s flagged a foreign message: success=%v flags=%v", name, flags.Success, flags.NewFlags)
		}

		del, err := cl.DeleteMessages(ctx, []string{id})
		if err != nil {
			t.Fatalf("%s delete: %v", name, err)
		}
		if del.DeletedCount != 0 {
			t.Errorf("%s deleted %d foreign messages; want 0", name, del.DeletedCount)
		}
	}

	// The message survived all of that, untouched, for its owner.
	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("recipient retrieve: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("recipient has %d messages after foreign mutations; want 1", len(msgs))
	}
	if msgs[0].MsgFlags != 0 {
		t.Errorf("message flags = %v after foreign flag updates; want 0", msgs[0].MsgFlags)
	}
}

// The delete acknowledgement reports what was removed, not what was asked.
func TestDeleteCountIsRowsRemoved(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := sender.SendMessage(ctx, recipient.PeerID(), []byte("delete me"),
		client.WithPersistent(true))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	ack, err := recipient.DeleteMessages(ctx, []string{result.MessageID, "no-such-id", "another-missing"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !ack.Success {
		t.Fatalf("delete not successful: %s", ack.ErrorMessage)
	}
	if ack.DeletedCount != 1 {
		t.Errorf("deleted count = %d; want 1 (one real ID, two that never existed)", ack.DeletedCount)
	}
}
