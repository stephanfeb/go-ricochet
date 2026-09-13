package mailboxes_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	"github.com/twostack/go-ricochet/internal/mda/mailboxes"
	"github.com/twostack/go-ricochet/internal/storage"
	"github.com/twostack/go-ricochet/internal/storage/storagetest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newRecord(t *testing.T, store *storagetest.Fake, owner peer.ID, typ core.MailboxType, maxMessages int, retentionCount *int) *storage.MailboxRecord {
	t.Helper()
	addr, err := core.NewMailboxAddress(owner, "inbox", typ)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := store.GetOrCreateMailbox(context.Background(), addr, maxMessages, 30, retentionCount)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func message(sender, recipient peer.ID, body string) *core.Message {
	return core.NewMessageWithDefaultExpiry(sender, recipient, []byte(body))
}

func isUnauthorized(err error) bool {
	var u *mailboxes.UnauthorizedError
	return errors.As(err, &u)
}

func TestPrivateMailboxIsOwnerOnly(t *testing.T) {
	store := storagetest.New()
	owner, stranger := newPeer(t), newPeer(t)
	mb := mailboxes.NewPrivateMailbox(newRecord(t, store, owner, core.MailboxPrivate, 0, nil), store, quiet)
	ctx := context.Background()

	if err := mb.StoreMessage(ctx, message(stranger, owner, "for the owner")); err != nil {
		t.Fatalf("delivery addressed to the owner refused: %v", err)
	}
	if err := mb.StoreMessage(ctx, message(owner, stranger, "misaddressed")); !isUnauthorized(err) {
		t.Errorf("a message addressed to someone else was stored in the owner's private mailbox: err = %v", err)
	}

	if _, _, err := mb.RetrieveMessages(ctx, &stranger, mailboxes.RetrieveOpts{}); !isUnauthorized(err) {
		t.Errorf("stranger read a private mailbox: err = %v", err)
	}
	msgs, hasMore, err := mb.RetrieveMessages(ctx, &owner, mailboxes.RetrieveOpts{})
	if err != nil || len(msgs) != 1 || hasMore {
		t.Errorf("owner read: %d messages, hasMore %v, err %v; want 1, false, nil", len(msgs), hasMore, err)
	}
}

func TestSharedMailboxHonoursTheACL(t *testing.T) {
	store := storagetest.New()
	owner, writer, reader, stranger := newPeer(t), newPeer(t), newPeer(t), newPeer(t)
	rec := newRecord(t, store, owner, core.MailboxShared, 0, nil)
	mb := mailboxes.NewSharedMailbox(rec, store, quiet)
	ctx := context.Background()

	if err := store.GrantAccess(ctx, rec.ID, writer, core.AccessWriteOnly); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantAccess(ctx, rec.ID, reader, core.AccessReadOnly); err != nil {
		t.Fatal(err)
	}

	if err := mb.StoreMessage(ctx, message(owner, owner, "from owner")); err != nil {
		t.Errorf("owner write refused: %v", err)
	}
	if err := mb.StoreMessage(ctx, message(writer, owner, "from writer")); err != nil {
		t.Errorf("write-only grantee refused: %v", err)
	}
	if err := mb.StoreMessage(ctx, message(reader, owner, "from reader")); !isUnauthorized(err) {
		t.Errorf("read-only grantee wrote to a shared mailbox: err = %v", err)
	}
	if err := mb.StoreMessage(ctx, message(stranger, owner, "from stranger")); !isUnauthorized(err) {
		t.Errorf("stranger wrote to a shared mailbox: err = %v", err)
	}

	if _, _, err := mb.RetrieveMessages(ctx, &writer, mailboxes.RetrieveOpts{}); !isUnauthorized(err) {
		t.Errorf("write-only grantee read a shared mailbox: err = %v", err)
	}
	if _, _, err := mb.RetrieveMessages(ctx, nil, mailboxes.RetrieveOpts{}); err == nil {
		t.Error("anonymous read of a shared mailbox succeeded")
	}
	msgs, _, err := mb.RetrieveMessages(ctx, &reader, mailboxes.RetrieveOpts{})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("reader got %d messages, err %v; want 2", len(msgs), err)
	}

	// A shared mailbox keeps a cursor per reader: the next read starts after
	// the last message handed out.
	if err := mb.StoreMessage(ctx, message(owner, owner, "third")); err != nil {
		t.Fatal(err)
	}
	msgs, _, err = mb.RetrieveMessages(ctx, &reader, mailboxes.RetrieveOpts{})
	if err != nil || len(msgs) != 1 || string(msgs[0].Payload) != "third" {
		t.Errorf("second read returned %d messages (err %v); want only the one stored since", len(msgs), err)
	}
}

func TestPublicMailboxIsReadableByAnyoneAndWritableByGrant(t *testing.T) {
	store := storagetest.New()
	owner, publisher, stranger := newPeer(t), newPeer(t), newPeer(t)
	rec := newRecord(t, store, owner, core.MailboxPublic, 0, nil)
	mb := mailboxes.NewPublicMailbox(rec, store, quiet)
	ctx := context.Background()

	if err := store.GrantAccess(ctx, rec.ID, publisher, core.AccessReadWrite); err != nil {
		t.Fatal(err)
	}
	if err := mb.StoreMessage(ctx, message(owner, owner, "owner post")); err != nil {
		t.Errorf("owner publish refused: %v", err)
	}
	if err := mb.StoreMessage(ctx, message(publisher, owner, "granted post")); err != nil {
		t.Errorf("granted publisher refused: %v", err)
	}
	if err := mb.StoreMessage(ctx, message(stranger, owner, "spam")); !isUnauthorized(err) {
		t.Errorf("stranger published to a public mailbox: err = %v", err)
	}

	msgs, _, err := mb.RetrieveMessages(ctx, &stranger, mailboxes.RetrieveOpts{})
	if err != nil || len(msgs) != 2 {
		t.Errorf("stranger read %d messages, err %v; want 2, a public mailbox is readable by anyone", len(msgs), err)
	}
	msgs, _, err = mb.RetrieveMessages(ctx, nil, mailboxes.RetrieveOpts{})
	if err != nil || len(msgs) != 2 {
		t.Errorf("anonymous read %d messages, err %v; want 2", len(msgs), err)
	}
}

// A mailbox with a retention count is a rolling window: when the cap is hit
// the mailbox is pruned to its newest retention_count messages and the store
// retried, so it never refuses. The count has to sit below the cap for that
// to hold, which is what the MMA clamp enforces.
func TestRollingWindowPrunesInsteadOfRefusing(t *testing.T) {
	store := storagetest.New()
	owner := newPeer(t)
	keep := 2
	rec := newRecord(t, store, owner, core.MailboxPrivate, 4, &keep)
	mb := mailboxes.NewPrivateMailbox(rec, store, quiet)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := mb.StoreMessage(ctx, message(owner, owner, fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}
	var got []string
	for _, m := range store.Messages(rec.ID) {
		got = append(got, string(m.Payload))
	}
	// Four fill the cap; the fifth prunes to the newest two and lands.
	if want := "[m2 m3 m4]"; fmt.Sprint(got) != want {
		t.Errorf("mailbox holds %v, want %s", got, want)
	}

	// Without a retention count the same cap is a hard refusal.
	hard := mailboxes.NewPrivateMailbox(newRecord(t, store, newPeer(t), core.MailboxPrivate, 1, nil), store, quiet)
	who, _ := peer.Decode(hard.Record().OwnerPeerID)
	if err := hard.StoreMessage(ctx, message(who, who, "one")); err != nil {
		t.Fatal(err)
	}
	var full *mailboxes.MailboxFullError
	if err := hard.StoreMessage(ctx, message(who, who, "two")); !errors.As(err, &full) {
		t.Errorf("second store into a capped mailbox: err = %v, want MailboxFullError", err)
	}
}

func TestRetrievePagesAndReportsMore(t *testing.T) {
	store := storagetest.New()
	owner := newPeer(t)
	mb := mailboxes.NewPrivateMailbox(newRecord(t, store, owner, core.MailboxPrivate, 0, nil), store, quiet)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := mb.StoreMessage(ctx, message(owner, owner, fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	two := 2
	msgs, hasMore, err := mb.RetrieveMessages(ctx, &owner, mailboxes.RetrieveOpts{MaxMessages: &two})
	if err != nil || len(msgs) != 2 || !hasMore {
		t.Errorf("page of 2: got %d, hasMore %v, err %v", len(msgs), hasMore, err)
	}
	from := 4
	msgs, hasMore, err = mb.RetrieveMessages(ctx, &owner, mailboxes.RetrieveOpts{FromSequence: &from})
	if err != nil || len(msgs) != 2 || hasMore {
		t.Errorf("from sequence 4: got %d, hasMore %v, err %v; want the last two and no more", len(msgs), hasMore, err)
	}

	// A byte budget smaller than the page cuts it short but always returns
	// at least one message, or a client with a small budget could never
	// make progress.
	msgs, hasMore, err = mb.RetrieveMessages(ctx, &owner, mailboxes.RetrieveOpts{MaxBytes: 1})
	if err != nil || len(msgs) != 1 || !hasMore {
		t.Errorf("byte budget of 1: got %d, hasMore %v, err %v; want 1 message and more", len(msgs), hasMore, err)
	}

	// The page size is capped.
	huge := mailboxes.MaxPageSize * 10
	if _, _, err := mb.RetrieveMessages(ctx, &owner, mailboxes.RetrieveOpts{MaxMessages: &huge}); err != nil {
		t.Errorf("oversized page request: %v", err)
	}
}
