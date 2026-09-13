package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/twostack/go-ricochet/internal/core"
	client "github.com/twostack/go-ricochet/pkg/client"
)

// mustSend submits and fails the test unless the server accepted.
func mustSend(t *testing.T, ctx context.Context, cl *client.Client, to peer.ID, payload []byte, opts ...client.SendOption) *client.SendResult {
	t.Helper()
	res, err := cl.SendMessage(ctx, to, payload, opts...)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !res.Success {
		t.Fatalf("send refused: status %d: %s", res.Status, res.ErrorMessage)
	}
	return res
}

// refusedStatus submits, fails the test if the server accepted, and returns
// the status it refused with.
func refusedStatus(t *testing.T, ctx context.Context, cl *client.Client, to peer.ID, payload []byte, opts ...client.SendOption) int {
	t.Helper()
	res, err := cl.SendMessage(ctx, to, payload, opts...)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.Success {
		t.Fatalf("server accepted a submission it should have refused")
	}
	return res.Status
}

// Delivery creates the folder the sender names, and the name used to be
// accepted as sent: up to a frame in length, any bytes at all. It is now
// validated as an explicit create is.
func TestDeliveryRefusesAnInvalidFolderPath(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for name, folder := range map[string]string{
		"too long":          strings.Repeat("f", core.MaxFolderPathLength+1),
		"control character": "inbox\x00evil",
		"leading slash":     "/inbox",
	} {
		if got := refusedStatus(t, ctx, sender, recipient.PeerID(), []byte("x"), client.WithFolderPath(folder)); got != 400 {
			t.Errorf("%s: refused with status %d; want 400", name, got)
		}
	}

	boxes, err := recipient.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("list mailboxes: %v", err)
	}
	if len(boxes) != 0 {
		t.Fatalf("%d mailboxes exist after refused deliveries; want none", len(boxes))
	}
}

// A sender names the folder, so folders per owner is what stops one peer
// giving another an unbounded number of them. The cap applies whoever is
// creating: the owner's own createMailbox is held to it too.
func TestFoldersPerOwnerAreCapped(t *testing.T) {
	const perOwner = 2
	server := newTestServer(t, func(c *core.ServerConfig) { c.MaxMailboxesPerOwner = perOwner })
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < perOwner; i++ {
		mustSend(t, ctx, sender, recipient.PeerID(), []byte("x"), client.WithFolderPath(fmt.Sprintf("f%d", i)))
	}
	if got := refusedStatus(t, ctx, sender, recipient.PeerID(), []byte("x"), client.WithFolderPath("one-too-many")); got != 507 {
		t.Errorf("status %d; want 507", got)
	}

	if err := recipient.CreateMailbox(ctx, "mine", core.MailboxPrivate); err == nil {
		t.Fatal("owner created a folder past the per-owner quota")
	}

	// An existing folder is unaffected: the quota is on creation.
	mustSend(t, ctx, sender, recipient.PeerID(), []byte("y"), client.WithFolderPath("f0"))
}

// Client-chosen caps and retention are clamped to the server's, and values
// that could never have been meant are refused.
func TestClientMailboxSettingsAreClampedToTheServer(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cl.CreateMailbox(ctx, "greedy", core.MailboxPrivate,
		client.WithMailboxMaxMessages(2_000_000_000), client.WithRetentionDays(36500)); err != nil {
		t.Fatalf("create: %v", err)
	}
	info, err := cl.GetMailboxInfo(ctx, "greedy")
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.MaxMessages != server.Config.MaxMessagesPerMailbox {
		t.Errorf("maxMessages = %d; want clamped to server %d", info.MaxMessages, server.Config.MaxMessagesPerMailbox)
	}
	if want := server.MDA.MailboxDefaults().RetentionDays; info.RetentionDays != want {
		t.Errorf("retentionDays = %d; want clamped to server %d", info.RetentionDays, want)
	}

	if err := cl.CreateMailbox(ctx, "negative", core.MailboxPrivate, client.WithRetentionDays(-5)); err == nil {
		t.Error("a negative retention was accepted")
	}
	if err := cl.CreateMailbox(ctx, "zero", core.MailboxPrivate, client.WithMailboxMaxMessages(0)); err == nil {
		t.Error("a zero cap was accepted")
	}
}

// The public mailbox was the one type without a cap.
func TestPublicMailboxIsCapped(t *testing.T) {
	server := newTestServer(t)
	owner := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := owner.CreateMailbox(ctx, "announce", core.MailboxPublic, client.WithMailboxMaxMessages(1)); err != nil {
		t.Fatalf("create: %v", err)
	}
	mustSend(t, ctx, owner, owner.PeerID(), []byte("first"), client.WithFolderPath("announce"))
	if got := refusedStatus(t, ctx, owner, owner.PeerID(), []byte("second"), client.WithFolderPath("announce")); got != 507 {
		t.Errorf("status %d; want 507", got)
	}
}

// A sender's expiry is bounded by the mailbox's retention, and a message
// sent without one gets the same bound rather than living forever.
func TestExpiryIsClampedToRetention(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mustSend(t, ctx, sender, recipient.PeerID(), []byte("forever"), client.WithExpiry(100*365*24*time.Hour))
	time.Sleep(100 * time.Millisecond)

	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	retention := time.Duration(server.MDA.MailboxDefaults().RetentionDays) * 24 * time.Hour
	limit := time.Now().Add(retention + time.Minute).UnixMilli()
	if msgs[0].ExpiryTimestamp > limit {
		t.Errorf("expiry %d is past the retention bound %d", msgs[0].ExpiryTimestamp, limit)
	}
	if msgs[0].ExpiryTimestamp <= time.Now().UnixMilli() {
		t.Errorf("expiry %d is already in the past", msgs[0].ExpiryTimestamp)
	}
}

// Retention used to be enforced only for public mailboxes that happened to
// be in the delivery cache. The sweep now covers every mailbox.
func TestRetentionSweepsPrivateMailboxes(t *testing.T) {
	server := newTestServer(t)
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	old := mustSend(t, ctx, sender, recipient.PeerID(), []byte("stale"), client.WithPersistent(true))
	fresh := mustSend(t, ctx, sender, recipient.PeerID(), []byte("fresh"), client.WithPersistent(true))
	time.Sleep(100 * time.Millisecond)

	// Age one message past the mailbox's retention window.
	days := server.MDA.MailboxDefaults().RetentionDays
	if _, err := server.Storage.Pool().Exec(ctx,
		`UPDATE stored_messages SET created_at = NOW() - INTERVAL '1 day' * $1 WHERE message_id = $2`,
		days+1, old.MessageID); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	if err := server.MDA.PerformMaintenance(ctx); err != nil {
		t.Fatalf("maintenance: %v", err)
	}

	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != 1 || msgs[0].MessageID != fresh.MessageID {
		ids := make([]string, 0, len(msgs))
		for _, m := range msgs {
			ids = append(ids, m.MessageID)
		}
		t.Fatalf("after retention sweep mailbox holds %v; want only the fresh message %s", ids, fresh.MessageID)
	}
}

// A non-owner append to a feed that does not exist used to create it, as a
// collaborative feed under the owner's identity, that the owner could not
// then make private. It is a 404 now; only the owner creates feeds.
func TestNonOwnerAppendDoesNotCreateAFeed(t *testing.T) {
	server := newTestServer(t)
	owner := newTestClient(t, server)
	stranger := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := stranger.AppendToFeed(ctx, owner.PeerID(), "microblog/threads/planted", []byte("hi"), "reply")
	if err == nil {
		t.Fatal("append to a missing feed of another peer succeeded")
	}
	if got := client.Status(err); got != 404 {
		t.Errorf("status %d (%v); want 404", got, err)
	}

	feeds, err := owner.ListFeeds(ctx, owner.PeerID())
	if err != nil {
		t.Fatalf("list feeds: %v", err)
	}
	if len(feeds) != 0 {
		t.Fatalf("owner has %d feeds after a stranger's append; want none", len(feeds))
	}

	// A collaborative feed the owner created still takes the append.
	if err := owner.CreateFeed(ctx, "microblog/threads/real", "Thread", "", client.WithCollaborative()); err != nil {
		t.Fatalf("create collaborative feed: %v", err)
	}
	if _, err := stranger.AppendToFeed(ctx, owner.PeerID(), "microblog/threads/real", []byte("reply"), "reply"); err != nil {
		t.Fatalf("append to the owner's collaborative feed refused: %v", err)
	}
}

// Entries per feed are capped by the server, which is what bounds a
// collaborative feed anyone may append to.
func TestFeedEntriesAreCapped(t *testing.T) {
	const cap = 3
	server := newTestServer(t, func(c *core.ServerConfig) { c.MaxEntriesPerFeed = cap })
	owner := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := owner.CreateFeed(ctx, "notes", "Notes", ""); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < cap; i++ {
		if _, err := owner.AppendFeedEntry(ctx, "notes", []byte{byte(i)}, ""); err != nil {
			t.Fatalf("append %d within cap: %v", i, err)
		}
	}
	_, err := owner.AppendFeedEntry(ctx, "notes", []byte("overflow"), "")
	if err == nil {
		t.Fatal("append past the per-feed cap succeeded")
	}
	if got := client.Status(err); got != 507 {
		t.Errorf("status %d (%v); want 507", got, err)
	}
}

// max_storage_bytes was reported and never enforced. With the budget set
// below what the database already occupies, writes are refused and reads
// still work.
func TestWritesAreRefusedWhenStorageIsOverBudget(t *testing.T) {
	// One byte is under any database's size on disk, so the first capacity
	// sample, taken as the server starts, already puts it over budget.
	server := newTestServer(t, func(c *core.ServerConfig) { c.MaxStorageBytes = 1 })
	sender := newTestClient(t, server)
	recipient := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Something to read back, delivered behind the gate.
	before := core.NewMessageWithDefaultExpiry(sender.PeerID(), recipient.PeerID(), []byte("before"))
	if _, err := server.MDA.DeliverLocal(ctx, before); err != nil {
		t.Fatalf("deliver directly: %v", err)
	}

	if got := refusedStatus(t, ctx, sender, recipient.PeerID(), []byte("after")); got != 507 {
		t.Errorf("submit status %d; want 507", got)
	}
	if err := recipient.CreateFeed(ctx, "f", "F", ""); err == nil {
		t.Error("feed create accepted while over budget")
	} else if got := client.Status(err); got != 507 {
		t.Errorf("feed create status %d (%v); want 507", got, err)
	}

	msgs, err := recipient.RetrieveMessages(ctx)
	if err != nil {
		t.Fatalf("retrieve while over budget: %v", err)
	}
	if len(msgs) != 1 || !bytes.Equal(msgs[0].Payload, []byte("before")) {
		t.Fatalf("read while over budget returned %d messages", len(msgs))
	}
}
