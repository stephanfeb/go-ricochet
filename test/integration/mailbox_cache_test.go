package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	client "github.com/twostack/go-ricochet/pkg/client"
	"github.com/twostack/go-ricochet/pkg/wire"
)

// The delivery path caches loaded mailboxes. The cache used to be a plain
// map that grew by one entry per folder ever delivered to and was cleared
// only at shutdown.
func TestMailboxCacheStaysWithinItsBound(t *testing.T) {
	const bound, folders = 3, 8

	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.MailboxCacheSize = bound
	})
	cl := newTestClient(t, server)
	self := cl.PeerID()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < folders; i++ {
		res, err := cl.SendMessage(ctx, self, []byte("hello"),
			client.WithFolderPath(fmt.Sprintf("cache/folder-%d", i)))
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if !res.Success {
			t.Fatalf("send %d refused: %v", i, res.Err())
		}
	}

	stats := server.MDA.CacheStats()
	if stats.Capacity != bound {
		t.Errorf("cache capacity %d, want %d", stats.Capacity, bound)
	}
	if stats.Size > bound {
		t.Errorf("cache holds %d mailboxes after %d distinct folders, bound is %d", stats.Size, folders, bound)
	}
	if stats.Size != bound {
		t.Errorf("cache holds %d mailboxes, want it full at %d", stats.Size, bound)
	}

	// Eviction is only a cache concern: every folder still exists and reads.
	boxes, err := cl.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("list mailboxes: %v", err)
	}
	if len(boxes) != folders {
		t.Errorf("owner has %d mailboxes, want %d", len(boxes), folders)
	}
	for i := 0; i < folders; i++ {
		msgs, err := cl.RetrieveMessages(ctx, client.WithRetrieveFolderPath(fmt.Sprintf("cache/folder-%d", i)))
		if err != nil {
			t.Fatalf("retrieve folder %d: %v", i, err)
		}
		if len(msgs) != 1 {
			t.Errorf("folder %d holds %d messages, want 1", i, len(msgs))
		}
	}
}

// Delivery clamps a message's expiry to the mailbox's retention window, read
// from the cached record. An updateConfig used to write the new retention to
// storage only, so a mailbox that had been delivered to once kept clamping to
// its old window until the server restarted.
func TestUpdatedRetentionReachesTheNextDelivery(t *testing.T) {
	server := newTestServer(t)
	cl := newTestClient(t, server)
	self := cl.PeerID()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const folder = "cache/retention"
	if err := cl.CreateMailbox(ctx, folder, wire.MailboxPrivate, client.WithRetentionDays(30)); err != nil {
		t.Fatalf("create mailbox: %v", err)
	}

	// The client's default expiry is seven days, well inside a 30-day
	// window: this delivery loads the mailbox into the cache and is left
	// untouched by the clamp.
	send := func(body string) {
		t.Helper()
		res, err := cl.SendMessage(ctx, self, []byte(body), client.WithFolderPath(folder))
		if err != nil {
			t.Fatalf("send %q: %v", body, err)
		}
		if !res.Success {
			t.Fatalf("send %q refused: %v", body, res.Err())
		}
	}
	send("before")

	if err := cl.UpdateMailboxConfig(ctx, folder, client.WithRetentionDays(1)); err != nil {
		t.Fatalf("update mailbox config: %v", err)
	}
	info, err := cl.GetMailboxInfo(ctx, folder)
	if err != nil {
		t.Fatalf("mailbox info: %v", err)
	}
	if info.RetentionDays != 1 {
		t.Fatalf("stored retention_days = %d after update, want 1", info.RetentionDays)
	}

	sentAt := time.Now()
	send("after")

	msgs, err := cl.RetrieveMessages(ctx, client.WithRetrieveFolderPath(folder))
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("retrieved %d messages, want 2", len(msgs))
	}
	expiry := map[string]time.Time{}
	for _, m := range msgs {
		expiry[string(m.Payload)] = time.UnixMilli(m.ExpiryTimestamp)
	}

	oneDay := sentAt.Add(24 * time.Hour)
	if got := expiry["after"]; got.After(oneDay.Add(time.Minute)) {
		t.Errorf("message delivered after the update expires at %s, want within a day of %s: delivery clamped to the stale retention window",
			got.Format(time.RFC3339), sentAt.Format(time.RFC3339))
	}
	if got := expiry["before"]; got.Before(sentAt.Add(6 * 24 * time.Hour)) {
		t.Errorf("message delivered before the update expires at %s, want the client's seven-day default", got.Format(time.RFC3339))
	}
}
