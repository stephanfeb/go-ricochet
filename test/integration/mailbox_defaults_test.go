package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/twostack/go-ricochet/internal/core"
	client "github.com/twostack/go-ricochet/pkg/client"
)

// A mailbox created by delivery has to honour max_messages_per_mailbox.
//
// It did not: getMailbox passed a hardcoded 1000, so the setting reached only
// mailboxes somebody created explicitly through MMA. An operator could lower
// the cap, watch delivery keep filling mailboxes well past it, and find
// nothing in the logs to explain the discrepancy — the same shape as sumi's
// Finding A, a knob that never reaches the code that would honour it.
func TestConfiguredCapReachesMailboxesCreatedByDelivery(t *testing.T) {
	const cap = 3

	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.MaxMessagesPerMailbox = cap
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	self := cl.PeerID()

	// Nothing creates this mailbox in advance: the first send is what brings
	// it into existence, which is the path that ignored the configuration.
	folder := "delivery-cap/inbox"
	for i := 0; i < cap; i++ {
		res, err := cl.SendMessage(ctx, self, []byte("x"), client.WithFolderPath(folder))
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if !res.Success {
			t.Fatalf("send %d rejected below the cap of %d: %s", i, cap, res.ErrorMessage)
		}
	}

	res, err := cl.SendMessage(ctx, self, []byte("x"), client.WithFolderPath(folder))
	if err != nil {
		t.Fatalf("send past the cap: %v", err)
	}
	if res.Success {
		t.Fatalf("a %d-message cap accepted message %d; the configured value did not reach delivery",
			cap, cap+1)
	}

	// And the record must carry the configured cap, not the old literal.
	var maxMessages int
	if err := server.Storage.Pool().QueryRow(ctx,
		`SELECT max_messages FROM mailboxes WHERE owner_peer_id = $1 AND folder_path = $2`,
		self.String(), folder).Scan(&maxMessages); err != nil {
		t.Fatalf("read the mailbox record: %v", err)
	}
	if maxMessages != cap {
		t.Errorf("stored max_messages = %d, want the configured %d", maxMessages, cap)
	}
}

// Retention days must arrive as whole days derived from the policy, and never
// as zero — retention deletes anything older than N days, so zero deletes
// everything on the next sweep.
func TestConfiguredRetentionReachesMailboxesCreatedByDelivery(t *testing.T) {
	server := newTestServer(t, func(cfg *core.ServerConfig) {
		cfg.RetentionPolicy = 5 * 24 * time.Hour
	})
	cl := newTestClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	self := cl.PeerID()

	folder := "delivery-retention/inbox"
	if _, err := cl.SendMessage(ctx, self, []byte("x"), client.WithFolderPath(folder)); err != nil {
		t.Fatalf("send: %v", err)
	}

	var retentionDays int
	if err := server.Storage.Pool().QueryRow(ctx,
		`SELECT retention_days FROM mailboxes WHERE owner_peer_id = $1 AND folder_path = $2`,
		self.String(), folder).Scan(&retentionDays); err != nil {
		t.Fatalf("read the mailbox record: %v", err)
	}

	if retentionDays != 5 {
		t.Errorf("stored retention_days = %d, want the configured 5", retentionDays)
	}
	if retentionDays == 0 {
		t.Error("a retention of zero days deletes every message on the next sweep")
	}
}
